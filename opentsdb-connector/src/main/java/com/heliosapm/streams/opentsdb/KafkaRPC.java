/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.heliosapm.streams.opentsdb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.hbase.async.jsr166e.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.heliosapm.streams.buffers.BufferManager;
import com.heliosapm.streams.buffers.ByteBufSerde.ByteBufDeserializer;
import com.heliosapm.streams.chronicle.MessageListener;
import com.heliosapm.streams.chronicle.MessageQueue;
import com.heliosapm.streams.common.kafka.interceptor.MonitoringConsumerInterceptor;
import com.heliosapm.streams.metrics.Blacklist;
import com.heliosapm.streams.metrics.StreamedMetricValue;
import com.heliosapm.streams.opentsdb.plugin.PluginMetricManager;
import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.jmx.SharedScheduler;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import io.netty.buffer.ByteBuf;
import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.tsd.RpcPlugin;

/**
 * <p>Title: KafkaRPC</p>
 * <p>Description: Kafka based metric ingestion RPC service</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.opentsdb.KafkaRPC</code></p>
 */

public class KafkaRPC extends RpcPlugin implements KafkaRPCMBean, Runnable, MessageListener, ConsumerRebalanceListener  {
	/** The TSDB instance */
	static TSDB tsdb = null;
	/** The prefix on TSDB config items marking them as applicable to this service */
	public static final String CONFIG_PREFIX = "tsd.rpc.kafka.";
	/** The prefix length */
	public static final int CONFIG_PREFIX_LEN = CONFIG_PREFIX.length();
	/** The TSDB config key for the names of topics to subscribe to */
	public static final String CONFIG_TOPICS = "tsdbtopics";
	/** The default topic name to listen on */
	public static final String[] DEFAULT_TOPIC = {"tsdb-metrics"};
	/** The TSDB config key for sync processing of add data points */
	public static final String CONFIG_SYNC_ADD = "syncadd";
	/** The default sync add */
	public static final boolean DEFAULT_SYNC_ADD = true;

	/** The config key name for the timeout on synchronous data point puts to the TSDB in ms. */
	public static final String CONFIG_SYNCADD_TIMEOUT = "syncadd.timeout";
	/** The default timeout on synchronous data point puts to the TSDB in ms. */
	public static final long DEFAULT_SYNCADD_TIMEOUT = 10000;
	
	/** The config key name for the timeout on waiting for kafka partition data during startup in secs. */
	public static final String CONFIG_KAFKAMETA_TIMEOUT = "syncadd.timeout";
	/** The default timeout on waiting for kafka partition data during startup in secs. */
	public static final int DEFAULT_KAFKAMETA_TIMEOUT = 120;
	
	/** The config key name for buffer write compression */
	public static final String CONFIG_COMPRESS_QWRITES = "writer.compression";
	/** The default buffer write compression. */
	public static final boolean DEFAULT_COMPRESS_QWRITES = false;
	
	/** The config key name for enabling kafka monitoring interceptor */
	public static final String CONFIG_KAFKA_MONITOR = "kafka.monitor";
	/** The default enablement of kafka monitoring interceptor */
	public static final boolean DEFAULT_KAFKA_MONITOR = true;
	
	/** The TSDB config key for the polling timeout in ms. */
	public static final String CONFIG_POLLTIMEOUT = "polltime";
	/** The default polling timeout in ms. */
	public static final long DEFAULT_POLLTIMEOUT = 10000;
	
	/** The config key name for stat collection forwarding */
	public static final String CONFIG_STATS_FORWARD = "stats.forward";
	/** The default stat collection forwarding */
	public static final boolean DEFAULT_STATS_FORWARD = false;
	
	
	/** A ref to the buffer manager */
	protected final BufferManager bufferManager;
	
	/** Instance logger */
	protected final Logger log = LoggerFactory.getLogger(getClass());
	/** The kafka consumer client configuration */
	protected final Properties consumerConfig = new Properties();
	/** The kafka consumer */
	protected KafkaConsumer<String, ByteBuf> consumer = null;
	/** The topics to subscribe to */
	protected String[] topics = null;
	/** Indicates if the consumer is closed */
	protected final AtomicBoolean closed = new AtomicBoolean(false);
	/** The configured consumer pool timeout */
	protected long pollTimeout = DEFAULT_POLLTIMEOUT;
	/** The configured sync add */
	protected boolean syncAdd = DEFAULT_SYNC_ADD;
	/** The subscriber thread */
	protected Thread subThread = null;
	/** The timeout for sync adds to TSDB in ms. */
	protected long syncAddTimeout = DEFAULT_SYNCADD_TIMEOUT;
	/** The timeout for waiting on kafka partition data at startup in sec. */
	protected int kafkaStartupTimeout = DEFAULT_KAFKAMETA_TIMEOUT;
	
	
	
	
	/** Indicates if message queue writes should be compressed */
	protected boolean compression = false;
	/** Indicates if the kafka monitoring interceptors should be installed */
	protected boolean monitoringInterceptor = false;
	/** Indicates if stats collection should include a save to the tsd */
	protected boolean forwardStats = false;
	
	/** The chronicle message queue */
	protected MessageQueue messageQueue;
	
	/** The currently assigned topic partitions */
	protected final Set<String> assignedPartitions = new CopyOnWriteArraySet<String>();
	



	/** The blacklisted metric key manager */
	protected final Blacklist blacklist = Blacklist.getInstance();
	
	/** A counter tracking the number of pending data point adds */
	protected final LongAdder pendingDataPointAdds = new LongAdder();
	
	protected CountDownLatch startLatch =new CountDownLatch(1);
	
	/** The metric manager for this plugin */
	protected final PluginMetricManager metricManager = new PluginMetricManager(getClass().getSimpleName());
	
	/** A meter to track the rate of points added */
	protected final Meter pointsAddedMeter = metricManager.meter("pointsAdded");
	/** A timer to track the elapsed time per message ingested */
	protected final Timer perMessageTimer = metricManager.timer("perMessage");
	/** A counter of tsdb data point put timeouts */
	protected final Counter putTimeouts = metricManager.counter("putTimeouts");
	/** A counter of metric deserialization errors */
	protected final Counter deserErrors = metricManager.counter("deserErrors");
	
	
	/** The per message timer snapshot */
	protected final CachedGauge<Snapshot> perMessageTimerSnap = new CachedGauge<Snapshot>(5, TimeUnit.SECONDS) {
		@Override
		protected Snapshot loadValue() {
			return perMessageTimer.getSnapshot();
		}
	};
	
	
	
	/**
	 * Creates a new KafkaRPC
	 */
	public KafkaRPC() {		
		log.info("Insantiated KafkaRPC Plugin, cp:[{}]", getClass().getProtectionDomain().getCodeSource().getLocation());
		bufferManager = BufferManager.getInstance();
	}
	
	

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RpcPlugin#initialize(net.opentsdb.core.TSDB)
	 */
	@Override
	public void initialize(final TSDB tsdb) {	
		KafkaRPC.tsdb = tsdb;
		
		final Properties p = new Properties();
		p.putAll(tsdb.getConfig().getMap());
		final Properties rpcConfig = extractOrEnv(CONFIG_PREFIX, p, true); 
		topics = ConfigurationHelper.getArraySystemThenEnvProperty(CONFIG_TOPICS, DEFAULT_TOPIC, rpcConfig);
		pollTimeout = ConfigurationHelper.getLongSystemThenEnvProperty(CONFIG_POLLTIMEOUT, DEFAULT_POLLTIMEOUT, rpcConfig);
		syncAdd = ConfigurationHelper.getBooleanSystemThenEnvProperty(CONFIG_SYNC_ADD, DEFAULT_SYNC_ADD, rpcConfig);
		forwardStats = ConfigurationHelper.getBooleanSystemThenEnvProperty(CONFIG_STATS_FORWARD, DEFAULT_STATS_FORWARD, rpcConfig);
		syncAddTimeout = ConfigurationHelper.getLongSystemThenEnvProperty(CONFIG_SYNCADD_TIMEOUT, DEFAULT_SYNCADD_TIMEOUT, rpcConfig);
		kafkaStartupTimeout = ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_KAFKAMETA_TIMEOUT, DEFAULT_KAFKAMETA_TIMEOUT, rpcConfig);
		monitoringInterceptor = ConfigurationHelper.getBooleanSystemThenEnvProperty(CONFIG_KAFKA_MONITOR, DEFAULT_KAFKA_MONITOR, rpcConfig);
		compression = ConfigurationHelper.getBooleanSystemThenEnvProperty(CONFIG_COMPRESS_QWRITES, DEFAULT_COMPRESS_QWRITES, rpcConfig);
		messageQueue = MessageQueue.getInstance(getClass().getSimpleName(), this, rpcConfig);
		
		if(monitoringInterceptor) {
			rpcConfig.put("interceptor.classes", MonitoringConsumerInterceptor.class.getName());
//			Props.appendToValue("interceptor.classes", MonitoringConsumerInterceptor.class.getName(), ",", rpcConfig);
		}
		InterceptorInstaller.filter(false, rpcConfig);
//		
		consumerConfig.putAll(extractOrEnv(CONFIG_PREFIX, rpcConfig, true));
		metricManager.addExtraTag("mode", syncAdd ? "sync" : "async");		
		printConfig();
//		try {
//			final Class<?> consumerClazz = Class.forName("org.apache.kafka.clients.consumer.KafkaConsumer");
//			final Class<?> interceptorClazz = Class.forName("org.apache.kafka.clients.consumer.KafkaConsumer");
//		} catch (Exception ex) {
//			throw new RuntimeException(ex);
//		}
		consumer = new KafkaConsumer<String, ByteBuf>(rpcConfig, new StringDeserializer(), new ByteBufDeserializer());
		
		subThread = new Thread(this, "KafkaSubscriptionThread");
		subThread.start();
		JMXHelper.registerMBean(this, OBJECT_NAME);
		try {
			log.info("\n\t#########################\n\tWaiting on partition assignment\n\t#########################\n");
			if(!startLatch.await(kafkaStartupTimeout, TimeUnit.SECONDS)) {  
				log.error("Timed out waiting on partition assignment");
				throw new IllegalArgumentException();
			}
			log.info("\n\t#########################\n\tPartitions assigned\n\t#########################\n");
		} catch (Exception x) {
			log.error("Interrupted while waiting on partition assignment");
			throw new IllegalArgumentException();
		}
		final InternalStatsCollector collector = new InternalStatsCollector(tsdb, "tsd");
		SharedScheduler.getInstance().scheduleWithFixedDelay(new Runnable(){
			@Override
			public void run() {
				tsdb.collectStats(collector);
			}
		}, 5, 15, TimeUnit.SECONDS);
	}
	
	/**
	 * Scans the passed source properties, system properties, then environment for any property keys starting with the passed prefix and copies any matching to the target which is returned.
	 * @param prefix The prefix to match
	 * @param source The source propereties to copy from
	 * @param removePrefix If true, the prefix is removed from the key before adding to the target
	 * @return the target properties
	 */
	public static Properties extractOrEnv(final String prefix, final Properties source, final boolean removePrefix) {
		if(prefix==null || prefix.trim().isEmpty()) throw new IllegalArgumentException("The passed prefix was null or empty");
		
		final String _prefix = prefix.trim().endsWith(".") ? prefix.trim() : (prefix.trim() + ".");
		final int len = _prefix.length();
		final Properties p = scan(_prefix, source, System.getProperties(), env());
		final Properties p2 = new Properties();
		if(removePrefix && !p.isEmpty()) {
			for(final String key: p.stringPropertyNames()) {
				final String newKey = removePrefix ? key.substring(len) : key;
				p2.put(newKey, p.getProperty(key));
			}
			return p2;
		}
		return p;
	}
	
	/**
	 * Returns the environment variables as lowercased properties with dashes replaced as dots
	 * @return property equivalents for environment variables 
	 */
	public static Properties env() {
		final Properties p = new Properties();
		for(Map.Entry<String, String> entry: System.getenv().entrySet()) {
			p.put(entry.getKey().toLowerCase().replace('-', '.'), entry.getValue());
		}
		return p;
	}
	
	public static Properties scan(final String prefix, final Properties...props) {
		if(prefix==null || prefix.trim().isEmpty()) throw new IllegalArgumentException("The passed prefix was null or empty");
		if(props.length==0) return null;
		final Map<String, String> capture = new HashMap<String, String>();
		for(Properties p: props) {
			if(p==null || p.isEmpty()) continue;
			for(String key: p.stringPropertyNames()) {
				if(key.indexOf(prefix)==0)  {
					if(capture.containsKey(key)) continue;
					capture.put(key, p.getProperty(key));
				}
			}
		}
		final Properties extracted = new Properties();
		extracted.putAll(capture);
		return extracted;
	}
	
	
	
	
	/**
	 * Prints the critical configuration
	 */
	protected void printConfig() {
		final StringBuilder b = new StringBuilder("\n\t===================== ").append(getClass().getSimpleName()).append(" Configuration =====================");
		b.append("\n\tKafka Topics:").append(Arrays.toString(topics));
		b.append("\n\tKafka Poll Timeout:").append(pollTimeout);
		b.append("\n\tKafka Async Commit:").append(syncAdd);
		b.append("\n\tKafka Monitor Enabled:").append(syncAdd);
		b.append("\n\tTSDB Put Timeout (ms):").append(syncAddTimeout);
		b.append("\n\tCompressed MessageQueue Writes:").append(monitoringInterceptor);		
		b.append("\n\t=====================\n");
		log.info(b.toString());
	}
	
	
	/**
	 * Handles the kafka consumer
	 */
	@Override
	public void run() {
		log.info("Kafka Subscription Thread Started");
		try {
            consumer.subscribe(Arrays.asList(topics), this);            
            while (!closed.get()) {
            	try {
	                final ConsumerRecords<String, ByteBuf> records;
	                try {
	                	records = consumer.poll(pollTimeout);	                	
	                	final Context ctx = perMessageTimer.time();
	                	final int recordCount = records.count();	                	
	                	if(recordCount > 0) {
	                		log.info("Polled {} messages:", records.count());
	                		int i = 0;
	                		
	                		for(final Iterator<ConsumerRecord<String, ByteBuf>> iter = records.iterator(); iter.hasNext();) {
	                			i++;
	                			final ConsumerRecord<String, ByteBuf> record = iter.next();
	                			final ByteBuf b = record.value();
	                			log.debug("Polled Record #{}: {} bytes", i, b.readableBytes());
	                			messageQueue.writeEntry(b);
	                			
	                		}
	                		final long st = System.currentTimeMillis();
	                		
	                		log.info("Wrote [{}] records to MessageQueue in [{}] ms.", recordCount, System.currentTimeMillis()-st);
	                	}
	                	if(syncAdd) {
	                		consumer.commitAsync();
	                	} else {
	                		consumer.commitAsync();			// FIXME:  this will break at some point
	                	}
	                	ctx.stop();
	                } catch (Exception ex) {
	                	deserErrors.inc();
	                	continue;  
	                }
            	} catch (Exception ex) {
            		log.error("Unexpected Exception1", ex);
            	}
            }
		} catch (Exception ex) {
			log.error("Unexpected Exception2", ex);
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.chronicle.MessageListener#onMetric(io.netty.buffer.ByteBuf)
	 */
	@Override
	public void onMetric(final ByteBuf buf) {
		log.debug("OnMetric Buffer: {} bytes", buf.readableBytes());
		try {			
			final List<Deferred<Object>> addPointDeferreds = new ArrayList<Deferred<Object>>();
			int recordCount = 0;
			int totalCount = 0;
			int totalBlacklisted = 0;
			final long startTimeNanos = System.nanoTime();
			try {
				final Iterator<StreamedMetricValue> iter = StreamedMetricValue.streamedMetricValues(true, buf, true).iterator();
				while(iter.hasNext()) {
					final StreamedMetricValue smv = iter.next();
					
					totalCount++;
					try {
						if(blacklist.isBlackListed(smv.metricKey())) {
							totalBlacklisted++;
							continue;
						}
						Deferred<Object> thisDeferred = null;
						if(smv.isDoubleValue()) {
							thisDeferred = tsdb.addPoint(smv.getMetricName(), smv.getTimestamp(), smv.getDoubleValue(), smv.getTags());
						} else {
							thisDeferred = tsdb.addPoint(smv.getMetricName(), smv.getTimestamp(), smv.getLongValue(), smv.getTags());
						}
						pendingDataPointAdds.increment();
						recordCount++;
						addPointDeferreds.add(thisDeferred);  // keep all the deferreds so we can wait on them
					} catch (Exception adpe) {
						if(smv != null) {
							log.error("Failed to add data point for invalid metric name: {}, cause: {}", smv.metricKey(), adpe.getMessage());
							blacklist.blackList(smv.metricKey());
						}
						log.error("Failed to process StreamedMetricValue", adpe);
					}
				}
				log.debug("Async Writes Complete. total-reads: {}, total-writes: {}, blacklisted: {}", totalCount, recordCount, totalBlacklisted);
			} catch (Exception ex) {
				log.error("BufferIteration Failure on read #" + totalCount, ex);
			}
			final long readAndWriteTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNanos);					
			log.info("Read [{}] total metrics and wrote [{}] to OpenTSDB in [{}] ms.", totalCount, recordCount, readAndWriteTime);
			Deferred<ArrayList<Object>> d = Deferred.group(addPointDeferreds);
			final int rcount = recordCount;
			d.addCallback(new Callback<Void, ArrayList<Object>>() {
				@Override
				public Void call(final ArrayList<Object> arg) throws Exception {
					pendingDataPointAdds.add(-1 * rcount);
					final long elapsed = System.nanoTime() - startTimeNanos; 
					perMessageTimer.update(nanosPerMessage(elapsed, rcount), TimeUnit.NANOSECONDS);
					pointsAddedMeter.mark(rcount);
					if(syncAdd) log.info("Sync Processed {} records in {} ms. Pending: {}", rcount, TimeUnit.NANOSECONDS.toMillis(elapsed), pendingDataPointAdds.longValue());							
					return null;
				}                		 
			});
			if(syncAdd) {
				try {
					d.joinUninterruptibly(syncAddTimeout);
				} catch (Exception ex) {
					putTimeouts.inc(rcount);
					log.warn("Datapoints Write Timed Out");
				}
			} else {
				final long elapsed = System.nanoTime() - startTimeNanos; 
				perMessageTimer.update(nanosPerMessage(elapsed, recordCount), TimeUnit.NANOSECONDS);
				pointsAddedMeter.mark(recordCount);
				log.debug("Async Processed {} records in {} ms. Pending: {}", recordCount, TimeUnit.NANOSECONDS.toMillis(elapsed), pendingDataPointAdds.longValue());							
			}
			
		} finally {
			try { buf.release(); } catch (Exception ex) {/* No Op */}
		}
	}
	

	
	
	
	/**
	 * Computes the nano time per message
	 * @param nanosElapsed The nanos elapsed for the whole batch
	 * @param messageCount The number of messages in the batch
	 * @return the nanos per message
	 */
	protected static long nanosPerMessage(final double nanosElapsed, final double messageCount) {
		if(nanosElapsed==0 || messageCount==0) return 0;
		return (long)(nanosElapsed/messageCount);
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RpcPlugin#shutdown()
	 */
	@Override
	public Deferred<Object> shutdown() {
		final Deferred<Object> d = new Deferred<Object>();
		if(subThread!=null) {
			final Thread t = new Thread(getClass().getSimpleName() + "ShutdownThread") {
				@Override
				public void run() {
					try {
						subThread.join();
						try { 
							consumer.close();
							log.info("Kafka Consumer Closed");
						} catch (Exception x) {/* No Op */}
						log.info("KafkaRPC Shutdown Cleanly");
					} catch (Exception ex) {
						log.error("KafkaRPC Dirty Shutdown:" + ex);
					} finally {
						d.callback(null);
					}
				}
			};
			t.setDaemon(true);
			t.start();
		} else {
			log.warn("KafkaRPC Sub Thread Not Running on Shutdown");
			d.callback(null);
		}
	     closed.set(true);
	     consumer.wakeup();		
		return d;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RpcPlugin#version()
	 */
	@Override
	public String version() {
		return "2.1";
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.KafkaRPCMBean#isSyncAdd()
	 */
	@Override
	public boolean isSyncAdd() {
		return syncAdd;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.KafkaRPCMBean#getTotalDataPoints()
	 */
	@Override
	public long getTotalDataPoints() {
		return perMessageTimer.getCount();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.KafkaRPCMBean#getAssignedPartitions()
	 */
	@Override
	public Set<String> getAssignedPartitions() {
		return Collections.unmodifiableSet(assignedPartitions);
	}

	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.KafkaRPCMBean#getDataPointsMeanRate()
	 */
	@Override
	public double getDataPointsMeanRate() {
		return perMessageTimer.getMeanRate();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.KafkaRPCMBean#getDataPoints15mRate()
	 */
	@Override
	public double getDataPoints15mRate() {
		return perMessageTimer.getFifteenMinuteRate();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.KafkaRPCMBean#getDataPoints5mRate()
	 */
	@Override
	public double getDataPoints5mRate() {
		return perMessageTimer.getFiveMinuteRate();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.KafkaRPCMBean#getDataPoints1mRate()
	 */
	@Override
	public double getDataPoints1mRate() {
		return perMessageTimer.getOneMinuteRate();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.KafkaRPCMBean#getPerDataPointMeanTimeMs()
	 */
	@Override
	public double getPerDataPointMeanTimeMs() {
		return perMessageTimerSnap.getValue().getMean();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.KafkaRPCMBean#getPerDataPointMedianTimeMs()
	 */
	@Override
	public double getPerDataPointMedianTimeMs() {
		return perMessageTimerSnap.getValue().getMedian();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.KafkaRPCMBean#getPerDataPoint999pctTimeMs()
	 */
	@Override
	public double getPerDataPoint999pctTimeMs() {
		return perMessageTimerSnap.getValue().get999thPercentile();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.KafkaRPCMBean#getPerDataPoint99pctTimeMs()
	 */
	@Override
	public double getPerDataPoint99pctTimeMs() {
		return perMessageTimerSnap.getValue().get99thPercentile();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.KafkaRPCMBean#getPerDataPoint75pctTimeMs()
	 */
	@Override
	public double getPerDataPoint75pctTimeMs() {
		return perMessageTimerSnap.getValue().get75thPercentile();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.KafkaRPCMBean#getBatchMeanRate()
	 */
	@Override
	public double getBatchMeanRate() {
		return pointsAddedMeter.getMeanRate();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.KafkaRPCMBean#getBatch15mRate()
	 */
	@Override
	public double getBatch15mRate() {
		return pointsAddedMeter.getFifteenMinuteRate();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.KafkaRPCMBean#getBatch5mRate()
	 */
	@Override
	public double getBatch5mRate() {
		return pointsAddedMeter.getFiveMinuteRate();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.KafkaRPCMBean#getBatch1mRate()
	 */
	@Override
	public double getBatch1mRate() {
		return pointsAddedMeter.getOneMinuteRate();
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.KafkaRPCMBean#getPendingDataPointAdds()
	 */
	@Override
	public long getPendingDataPointAdds() {
		return pendingDataPointAdds.longValue();
	}
	

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RpcPlugin#collectStats(net.opentsdb.stats.StatsCollector)
	 */
	@Override
	public void collectStats(final StatsCollector collector) {
		metricManager.collectStats(collector);
	}



	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsRevoked(java.util.Collection)
	 */
	@Override
	public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
		if(!partitions.isEmpty()) {
			final StringBuilder b = new StringBuilder("\n\t===========================\n\tPARTITIONS REVOKED !!\n\t===========================");
			for(TopicPartition tp: partitions) {
				b.append("\n\t").append(tp);
				assignedPartitions.remove(tp.toString());
			}
			b.append("\n\t===========================\n");
			if(assignedPartitions.isEmpty()) {
				log.warn(b.toString());
			} else {
				log.info(b.toString());
			}
		}
	}



	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsAssigned(java.util.Collection)
	 */
	@Override
	public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
		startLatch.countDown();
		final StringBuilder b = new StringBuilder("\n\t===========================\n\tPARTITIONS ASSIGNED\n\t===========================");
		for(TopicPartition tp: partitions) {
			b.append("\n\t").append(tp);
			assignedPartitions.add(tp.toString());
		}
		b.append("\n\t===========================\n");
		log.info(b.toString());

		
	}



	/**
	 * Returns the sync add data points timeout in ms. 
	 * @return the sync add data points timeout in ms.
	 */
	@Override
	public long getSyncAddTimeout() {
		return syncAddTimeout;
	}



	/**
	 * Sets the sync add data points timeout in ms.
	 * @param syncAddTimeout the sync add data points timeout in ms.
	 */
	@Override
	public void setSyncAddTimeout(final long syncAddTimeout) {
		if(syncAddTimeout < 1) throw new IllegalArgumentException("Invalid sync add timeout [" + syncAddTimeout + "]");
		this.syncAddTimeout = syncAddTimeout;
	}



	/**
	 * Returns the cummulative number of TSDB put timeout errors
	 * @return the cummulative number of TSDB put timeout errors
	 */
	@Override
	public Counter getPutTimeouts() {
		return putTimeouts;
	}



	/**
	 * Returns the cummulative number of metric deserialization errors
	 * @return the cummulative number of metric deserialization errors
	 */
	@Override
	public Counter getDeserErrors() {
		return deserErrors;
	}



	/**
	 * Returns the timeout on waiting for Kafka partition info on startup in secs.
	 * @return the timeout on waiting for Kafka partition info on startup in secs.
	 */
	@Override
	public int getKafkaStartupTimeout() {
		return kafkaStartupTimeout;
	}




}
