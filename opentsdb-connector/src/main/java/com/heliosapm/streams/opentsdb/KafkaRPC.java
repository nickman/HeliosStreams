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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.heliosapm.streams.metrics.Blacklist;
import com.heliosapm.streams.metrics.StreamedMetricValue;
import com.heliosapm.streams.metrics.StreamedMetricValueDeserializer;
import com.heliosapm.streams.metrics.internal.SharedMetricsRegistry;
import com.heliosapm.utils.jmx.JMXHelper;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

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

public class KafkaRPC extends RpcPlugin implements KafkaRPCMBean, Runnable {
	/** The TSDB instance */
	static TSDB tsdb = null;
	/** The prefix on TSDB config items marking them as applicable to this service */
	public static final String CONFIG_PREFIX = "tsd.rpc.kafka.";
	/** The prefix length */
	public static final int CONFIG_PREFIX_LEN = CONFIG_PREFIX.length();
	/** The TSDB config key for the names of topics to subscribe to */
	public static final String CONFIG_TOPICS = CONFIG_PREFIX + "tsdbtopics";
	/** The default topic name to listen on */
	public static final String DEFAULT_TOPIC = "tsdb-metrics";
	/** The TSDB config key for sync processing of add data points */
	public static final String CONFIG_SYNC_ADD = CONFIG_PREFIX + "syncadd";
	/** The default sync add */
	public static final boolean DEFAULT_SYNC_ADD = true;
	
	/** The TSDB config key for the polling timeout in ms. */
	public static final String CONFIG_POLLTIMEOUT = CONFIG_PREFIX + "polltime";
	/** The default polling timeout in ms. */
	public static final long DEFAULT_POLLTIMEOUT = 10000;
	
	/** Instance logger */
	protected final Logger log = LoggerFactory.getLogger(getClass());
	/** The kafka consumer client configuration */
	protected final Properties consumerConfig = new Properties();
	/** The kafka consumer */
	protected KafkaConsumer<String, StreamedMetricValue> consumer = null;
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
	
	/** The blacklisted metric key manager */
	protected final Blacklist blacklist = Blacklist.getInstance();
	
	
	/** A meter to track the rate of points added */
	protected final Meter pointsAddedMeter = SharedMetricsRegistry.getInstance().meter("KafkaRPC.pointsAddedMeter");
	/** A timer to track the elapsed time per message ingested */
	protected final Timer perMessageTimer = SharedMetricsRegistry.getInstance().timer("KafkaRPC.perMessageTimer");
	
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
	}
	
	

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RpcPlugin#initialize(net.opentsdb.core.TSDB)
	 */
	@Override
	public void initialize(final TSDB tsdb) {	
		KafkaRPC.tsdb = tsdb;
		final Map<String, String> config = tsdb.getConfig().getMap();
		final StringBuilder b = new StringBuilder("\n\tKafkaRPC Configuration\n\t======================");
		for(Map.Entry<String, String> entry: config.entrySet()) {
			final String tsdbKey = entry.getKey();
			if(tsdbKey.indexOf(CONFIG_PREFIX)==0) {
				final String key = tsdbKey.substring(CONFIG_PREFIX_LEN);
				final String value = entry.getValue();
				b.append("\n\t").append(key).append("=").append(value);
				consumerConfig.setProperty(key, entry.getValue());
			}
		}
		b.append("\n\t======================");
		log.info(b.toString());
		final String topicStr = config.get(CONFIG_TOPICS);
		if(topicStr==null || topicStr.trim().isEmpty()) {
			topics = new String[]{DEFAULT_TOPIC};
		} else {
			topics = topicStr.replace(" ", "").split(",");
		}
		if(tsdb.getConfig().hasProperty(CONFIG_POLLTIMEOUT)) {
			try {
				pollTimeout = tsdb.getConfig().getInt(CONFIG_POLLTIMEOUT);
			} catch (Exception ex) {
				pollTimeout = DEFAULT_POLLTIMEOUT;
			}
		}
		if(tsdb.getConfig().hasProperty(CONFIG_SYNC_ADD)) {
			try {
				syncAdd = tsdb.getConfig().getBoolean(CONFIG_SYNC_ADD);
			} catch (Exception ex) {
				syncAdd = DEFAULT_SYNC_ADD;
			}
		}
		
		log.info("Kafka TSDB Metric Topics: {}", Arrays.toString(topics));
		log.info("Kafka TSDB Poll Size: {}", pollTimeout);
		consumer = new KafkaConsumer<String, StreamedMetricValue>(consumerConfig, new StringDeserializer(), new StreamedMetricValueDeserializer());
		subThread = new Thread(this, "KafkaSubscriptionThread");
		subThread.start();
		JMXHelper.registerMBean(this, OBJECT_NAME);
	}
	
	
	/**
	 * Handles the kafka consumer
	 */
	public void run() {
		log.info("Kafka Subscription Thread Started");
		try {
            consumer.subscribe(Arrays.asList(topics));
            while (!closed.get()) {
            	try {
	                final ConsumerRecords<String, StreamedMetricValue> records = consumer.poll(pollTimeout);
	                final int recordCount = records.count();
	                if(recordCount==0) continue;
	                final long startTimeNanos = System.nanoTime();
	                
	                final List<Deferred<Object>> addPointDeferreds = syncAdd ? 
	                		new ArrayList<Deferred<Object>>(recordCount)  	// We're going to wait for all the Deferreds to complete and then commit.
	                		: null;											// We're just going to commit as soon as all the async add points are dispatched
	                for(final Iterator<ConsumerRecord<String, StreamedMetricValue>> iter = records.iterator(); iter.hasNext();) {
	                	StreamedMetricValue im = null;
	                	try {
		                	final ConsumerRecord<String, StreamedMetricValue> record = iter.next();
		                	im = record.value();
		                	if(blacklist.isBlackListed(im.metricKey())) continue;
		                	Deferred<Object> thisDeferred = null;
		                	if(im.isDoubleValue()) {
		                		thisDeferred = tsdb.addPoint(im.getMetricName(), im.getTimestamp(), im.getDoubleValue(), im.getTags());
		                	} else {
		                		thisDeferred = tsdb.addPoint(im.getMetricName(), im.getTimestamp(), im.getLongValue(), im.getTags());
		                	}
		                	if(syncAdd) addPointDeferreds.add(thisDeferred);  // keep all the deferreds so we can wait on them
	                	} catch (Exception adpe) {
	                		if(im!=null) {
	                			log.error("Failed to add data point for metric: {}", im.metricKey(), adpe);
	                			blacklist.blackList(im.metricKey());
	                		} else {
	                			log.error("Failed to add data point", adpe);
	                		}
	                	}
	                }
	                if(syncAdd) {
	                	 Deferred<ArrayList<Object>> d = Deferred.group(addPointDeferreds);
	                	 d.addBoth(new Callback<Void, ArrayList<Object>>() {
							@Override
							public Void call(final ArrayList<Object> arg) throws Exception {
								consumer.commitSync();
								final long elapsed = System.nanoTime() - startTimeNanos; 
								perMessageTimer.update(nanosPerMessage(elapsed, recordCount), TimeUnit.NANOSECONDS);
								pointsAddedMeter.mark(recordCount);
								log.info("Sync Processed {} records in {} ms.", recordCount, TimeUnit.NANOSECONDS.toMillis(elapsed));							
								return null;
							}                		 
	                	 });                	 
	                } else {
	                	consumer.commitSync();
						final long elapsed = System.nanoTime() - startTimeNanos; 
						perMessageTimer.update(nanosPerMessage(elapsed, recordCount), TimeUnit.NANOSECONDS);
						pointsAddedMeter.mark(recordCount);
						log.info("Async Processed {} records in {} ms.", recordCount, TimeUnit.NANOSECONDS.toMillis(elapsed));							
	                }       
            	} catch (Exception exx) {
            		log.error("Unexpected exception in processing loop", exx);
            	}
            }  // end of while loop
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!closed.get()) throw e;
        } catch (Exception ex) {
        	log.error("Failed to start subscription", ex);
        } finally {
            try { consumer.close(); } catch (Exception x) {/* No Op */}
            subThread = null;
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
	public boolean isSyncAdd() {
		return syncAdd;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.KafkaRPCMBean#getTotalDataPoints()
	 */
	public long getTotalDataPoints() {
		return perMessageTimer.getCount();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.KafkaRPCMBean#getDataPointsMeanRate()
	 */
	public double getDataPointsMeanRate() {
		return perMessageTimer.getMeanRate();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.KafkaRPCMBean#getDataPoints15mRate()
	 */
	public double getDataPoints15mRate() {
		return perMessageTimer.getFifteenMinuteRate();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.KafkaRPCMBean#getDataPoints5mRate()
	 */
	public double getDataPoints5mRate() {
		return perMessageTimer.getFiveMinuteRate();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.KafkaRPCMBean#getDataPoints1mRate()
	 */
	public double getDataPoints1mRate() {
		return perMessageTimer.getOneMinuteRate();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.KafkaRPCMBean#getPerDataPointMeanTimeMs()
	 */
	public double getPerDataPointMeanTimeMs() {
		return perMessageTimerSnap.getValue().getMean();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.KafkaRPCMBean#getPerDataPointMedianTimeMs()
	 */
	public double getPerDataPointMedianTimeMs() {
		return perMessageTimerSnap.getValue().getMedian();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.KafkaRPCMBean#getPerDataPoint999pctTimeMs()
	 */
	public double getPerDataPoint999pctTimeMs() {
		return perMessageTimerSnap.getValue().get999thPercentile();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.KafkaRPCMBean#getPerDataPoint99pctTimeMs()
	 */
	public double getPerDataPoint99pctTimeMs() {
		return perMessageTimerSnap.getValue().get99thPercentile();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.KafkaRPCMBean#getPerDataPoint75pctTimeMs()
	 */
	public double getPerDataPoint75pctTimeMs() {
		return perMessageTimerSnap.getValue().get75thPercentile();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.KafkaRPCMBean#getBatchMeanRate()
	 */
	public double getBatchMeanRate() {
		return pointsAddedMeter.getMeanRate();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.KafkaRPCMBean#getBatch15mRate()
	 */
	public double getBatch15mRate() {
		return pointsAddedMeter.getFifteenMinuteRate();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.KafkaRPCMBean#getBatch5mRate()
	 */
	public double getBatch5mRate() {
		return pointsAddedMeter.getFiveMinuteRate();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.KafkaRPCMBean#getBatch1mRate()
	 */
	public double getBatch1mRate() {
		return pointsAddedMeter.getOneMinuteRate();
	}
	

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RpcPlugin#collectStats(net.opentsdb.stats.StatsCollector)
	 */
	@Override
	public void collectStats(final StatsCollector collector) {
		try {			
			collector.addExtraTag("mode", syncAdd ? "sync" : "async");
			collector.record("tsd.rpc.kafka.messages.rate.mean", perMessageTimer.getMeanRate());
			collector.record("tsd.rpc.kafka.messages.count", perMessageTimer.getCount());
			collector.record("tsd.rpc.kafka.messages.rate.15m", perMessageTimer.getFifteenMinuteRate());
			collector.record("tsd.rpc.kafka.messages.rate.5m", perMessageTimer.getFiveMinuteRate());
			collector.record("tsd.rpc.kafka.messages.rate.1m", perMessageTimer.getOneMinuteRate());
			
			final Snapshot snap = perMessageTimer.getSnapshot();
			
			collector.record("tsd.rpc.kafka.messages.time.mean", snap.getMean());
			collector.record("tsd.rpc.kafka.messages.time.median", snap.getMedian());
			collector.record("tsd.rpc.kafka.messages.time.p999", snap.get999thPercentile());
			collector.record("tsd.rpc.kafka.messages.time.p99", snap.get99thPercentile());
			collector.record("tsd.rpc.kafka.messages.time.p75", snap.get75thPercentile());
			
			collector.record("tsd.rpc.kafka.batches.rate.mean", pointsAddedMeter.getMeanRate());
			collector.record("tsd.rpc.kafka.batches.rate.15m", pointsAddedMeter.getFifteenMinuteRate());
			collector.record("tsd.rpc.kafka.batches.rate.5m", pointsAddedMeter.getFiveMinuteRate());
			collector.record("tsd.rpc.kafka.batches.rate.1m", pointsAddedMeter.getOneMinuteRate());
		} finally {
			collector.clearExtraTag("mode");
		}
	}

}
