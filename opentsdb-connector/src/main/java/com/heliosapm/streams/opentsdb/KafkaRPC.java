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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.heliosapm.streams.metrics.StreamedMetricValue;
import com.heliosapm.streams.metrics.StreamedMetricValueDeserializer;
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

public class KafkaRPC extends RpcPlugin implements Runnable {
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
		log.info("Kafka Subscription Thread Started");
	}
	
	
	/**
	 * Handles the kafka consumer
	 */
	public void run() {
		try {
            consumer.subscribe(Arrays.asList(topics));
            while (!closed.get()) {
                final ConsumerRecords<String, StreamedMetricValue> records = consumer.poll(pollTimeout);
                final int recordCount = records.count();
                if(recordCount==0) continue;
                final long startTime = System.currentTimeMillis();
                
                final List<Deferred<Object>> addPointDeferreds = !syncAdd ? new ArrayList<Deferred<Object>>(recordCount) : null;
                for(final Iterator<ConsumerRecord<String, StreamedMetricValue>> iter = records.iterator(); iter.hasNext();) {
                	final ConsumerRecord<String, StreamedMetricValue> record = iter.next();
                	final StreamedMetricValue im = record.value();
                	Deferred<Object> thisDeferred = null;
                	if(im.isDoubleValue()) {
                		thisDeferred = tsdb.addPoint(im.getMetricName(), im.getTimestamp(), im.getDoubleValue(), im.getTags());
                	} else {
                		thisDeferred = tsdb.addPoint(im.getMetricName(), im.getTimestamp(), im.getLongValue(), im.getTags());
                	}
                	if(syncAdd) addPointDeferreds.add(thisDeferred);              	
                }
                if(!syncAdd) {
                	 Deferred<ArrayList<Object>> d = Deferred.group(addPointDeferreds);
                	 d.addBoth(new Callback<Void, ArrayList<Object>>() {
						@Override
						public Void call(final ArrayList<Object> arg) throws Exception {
							consumer.commitSync();
							return null;
						}                		 
                	 });                	 
                } else {
                	consumer.commitSync();                	
                }
                log.info("sync:{} :Processed {} records in {} ms.", syncAdd, recordCount, System.currentTimeMillis() - startTime );
            }
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
	 * @see net.opentsdb.tsd.RpcPlugin#collectStats(net.opentsdb.stats.StatsCollector)
	 */
	@Override
	public void collectStats(final StatsCollector collector) {

	}

}
