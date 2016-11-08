/**
 * Helios, OpenSource Monitoring
 * Brought to you by the Helios Development Group
 *
 * Copyright 2016, Helios Development Group and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org. 
 *
 */
package com.heliosapm.streams.onramp;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.ValueType;
import com.heliosapm.streams.common.metrics.SharedMetricsRegistry;
import com.heliosapm.utils.lang.StringHelper;

/**
 * <p>Title: MessageForwarder</p>
 * <p>Description: Wraps a Kafka message producer and factory</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.onramp.MessageForwarder</code></p>
 */

public class MessageForwarder implements Producer<String, StreamedMetric> {
	/** The singleton instance */
	private static volatile MessageForwarder instance = null;
	/** The singleton instance ctor lock */
	private static final Object lock = new Object();

	/** Instance logger */
	private final Logger log = LogManager.getLogger(getClass());
	/** The kafka producer properties */
	private final Properties kafkaConfig = new Properties();
	/** The wrapped Kafka producer */
	private final Producer<String, StreamedMetric> producer;
	/** Indicates if the producer is open */
	private final AtomicBoolean open = new AtomicBoolean(false);
	
	/** A counter of sent messages */
	private final Timer sendMessage = SharedMetricsRegistry.getInstance().timer("forwarder.message.send");
	/** A counter of sent messages */
	private final Counter sendCounter = SharedMetricsRegistry.getInstance().counter("forwarder.messages.sent");
	
	/** A counter of message send drops */
	private final Counter droppedMessages = SharedMetricsRegistry.getInstance().counter("forwarder.messages.drop.count");

	
	/**
	 * Initializes the MessageForwarder singleton instance
	 * @param appConfig The provided configuration
	 * @return the MessageForwarder singleton instance
	 */
	static MessageForwarder initialize(final Properties appConfig) {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					instance = new MessageForwarder(appConfig);
				}
			}
		}
		return instance;
	}
	
	/**
	 * Acquires and returns the MessageForwarder singleton instance
	 * @return the MessageForwarder singleton instance
	 */
	public static MessageForwarder getInstance() {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					throw new IllegalStateException("The MessageForwarder has not been initialized");
				}
			}
		}
		return instance;
		
	}
	
	/**
	 * Stops this service
	 */
	static void stop() {
		instance.log.info(">>>>> Closing MessageForwarder...");
		instance.open.set(false);
		try { instance.producer.flush(); } catch (Exception x) {/* No Op */}
		try { instance.producer.close(); } catch (Exception x) {/* No Op */}
		instance.log.info("<<<<< MessageForwarder Closed.");
	}
	

	/**
	 * Creates a new MessageForwarder
	 * @param appConfig The provided configuration
	 */
	private MessageForwarder(final Properties appConfig) {
		log.info(">>>>> MessageForwarder Starting...");
		if(appConfig==null || appConfig.isEmpty()) throw new IllegalArgumentException("The passed properties were null or empty");
		final StringBuilder b = new StringBuilder("\n\tKafka Producer Properties\n\t==================================");
		for(final String key: appConfig.stringPropertyNames()) {
			final String[] frags = StringHelper.splitString(key, '.', true);
			
			if("KAFKA".equalsIgnoreCase(frags[0])) {
				final String kkey = key.substring(6).trim();
				kafkaConfig.put(kkey, appConfig.get(key));
				b.append("\n\t").append(kkey).append(" : ").append(appConfig.get(key));
			}
		}
		b.append("\n\t==================================");
		producer = new KafkaProducer<String, StreamedMetric>(kafkaConfig);
		open.set(true);
		log.info("<<<<< Started MessageForwarder.");
	}


	/**
	 * Send the given record asynchronously and return a future which will eventually contain the response information.
	 * @param record The record to send
	 * @return A future which will eventually contain the response information
	 * @see org.apache.kafka.clients.producer.Producer#send(org.apache.kafka.clients.producer.ProducerRecord)
	 */
	@Override
	public Future<RecordMetadata> send(final ProducerRecord<String, StreamedMetric> record) {
		final Context ctx = sendMessage.time();
		try { 
			return producer.send(record);
		} finally {
			sendCounter.inc();
			ctx.stop();
		}
	}
	
	/**
	 * Sends a metric passed in string form to the specified topic
	 * @param metric The metric to send
	 * @param callback an optional callback 
	 */
	public void send(final String metric, final Callback callback) {
		final StreamedMetric sm;
		try {
			sm = StreamedMetric.fromString(metric);
		} catch (Exception ex) {
			droppedMessages.inc();
			return;
		}
		final ValueType vt = sm.getValueType();
		final String topic = vt==null ? ValueType.STRAIGHTTHROUGH.topicName : vt.topicName;
		send(new ProducerRecord<String, StreamedMetric>(topic, sm.getMetricName(), sm), callback);
	}
	
	/**
	 * Sends a metric passed in string form to the specified topic
	 * @param metric The metric to send
	 */
	public void send(final String metric) {
		send(metric, (Callback)null);
	}
	

	
	/**
	 * Sends an array of metrics to the specified topic
	 * @param topic The topic to send to
	 * @param metrics The metrics to send
	 * @return a collection of the futures, each of which will eventually contain the response information
	 */
	public Collection<Future<RecordMetadata>> send(final String topic, final StreamedMetric...metrics) {
		final Set<Future<RecordMetadata>> futures = new LinkedHashSet<Future<RecordMetadata>>(metrics.length);
		for(final StreamedMetric sm: metrics) {
			futures.add(send(new ProducerRecord<String, StreamedMetric>(topic, sm.getMetricName(), sm)));
		}
		return futures;
	}
	
	/**
	 * Sends an array of metrics to the default topic
	 * @param metrics The metrics to send
	 * @return a collection of the futures, each of which will eventually contain the response information
	 */
	public Collection<Future<RecordMetadata>> send(final StreamedMetric...metrics) {
		final Set<Future<RecordMetadata>> futures = new LinkedHashSet<Future<RecordMetadata>>(metrics.length);
		for(final StreamedMetric sm: metrics) {
			final ValueType vt = sm.getValueType();
			if(vt==null) continue;			
			futures.add(send(new ProducerRecord<String, StreamedMetric>(sm.getValueType().topicName, sm.getMetricName(), sm)));
		}
		return futures;		
	}
	


	/**
	 * Send a record and invoke the given callback when the record has been acknowledged by the server
	 * @param record The record to send
	 * @param callback The callback that will be invoked when the server acknowledges
	 * @return A future which will eventually contain the response information
	 * @see org.apache.kafka.clients.producer.Producer#send(org.apache.kafka.clients.producer.ProducerRecord, org.apache.kafka.clients.producer.Callback)
	 */
	@Override
	public Future<RecordMetadata> send(final ProducerRecord<String, StreamedMetric> record, final Callback callback) {
		final Context ctx = sendMessage.time();
		try { 
			return producer.send(record, callback);
		} finally {
			sendCounter.inc();
			ctx.stop();
		}
	}


	/**
	 * Flush any accumulated records from the producer.
	 * @see org.apache.kafka.clients.producer.Producer#flush()
	 */
	@Override
	public void flush() {
		producer.flush();
	}


	/**
	 * Get a list of partitions for the given topic for custom partition assignment. 
	 * The partition metadata will change over time so this list should not be cached.
	 * @param topic The topic name
	 * @return the partition data
	 * @see org.apache.kafka.clients.producer.Producer#partitionsFor(java.lang.String)
	 */
	@Override
	public List<PartitionInfo> partitionsFor(final String topic) {
		return producer.partitionsFor(topic);
	}


	/**
	 * Return a map of metrics maintained by the producer
	 * @return a map of metrics maintained by the producer
	 * @see org.apache.kafka.clients.producer.Producer#metrics()
	 */
	@Override
	public Map<MetricName, ? extends Metric> metrics() {
		return producer.metrics();
	}


	/**
	 * This is a No Op
	 * @see org.apache.kafka.clients.producer.Producer#close()
	 */
	@Override
	public void close() {
		/* No Op */
	}


	/**
	 * This is a No Op
	 * @param timeout
	 * @param unit
	 * @see org.apache.kafka.clients.producer.Producer#close(long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public void close(final long timeout, final TimeUnit unit) {
		/* No Op */
	}

}
