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
package com.heliosapm.streams.common.kafka.producer;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.heliosapm.streams.common.metrics.SharedMetricsRegistry;
import com.heliosapm.utils.collections.Props;

import jsr166e.AccumulatingLongAdder;
import jsr166e.LongAdder;



/**
 * <p>Title: KafkaProducerService</p>
 * <p>Description: A shared kafka producer</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.common.kafka.producer.KafkaProducerService</code></p>
 * @param <K> The key type
 * @param <V> The message type
 */

public class KafkaProducerService<K, V> implements Producer<K, V> {
	/** The singleton instance */
	private static volatile KafkaProducerService<?, ?> instance = null;
	/** The singleton instance ctor lock */
	private static final Object lock = new Object();
	
	/** The prefix on config items marking them as applicable to this service */
	public static final String CONFIG_PREFIX = "kafka.producer.";
	
	/** Placeholder long adder */
	private static final LongAdder PLACEHOLDER = new LongAdder();
	
	
	/** The wrapped Kafka producer */
	private final Producer<K,V> producer;
	/** Instance logger */
	private final Logger log = LogManager.getLogger(getClass());

	/** The producer configuration */
	private final Properties producerProperties;
	
	/** Counters for the number of messages sent to each topic, keyed by the topic name */
	protected final NonBlockingHashMap<String, LongAdder> sentMessageCounter = new NonBlockingHashMap<String, LongAdder>();
	/** A counter for the total number of messages sent to all topics */
	protected final LongAdder totalSentMessages = new LongAdder();
	
	
	/** Indicates if the producer is open */
	private final AtomicBoolean open = new AtomicBoolean(false);
	
	/** A timer for sent messages */
	private final Timer sendMessage = SharedMetricsRegistry.getInstance().timer("producer.message.send");
	/** A timer for sent message response times */
	private final Timer sendMessageResponse = SharedMetricsRegistry.getInstance().timer("producer.message.roundtrip");
	
	/** A counter of sent messages */
	private final Counter sendCounter = SharedMetricsRegistry.getInstance().counter("producer.messages.sent");	
	/** A counter of message send drops */
	private final Counter droppedMessages = SharedMetricsRegistry.getInstance().counter("producer.messages.drop.count");
	
	/**
	 * Acquires, initializes and returns the singleton KafkaProducerService
	 * @param p The configuration properties
	 * @return the singleton KafkaProducerService
	 * @param <K> The key type
	 * @param <V> The message type
	 */	
	@SuppressWarnings("unchecked")
	public static <K,V> KafkaProducerService<K,V> getInstance(final Properties p) {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					instance = new KafkaProducerService<K,V>(p); 
				}
			}
		}
		return (KafkaProducerService<K, V>) instance;
	}
	
	/**
	 * Acquires and returns the KafkaProducerService singleton instance
	 * @return the KafkaProducerService singleton instance
	 * @param <K> The key type
	 * @param <V> The message type
	 */	
	@SuppressWarnings("unchecked")
	public static <K,V> KafkaProducerService<K,V> getInstance() {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					throw new IllegalStateException("The KafkaService has not been initialized yet"); 
				}
			}
		}
		return (KafkaProducerService<K, V>) instance;
	}
	
	/**
	 * Stops this service
	 */
	static void stop() {
		instance.log.info(">>>>> Closing KafkaProducerService...");
		instance.open.set(false);
		try { instance.producer.flush(); } catch (Exception x) {/* No Op */}
		try { instance.producer.close(); } catch (Exception x) {/* No Op */}
		instance.log.info("<<<<< KafkaProducerService Closed.");
	}
	
	
	private KafkaProducerService(final Properties config) {
		producerProperties = Props.extract("kafka.producer.", config, true, true);
		producer = new KafkaProducer<K, V>(producerProperties);
	}
	
	
	/**
	 * Returns the counter for the passed topic
	 * @param topicName The name of the topic to get a counter for
	 * @return the counter
	 */
	protected LongAdder getTopicCounter(final String topicName) {
		LongAdder la = sentMessageCounter.putIfAbsent(topicName, PLACEHOLDER);
		if(la==null || la==PLACEHOLDER) {
			la = new AccumulatingLongAdder(totalSentMessages);
			sentMessageCounter.replace(topicName, la);
		}
		return la;
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.clients.producer.Producer#send(org.apache.kafka.clients.producer.ProducerRecord)
	 */
	@Override
	public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
		return send(record, null);
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.clients.producer.Producer#send(org.apache.kafka.clients.producer.ProducerRecord, org.apache.kafka.clients.producer.Callback)
	 */
	@Override
	public Future<RecordMetadata> send(final ProducerRecord<K, V> record, final Callback callback) {
		final long startTime = System.nanoTime();		
		try { 
			final Future<RecordMetadata> f = producer.send(record, new Callback(){
				@Override
				public void onCompletion(final RecordMetadata metadata, final Exception exception) {
					sendMessageResponse.update(System.nanoTime()-startTime, TimeUnit.NANOSECONDS);
					if(callback!=null) {
						callback.onCompletion(metadata, exception);
					}
				}
			});
			sendCounter.inc();
			sendMessage.update(System.nanoTime()-startTime, TimeUnit.NANOSECONDS);
			getTopicCounter(record.topic()).increment();
			return f;
		} catch (Exception ex) {
			droppedMessages.inc();
			log.error("Failed to send producer record", ex);
			return failedFuture(ex);
		}
	}

	/**
	 * Creates a failed future for the passed exception
	 * @param ex The exception to provide to the failed future
	 * @return the failed future
	 */
	protected static Future<RecordMetadata> failedFuture(final Exception ex) {
		return new Future<RecordMetadata>() {

			@Override
			public boolean cancel(final boolean mayInterruptIfRunning) {
				return true;
			}

			@Override
			public boolean isCancelled() {				
				return false;
			}

			@Override
			public boolean isDone() {
				return true;
			}

			@Override
			public RecordMetadata get() throws InterruptedException, ExecutionException {
				throw new ExecutionException("Send operation failed", ex);
			}

			@Override
			public RecordMetadata get(final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
				throw new ExecutionException("Send operation failed", ex);
			}			
		};
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.clients.producer.Producer#flush()
	 */
	@Override
	public void flush() {
		producer.flush();		
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.clients.producer.Producer#partitionsFor(java.lang.String)
	 */
	@Override
	public List<PartitionInfo> partitionsFor(final String topic) {
		return producer.partitionsFor(topic);
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.clients.producer.Producer#metrics()
	 */
	@Override
	public Map<MetricName, ? extends Metric> metrics() {
		return producer.metrics();
	}
	
	/**
	 * <p>No Op</p>
	 * {@inheritDoc}
	 * @see org.apache.kafka.clients.producer.Producer#close()
	 */
	@Override
	public void close() {
		/* No Op */		
	}
	
	/**
	 * <p>No Op</p>
	 * {@inheritDoc}
	 * @see org.apache.kafka.clients.producer.Producer#close(long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public void close(long timeout, TimeUnit unit) {
		/* No Op */		
	}
	
	
	
}
