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
package com.heliosapm.streams.opentsdb.ringbuffer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadFactory;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.heliosapm.streams.buffers.BufferManager;
import com.heliosapm.streams.buffers.ByteBufSerde;
import com.heliosapm.streams.common.kafka.producer.KafkaProducerService;
import com.heliosapm.streams.common.metrics.SharedMetricsRegistry;
import com.heliosapm.streams.metrics.StreamedMetricValue;
import com.heliosapm.streams.metrics.ValueType;
import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.jmx.JMXManagedThreadFactory;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import io.netty.buffer.ByteBuf;

/**
 * <p>Title: RingBufferService</p>
 * <p>Description: A shared ring buffer service to support OpenTSDB plugins</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.opentsdb.ringbuffer.RingBufferService</code></p>
 */

public class RingBufferService implements EventFactory<ByteBuf>, EventHandler<ByteBuf> {
	/** The singleton instance */
	private static volatile RingBufferService instance = null;
	/** The singleton instance ctor lock */
	private static final Object lock = new Object();
	
	/** The maximum number of workers */
	public static final int MAX_CAP = 0x7fff;  // max #workers - 1
	/** The config prop for the buffer size */
	public static final String RB_CONF_BUFFER_SIZE = "disruptor.buffer.size";	
	/** The default buffer size */
	public static final int RB_DEFAULT_BUFFER_SIZE = 8192;
	
	
	/** The config prop for the shutdown timeout in seconds. -1 or less means halt. */
	public static final String RB_CONF_STOP_TIMEOUT = "disruptor.shutdown.timeout";	
	/** The default shutdown timeout in seconds. -1 or less means halt */
	public static final int RB_DEFAULT_STOP_TIMEOUT = 5;
	
	/** The config prop for the buffer event factory initial size */
	public static final String RB_CONF_BUFF_INITIAL_SIZE = "event.buff.initial.size";	
	/** The default buffer event factory initial size */
	public static final int RB_DEFAULT_BUFF_INITIAL_SIZE = 128;
	
	/** The config prop for the nameof the topic to send to */
	public static final String RB_CONF_TOPIC_NAME = "topic.endpoint";	
	/** The default buffer event factory initial size */
	public static final String RB_DEFAULT_TOPIC_NAME = "bosun.tsdb.rtp.metrics";
	
	
	//  "bosun.tsdb.rtp.metrics"
	
	
	/** A counter to track the number of event handling errors */
	protected final Counter handleEventExceptions = SharedMetricsRegistry.getInstance().counter("disruptor.handler.exceptions");

	
	/** The ring buffer thread factory */
	protected final ThreadFactory threadFactory = JMXManagedThreadFactory.newThreadFactory("RingBufferService", true);
	/** The ring buffer executor buffer size */
	protected final int bufferSize;

	/** The shutdown timeout for the disruptor in seconds */
	protected final int shutdownTimeout;
	
	/** The ring buffer dsl configurator */
	protected final Disruptor<ByteBuf> disruptor;  
	/** The ring buffer */
	protected final RingBuffer<ByteBuf> ringBuffer;	
	/** Instance logger */
	protected final Logger log = LoggerFactory.getLogger(getClass());
	/** The wait strategy */
	protected final WaitStrategy waitStrategy;
	
	/** The event buffer initial size */
	protected int eventBuffInitSize;
	
	/** The target topic */
	protected final String targetTopic;
	
	/** The kafka producer */
	protected final KafkaProducerService<String, ByteBuf> producer;
	
	/** The buffer factory */
	protected final BufferManager bufferManager = BufferManager.getInstance();
	
	
	/**
	 * Acquires and returns the RingBufferService singleton
	 * @param config The optional configuration
	 * @return the RingBufferService singleton
	 */
	public static RingBufferService getInstance(final Properties config) {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					instance = new RingBufferService(config);
				}
			}
		}
		return instance;
	}

	
	
	
	@SuppressWarnings("unchecked")
	private RingBufferService(final Properties config) {
		config.put(KafkaProducerService.CONFIG_PREFIX + "value.serializer", ByteBufSerde.ByteBufSerializer.class.getName());
		config.put(KafkaProducerService.CONFIG_PREFIX + "key.serializer", Serdes.String().serializer());
		producer = KafkaProducerService.getInstance(config);
		bufferSize = ConfigurationHelper.getIntSystemThenEnvProperty(RB_CONF_BUFFER_SIZE, RB_DEFAULT_BUFFER_SIZE, config);
		targetTopic = ConfigurationHelper.getSystemThenEnvProperty(RB_CONF_TOPIC_NAME, RB_DEFAULT_TOPIC_NAME, config);
		eventBuffInitSize = ConfigurationHelper.getIntSystemThenEnvProperty(RB_CONF_BUFF_INITIAL_SIZE, RB_DEFAULT_BUFF_INITIAL_SIZE, config);
		shutdownTimeout = ConfigurationHelper.getIntSystemThenEnvProperty(RB_CONF_STOP_TIMEOUT, RB_DEFAULT_STOP_TIMEOUT, config);
		waitStrategy = RBWaitStrategy.getConfiguredStrategy(config);
		disruptor = new Disruptor<ByteBuf>(this, bufferSize, threadFactory, ProducerType.MULTI, waitStrategy);
		disruptor.handleEventsWith(this);   // FIXME: need to able to supply several handlers
		disruptor.start();
		ringBuffer = disruptor.getRingBuffer();
		log.info("<<<<< RawRingBufferDispatcher Started.");		
	}




	/**
	 * {@inheritDoc}
	 * @see com.lmax.disruptor.EventFactory#newInstance()
	 */
	@Override
	public ByteBuf newInstance() {
		return bufferManager.buffer(eventBuffInitSize);  // FIXME: config 
	}


	/**
	 * Sends a metric to the configured end point topic
	 * @param metric The metric name
	 * @param timestamp The metric timestamp
	 * @param value The metric value
	 * @param tags The metric tags
	 */
	public void publishDataPoint(final String metric, final long timestamp, final double value, final Map<String, String> tags) {
		final long seq = ringBuffer.next();
		StreamedMetricValue.write(ringBuffer.get(seq), ValueType.STRAIGHTTHROUGH, metric, timestamp, value, tags);
		ringBuffer.publish(seq);
	}
	
	/**
	 * Sends a metric to the configured end point topic
	 * @param metric The metric name
	 * @param timestamp The metric timestamp
	 * @param value The metric value
	 * @param tags The metric tags
	 */
	public void publishDataPoint(final String metric, final long timestamp, final long value, final Map<String, String> tags) {
		final long seq = ringBuffer.next();
		StreamedMetricValue.write(ringBuffer.get(seq), ValueType.STRAIGHTTHROUGH, metric, timestamp, value, tags);
		ringBuffer.publish(seq);
	}


	/**
	 * {@inheritDoc}
	 * @see com.lmax.disruptor.EventHandler#onEvent(java.lang.Object, long, boolean)
	 */
	@Override
	public void onEvent(final ByteBuf event, final long sequence, final boolean endOfBatch) throws Exception {
		try {
			producer.send(new ProducerRecord<String, ByteBuf>(targetTopic, null, StreamedMetricValue.timestamp(event), StreamedMetricValue.metricName(event), event)); 
		} catch (Exception x) {
			handleEventExceptions.inc();
			log.error("Failed to handle event", x);
		} finally {
			if(endOfBatch) producer.flush();
			event.clear();
		}
	}
	
	/**
	 * Stops the ring buffer service
	 */
	public void shutdown() {
		disruptor.shutdown();		
	}
}
