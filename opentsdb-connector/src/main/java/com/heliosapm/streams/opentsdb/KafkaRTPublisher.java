/**
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
 */
package com.heliosapm.streams.opentsdb;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.heliosapm.utils.buffer.BufferManager;
import com.heliosapm.streams.chronicle.MessageListener;
import com.heliosapm.streams.chronicle.MessageQueue;
import com.heliosapm.streams.common.kafka.producer.KafkaProducerService;
import com.heliosapm.streams.common.metrics.SharedMetricsRegistry;
import com.heliosapm.streams.metrics.StreamedMetricValue;
import com.heliosapm.streams.metrics.ValueType;
import com.heliosapm.utils.collections.Props;
import com.stumbleupon.async.Deferred;
import com.sun.management.ThreadMXBean;

import io.netty.buffer.ByteBuf;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesRingBufferStats;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.Annotation;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.tsd.RTPublisher;

/**
 * <p>Title: KafkaRTPublisher</p>
 * <p>Description: RTPublisher to publish events to a kafka topic</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.opentsdb.KafkaRTPublisher</code></p>
 */

public class KafkaRTPublisher extends RTPublisher implements MessageListener  {
	/** Instance logger */
	protected final Logger log = LoggerFactory.getLogger(getClass());	
	/** The kafka message sender */
	protected KafkaProducerService<String, StreamedMetricValue> kafkaSender;
	/** The chronicle message queue */
	protected MessageQueue messageQueue;

	
	/** The platform ThreadMXBean */
	public static final ThreadMXBean TMX = (ThreadMXBean)ManagementFactory.getThreadMXBean();
	
	/** The prefix on TSDB config items marking them as applicable to this service */
	public static final String CONFIG_PREFIX = "tsd.rtp.kafka.";
	
	/** The keep running flag */
	protected final AtomicBoolean keepRunning = new AtomicBoolean(true);

	/** The name of the topic to send to */
	protected String topicName = null;
	
	/** A counter of deleted roll files */
	protected final Counter deletedRollFiles = SharedMetricsRegistry.getInstance().counter("chronicle.rollfile.deleted");
	/** A periodic counter of chronicle reads */
	protected final Counter chronicleReads = SharedMetricsRegistry.getInstance().counter("chronicle.reads");
	/** A periodic counter of chronicle writes */
	protected final Counter chronicleWrites = SharedMetricsRegistry.getInstance().counter("chronicle.writes");
	/** A timer of chronicle reads plus kafka writes */
	protected final Timer chronicleReadKafkaWrite = SharedMetricsRegistry.getInstance().timer("chronicleread.kafkawrite");
	/** A meter of thread allocated bytes during chronicle reads plus kafka writes */
	protected final Meter writerBytesAllocated = SharedMetricsRegistry.getInstance().meter("writer.bytesallocated");
	
	/** A zero value bytes const */
	public static final byte ZERO_BYTE = 0;
	/** A one value bytes const */
	public static final byte ONE_BYTE = 0;
	/**
	 * Creates a new KafkaRTPublisher
	 */
	public KafkaRTPublisher() {
		log.info("Created KafkaRTPublisher");
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#initialize(net.opentsdb.core.TSDB)
	 */
	@Override
	public void initialize(final TSDB tsdb) {
		log.info(">>>>> Initializing KafkaRTPublisher...");
		final Map<String, String> config = tsdb.getConfig().getMap();
		final Properties p = new Properties();		
		p.putAll(config);
		final Properties rtpConfig = Props.extract(CONFIG_PREFIX, p, true, true);
		for(final String key: rtpConfig.stringPropertyNames()) {
			rtpConfig.setProperty("kafka.producer." + key , rtpConfig.getProperty(key));
//			rtpConfig.remove(key);
		}
		log.info("EXTRACTED Config:" + rtpConfig);
		kafkaSender = KafkaProducerService.getInstance(rtpConfig);
		messageQueue = MessageQueue.getInstance(getClass().getSimpleName(), this, rtpConfig);
		topicName = rtpConfig.getProperty("kafka.producer.topic.name");
				
		log.info("<<<<< KafkaRTPublisher initialized.");
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.chronicle.MessageListener#onMetric(io.netty.buffer.ByteBuf)
	 */
	@Override
	public int onMetric(final ByteBuf buf) {
		log.debug("OnMetric Buffer: {} bytes", buf.readableBytes());
		try {			
			int totalCount = 0;
			try {
				final Iterator<StreamedMetricValue> iter = StreamedMetricValue.streamedMetricValues(true, buf, true).iterator();
				while(iter.hasNext()) {
					final StreamedMetricValue smv = iter.next();
					kafkaSender.send(new ProducerRecord<String, StreamedMetricValue>(topicName, smv.metricKey(), smv));
					totalCount++;
				}
			} catch (Exception ex) {
				log.error("OnMetric Error", ex);
			}
			return totalCount;
		} finally {
			try { buf.release(); } catch (Exception ex) {/* No Op */}
		}
	}
	

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#shutdown()
	 */
	@Override
	public Deferred<Object> shutdown() {
		return Deferred.fromResult(null);
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#version()
	 */
	@Override
	public String version() {		
		return "2.1";
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#collectStats(net.opentsdb.stats.StatsCollector)
	 */
	@Override
	public void collectStats(StatsCollector collector) {
		// TODO Auto-generated method stub
	}
	
	
	
	
	static class ByteByfReadBytesMarshallable implements ReadBytesMarshallable {
		static final ThreadLocal<ByteByfReadBytesMarshallable> threadReader = new ThreadLocal<ByteByfReadBytesMarshallable>() {
			@Override
			protected ByteByfReadBytesMarshallable initialValue() {				
				return new ByteByfReadBytesMarshallable();
			}
		};
		static ByteByfReadBytesMarshallable get() {
			return threadReader.get();
		}
		
		ByteBuf bb = null;
		@Override
		public void readMarshallable(final BytesIn bytes) throws IORuntimeException {
			final int len = bytes.readInt();			
			try {
				bb = BufferManager.getInstance().buffer(len);
				bb.writeBytes(bytes.inputStream(), len);
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}			
		}
		
		public ByteBuf getBuffer() {
			return bb;
		}
		
		
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#publishDataPoint(java.lang.String, long, long, java.util.Map, byte[])
	 */
	@Override
	public Deferred<Object> publishDataPoint(final String metric, final long timestamp, final long value, final Map<String, String> tags, final byte[] tsuid) {
		messageQueue.writeEntry(new StreamedMetricValue(timestamp, value, metric, tags));
		return Deferred.fromResult(null);
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#publishDataPoint(java.lang.String, long, double, java.util.Map, byte[])
	 */
	@Override
	public Deferred<Object> publishDataPoint(final String metric, final long timestamp, final double value, final Map<String, String> tags, final byte[] tsuid) {
		messageQueue.writeEntry(new StreamedMetricValue(timestamp, value, metric, tags));
		return Deferred.fromResult(null);
	}
	

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#publishAnnotation(net.opentsdb.meta.Annotation)
	 */
	@Override
	public Deferred<Object> publishAnnotation(Annotation annotation) {
		// TODO: 
		return Deferred.fromResult(null);
	}

		

}
