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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.heliosapm.streams.buffers.BufferManager;
import com.heliosapm.streams.common.kafka.producer.KafkaProducerService;
import com.heliosapm.streams.common.metrics.SharedMetricsRegistry;
import com.heliosapm.streams.metrics.StreamedMetricValue;
import com.heliosapm.streams.metrics.ValueType;
import com.heliosapm.streams.opentsdb.ringbuffer.RingBufferService;
import com.heliosapm.utils.collections.Props;
import com.stumbleupon.async.Deferred;
import com.sun.management.ThreadMXBean;

import io.netty.buffer.ByteBuf;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesRingBufferStats;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.wire.DocumentContext;
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

public class KafkaRTPublisher extends RTPublisher implements Consumer<BytesRingBufferStats>, StoreFileListener {
	/** Instance logger */
	protected final Logger log = LoggerFactory.getLogger(getClass());	
	/** The publication dispatch */
	protected RingBufferService ringBufferService = null;
	/** The chronicle queue */
	protected ChronicleQueue chronicle;
	/** The chronicle directory */
	protected String chronicleDir = System.getProperty("java.io.tmpdir") + File.separator + "opentsdb-chronicle" + File.separator + "kafka-publisher";
	/** The kafka message sender */
	protected KafkaProducerService<String, ByteBuf> kafkaSender;
	
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
		ringBufferService = RingBufferService.getInstance(p);
		kafkaSender = KafkaProducerService.getInstance(p);
		topicName = rtpConfig.getProperty("topic.name");
		chronicle = ChronicleQueueBuilder
				.single(chronicleDir)
				.buffered(true)
				.rollCycle(RollCycles.HOURLY)
				.onRingBufferStats(this)
				.storeFileListener(this)
				.build();
		
		log.info("<<<<< KafkaRTPublisher initialized.");
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#shutdown()
	 */
	@Override
	public Deferred<Object> shutdown() {
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#version()
	 */
	@Override
	public String version() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#collectStats(net.opentsdb.stats.StatsCollector)
	 */
	@Override
	public void collectStats(StatsCollector collector) {
		// TODO Auto-generated method stub
	}
	
	
	
	/**
	 * Reads from the chronicle and send on kafka topic
	 */
	@SuppressWarnings("restriction")
	public void run() {
		final ExcerptTailer tailer = chronicle.createTailer();
		final ByteByfReadBytesMarshallable reader = ByteByfReadBytesMarshallable.get();
		ByteBuf buf = null;
		try {
			final long threadId = Thread.currentThread().getId();
			while(keepRunning.get()) {
				try {
					final DocumentContext dc = tailer.readingDocument();
					if(dc.isPresent() && dc.isData()) {
						final long bytesAllocated = TMX.getThreadAllocatedBytes(threadId);
						final Context ctx = chronicleReadKafkaWrite.time();
						tailer.readBytes(reader);
						buf = reader.getBuffer();
						kafkaSender.send(new ProducerRecord<String, ByteBuf>(topicName, StreamedMetricValue.metricName(buf), buf));
						ctx.stop();
						writerBytesAllocated.mark(TMX.getThreadAllocatedBytes(threadId) - bytesAllocated);
					}
				} catch (Exception ex) {
					log.error("ChronicleQueue Read / Kafka Write Error", ex);
					if(buf!=null) {
						while(buf.refCnt()!=0) {
							buf.release(buf.refCnt());
						}
						buf = null;
					}					
				}
			}
		} finally {
			// 
		}
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
		ExcerptAppender appender = chronicle.acquireAppender();
		appender.writeBytes(write(value, ValueType.STRAIGHTTHROUGH, metric, timestamp, tags));
		return Deferred.fromResult(null);
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#publishDataPoint(java.lang.String, long, double, java.util.Map, byte[])
	 */
	@Override
	public Deferred<Object> publishDataPoint(final String metric, final long timestamp, final double value, final Map<String, String> tags, final byte[] tsuid) {
		ExcerptAppender appender = chronicle.acquireAppender();
		appender.writeBytes(write(value, ValueType.STRAIGHTTHROUGH, metric, timestamp, tags));
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

	/**
	 * {@inheritDoc}
	 * @see java.util.function.Consumer#accept(java.lang.Object)
	 */
	@Override
	public void accept(final BytesRingBufferStats t) {
		chronicleReads.inc(t.getAndClearReadCount());
		chronicleWrites.inc(t.getAndClearWriteCount());
	}

	/**
	 * {@inheritDoc}
	 * @see net.openhft.chronicle.queue.impl.StoreFileListener#onReleased(int, java.io.File)
	 */
	@Override
	public void onReleased(final int cycle, final File file) {
		final long size = file.length();
		final String name = file.getAbsolutePath();
		if(file.delete()) {
			deletedRollFiles.inc();
			log.info("Deleted RollFile [{}], size:[{}] bytes", name, size);
		} else {
			log.warn("Failed to Delete RollFile [{}], size:[{}] bytes", name, size);
		}		
	}
	
	/**
	 * Writes a metric definition to the passed Bytes
	 * @param value the metric value
	 * @param valueType The value type
	 * @param metricName The metric name
	 * @param timestamp The metric timestamp
	 * @param tags The metric tags
	 * @return the bytes to write to the chronicle queue
	 */
	public static Bytes<ByteBuffer> write(final long value, final ValueType valueType, final String metricName, final long timestamp, final Map<String, String> tags) {
		final Bytes<ByteBuffer> buff = Bytes.elasticByteBuffer(128);
		buff.writeInt(Integer.MAX_VALUE);
		buff.writeByte(StreamedMetricValue.TYPE_CODE);
		buff.writeByte((byte) (valueType==null ? 0 : valueType.ordinal()+1));
		buff.writeLong(timestamp);
		buff.writeUtf8(metricName);
		buff.writeByte((byte)tags.size());
		for(Map.Entry<String, String> entry: tags.entrySet()) {
			buff.writeUtf8(entry.getKey());
			buff.writeUtf8(entry.getValue());
		}		
		buff.writeByte(ONE_BYTE);
		buff.writeLong(value);		
		buff.writeInt(0, buff.length()-4);
		return buff;
	}
	
	/**
	 * Writes a metric definition to the passed Bytes
	 * @param value the metric value
	 * @param valueType The value type
	 * @param metricName The metric name
	 * @param timestamp The metric timestamp
	 * @param tags The metric tags
	 * @return the bytes to write to the chronicle queue
	 */
	public static Bytes<ByteBuffer> write(final double value, final ValueType valueType, final String metricName, final long timestamp, final Map<String, String> tags) {
		final Bytes<ByteBuffer> buff = Bytes.elasticByteBuffer(128);
		buff.writeInt(Integer.MAX_VALUE);
		buff.writeByte(StreamedMetricValue.TYPE_CODE);
		buff.writeByte((byte) (valueType==null ? 0 : valueType.ordinal()+1));
		buff.writeLong(timestamp);
		buff.writeUtf8(metricName);
		buff.writeByte((byte)tags.size());
		for(Map.Entry<String, String> entry: tags.entrySet()) {
			buff.writeUtf8(entry.getKey());
			buff.writeUtf8(entry.getValue());
		}		
		buff.writeByte(ZERO_BYTE);
		buff.writeDouble(value);	
		buff.writeInt(0, buff.length()-4);
		return buff;
	}

}
