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
package com.heliosapm.streams.metrics;

import java.io.DataInputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.heliosapm.streams.buffers.BufferManager;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.core.io.IORuntimeException;

/**
 * <p>Title: StreamedMetricValue</p>
 * <p>Description: A metric instance with a value</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.StreamedMetricValue</code></p>
 */

public class StreamedMetricValue extends StreamedMetric {
	/** Indicates if the value is a double (true) or a long (false)  */
	protected boolean isDoubleValue = true;
	/** The long typed value */
	protected long longValue = -1L;
	/** The double typed value */
	protected double doubleValue = -1L;
	
	static final int VBASE_SIZE = 9;
	
	/** The type code for this metric type */
	public static final byte TYPE_CODE = 1;
	

	/**
	 * Creates a new StreamedMetricValue
	 * @param timestamp The metric timestamp in ms. since the epoch
	 * @param value The metric value
	 * @param metricName The metric name
	 * @param tags The metric tags
	 */
	public StreamedMetricValue(final long timestamp, final long value, final String metricName, final Map<String, String> tags) {
		super(timestamp, metricName, tags);
		isDoubleValue = false;
		longValue = value;
		byteSize += VBASE_SIZE; 
	}

	/**
	 * Creates a new StreamedMetricValue with an auto assigned timestamp
	 * @param value The metric value
	 * @param metricName The metric name
	 * @param tags The metric tags
	 */
	public StreamedMetricValue(final long value, final String metricName, final Map<String, String> tags) {
		this(System.currentTimeMillis(), value, metricName, tags);
	}

	/**
	 * Creates a new StreamedMetricValue
	 * @param timestamp The metric timestamp in ms. since the epoch
	 * @param value The metric value
	 * @param metricName The metric name
	 * @param tags The metric tags
	 */
	public StreamedMetricValue(final long timestamp, final double value, final String metricName, final Map<String, String> tags) {
		super(timestamp, metricName, tags);
		isDoubleValue = true;
		doubleValue = value;
		
		byteSize += VBASE_SIZE;
	}

	/**
	 * Creates a new StreamedMetricValue with an auto assigned timestamp
	 * @param value The metric value
	 * @param metricName The metric name
	 * @param tags The metric tags
	 */
	public StreamedMetricValue(final double value, final String metricName, final Map<String, String> tags) {
		this(System.currentTimeMillis(), value, metricName, tags);
	}
	
	StreamedMetricValue() {
		super();
	}
	
	StreamedMetricValue(final StreamedMetric sm, final long value) {
		this(sm.timestamp, value, sm.metricName, sm.tags);
	}
	
	StreamedMetricValue(final StreamedMetric sm, final long timestamp, final long value) {
		this(timestamp, value, sm.metricName, sm.tags);
	}
	
	
	StreamedMetricValue(final StreamedMetric sm, final double value) {
		this(sm.timestamp, value, sm.metricName, sm.tags);
	}
	
	StreamedMetricValue(final StreamedMetric sm, final long timestamp, final double value) {
		this(timestamp, value, sm.metricName, sm.tags);
	}
	
	/**
	 * Returns the value as a long regardless of type
	 * @return the value as a long
	 */
	public long getValueAsLong() {
		return isDoubleValue ? (long)doubleValue : longValue;
	}
	
	/**
	 * Returns the value as a double regardless of type
	 * @return the value as a double
	 */
	public double getValueAsDouble() {
		return isDoubleValue ? doubleValue : longValue;
	}
	
	/**
	 * Returns the value as a string
	 * @return the value as a string
	 */
	public String getValue() {
		return isDoubleValue ? String.valueOf(doubleValue) : String.valueOf(longValue);
	}
	
	
	/**
	 * Returns the double value
	 * @return the doubleValue
	 */
	public double getDoubleValue() {
		return doubleValue;
	}
	
	/**
	 * Indicates if this metric is typed as a double 
	 * @return true if this metric is typed as a double, false if this metric is typed as a long
	 */
	public boolean isDoubleValue() {
		return isDoubleValue;
	}

	/**
	 * Returns the long value
	 * @return the longValue
	 */
	public long getLongValue() {
		return longValue;
	}
	
	/**
	 * Returns the value as a number
	 * @return the value as a number
	 */
	public Number getValueNumber() {
		return isDoubleValue ? doubleValue : longValue;
	}
	
	/**
	 * Indicates if this StreamedMetric has a value
	 * @return true since this type has a value
	 */
	@Override
	public boolean isValued() {
		return true;
	}
	
	/**
	 * Sets a value type
	 * @param vt The value type to set
	 * @return this metric
	 */
	@Override
	public StreamedMetricValue setValueType(final ValueType vt) {
		if(vt!=null) {
			if(vt.valueless) throw new IllegalArgumentException("Invalid value type for StreamedMetric. Type is valueless [" + vt.name + "]");
			this.valueType = vt;
		}
		return this;
	}
	
	
	
	/**
	 * Updates this metric with a new timestamp and value
	 * @param timestamp the new timestamp
	 * @param newValue the new value
	 * @return this metric instance
	 */
	public StreamedMetricValue update(final long timestamp, final long newValue) {
		if(isDoubleValue) throw new IllegalArgumentException("This is a double valued StreamedMetric. Cannot update with a long value");
		this.timestamp = timestamp;
		this.longValue = newValue;
		return this;
	}
	
	
	/**
	 * Updates the timestamp for this metric
	 * @param timestamp The new timestamp in ms
	 * @return this metric
	 */
	public StreamedMetricValue updateTimestamp(final long timestamp) {
		this.timestamp = timestamp;
		return this;		 
	 }
	
	
	
	/**
	 * Updates this StreamedMetricValue using the next serialized version in the passed ByteBuf. 
	 * @param buf The buffer to update from
	 * @return this StreamedMetricValue
	 */
	@Override
	public StreamedMetricValue update(final ByteBuf buf) {
		super.update(buf);
		if(buf.readByte()==ZERO_BYTE) {			
			isDoubleValue = true;
			doubleValue = buf.readDouble();
		} else {
			isDoubleValue = false;
			longValue = buf.readLong();			
		}		
		return this;
	}
	
	/**
	 * Updates this metric with a new value
	 * @param newValue the new value
	 * @return this metric instance
	 */
	public StreamedMetricValue update(final long newValue) {
		return update(timestamp, newValue);
	}
	
	
	/**
	 * Updates this metric with a new timestamp and value
	 * @param timestamp the new timestamp
	 * @param newValue the new value
	 * @return this metric instance
	 */
	public StreamedMetricValue update(final long timestamp, final double newValue) {
		if(!isDoubleValue) throw new IllegalArgumentException("This is a long valued StreamedMetric. Cannot update with a double value");
		this.timestamp = timestamp;
		this.doubleValue = newValue;
		return this;
	}
	
	/**
	 * Updates this metric with a new value
	 * @param newValue the new value
	 * @return this metric instance
	 */
	public StreamedMetricValue update(final double newValue) {
		return update(timestamp, newValue);
	}
	
	
	/**
	 * Renders this metric as an OpenTSDB <b>put</p> text line which is:
	 * <b><code>put &lt;metric&gt; &lt;timestamp&gt; &lt;value&gt; &lt;tagk1=tagv1[ tagk2=tagv2 ...tagkN=tagvN]&gt;</code></b>.
	 * @return the OpenTSDB rendered put line
	 */
	@Override
	public String toOpenTSDBString() {
		final StringBuilder b = new StringBuilder(96).append("put ")
		.append(metricName).append(" ")
		.append(timestamp).append(" ")
		.append(isDoubleValue ? 
				Double.toString(doubleValue) : 
				Long.toString(longValue))
		.append(" ");
				
		for(Map.Entry<String, String> entry: tags.entrySet()) {
			b.append(entry.getKey()).append("=").append(entry.getValue()).append(" ");
		}		
		return b.toString();
	}
	
	/**
	 * Writes a metric definition to the passed buffer
	 * @param buff The buffer to write to
	 * @param valueType The value type
	 * @param metricName The metric name
	 * @param timestamp The metric timestamp
	 * @param tags The metric tags
	 * @return the number of bytes written
	 */
	public static int write(final ByteBuf buff, final ValueType valueType, final String metricName, final long timestamp, final Map<String, String> tags) {
		final int offset = buff.writerIndex();
		buff.writeByte(TYPE_CODE);
		buff.writeByte(valueType==null ? 0 : valueType.ordinal()+1);
		buff.writeLong(timestamp);
		BufferManager.writeUTF(metricName, buff);			
		buff.writeByte(tags.size());
		for(Map.Entry<String, String> entry: tags.entrySet()) {
			BufferManager.writeUTF(entry.getKey(), buff);
			BufferManager.writeUTF(entry.getValue(), buff);
		}		
		return buff.writerIndex() - offset;
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.StreamedMetric#writeMarshallable(net.openhft.chronicle.bytes.BytesOut)
	 */
	@Override
	public void writeMarshallable(final BytesOut bytes) {
		bytes.writeByte(TYPE_CODE);
		writeBytes(bytes);
		if(isDoubleValue) {
			bytes.writeByte(ZERO_BYTE);
			bytes.writeDouble(doubleValue);			
		} else {
			bytes.writeByte(ONE_BYTE);
			bytes.writeLong(longValue);
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.StreamedMetric#readMarshallable(net.openhft.chronicle.bytes.BytesIn)
	 */
	@Override
	public void readMarshallable(final BytesIn bytes) throws IORuntimeException {
		super.readMarshallable(bytes);
		if(bytes.readByte()==ZERO_BYTE) {			
			isDoubleValue = true;
			doubleValue = bytes.readDouble();
		} else {
			isDoubleValue = false;
			longValue = bytes.readLong();			
		}
	}
	
	/**
	 * Reads a streamed metric from the passed buffer
	 * @param buff the buffer to read the streamed metric from
	 * @return the appropriate type of StreamedMetric
	 */
	public static StreamedMetricValue[] readAll(final ByteBuf buff) {
		final Set<StreamedMetricValue> metrics = new HashSet<StreamedMetricValue>();
		while(buff.isReadable(MIN_READABLE_BYTES)) {
			metrics.add(read(buff).forValue(1L));
		}
		return metrics.toArray(new StreamedMetricValue[metrics.size()]);
	}
	
	/**
	 * Returns an interator over the StreamMetricValues in the passed buffer
	 * @param singleInstance If true, the StreamMetricValue returns from the iterator will be the same actual instance, updated on each loop of the iterator.
	 * As such, the returned StreamMetricValue should be used before the next iterator loop since the values of that instance will change.
	 * In other words, attempting to stash all the returned StreamMetricValues in a collection, or the like, will void the warranty. 
	 * @param buf The buffer to read from
	 * @param releaseOnDone true to release the buffer on iterator end
	 * @return the iterator
	 * FIXME: all this stuff needs to return SM or SMV
	 */
	public static Iterable<StreamedMetricValue> streamedMetricValues(final boolean singleInstance, final ByteBuf buf, final boolean releaseOnDone) {
		final StreamedMetricValue single  = singleInstance ? new StreamedMetricValue() : null;
		return new Iterable<StreamedMetricValue>() {
			@Override
			public Iterator<StreamedMetricValue> iterator() {
				return new Iterator<StreamedMetricValue>() {
					@Override
					public boolean hasNext() {
						final boolean hasNext = buf.readableBytes() > MIN_READABLE_BYTES;
						if(!hasNext && releaseOnDone) buf.release();
						return hasNext;
					}
					@Override
					public StreamedMetricValue next() {
						if(singleInstance) {
							buf.readByte();
							return single.update(buf);
						} 
						return read(buf).forValue(1L);
					}					
				};
			}			
		};
	}
	
	
	
	
	
	/**
	 * Extracts the timestamp from a byte buff
	 * @param buff the buff to extract from
	 * @return the timestamp
	 */
	public static long timestamp(final ByteBuf buff) {
		return buff.getLong(2);
	}
	
	/**
	 * Extracts the metric name from a byte buff
	 * @param buff the buff to extract from
	 * @return the metric name
	 */
	public static String metricName(final ByteBuf buff) {
		return BufferManager.readUTF(10, buff);
	}
	
	/**
	 * Reads a StreamedMetricValue from the passed buffer
	 * @param buff The buffer to read from
	 * @return the StreamedMetricValue
	 */
	public static StreamedMetricValue from(final ByteBuf buff) {
		buff.readByte();
		final byte vt = buff.readByte();
		final ValueType valueType = vt==0 ? null : ValueType.ordinal(vt);
		final long ts = buff.readLong();
		final String mn = BufferManager.readUTF(buff);
		final int tagCount = buff.readByte();
		final Map<String, String> tags = new HashMap<String, String>(tagCount);
		for(int i = 0; i < tagCount; i++) {
			tags.put(BufferManager.readUTF(buff), BufferManager.readUTF(buff));			
		}
		final byte lord = buff.readByte();
		if(lord==0) {
			return new StreamedMetricValue(ts, buff.readDouble(), mn, tags).setValueType(valueType);
		}
		return new StreamedMetricValue(ts, buff.readLong(), mn, tags).setValueType(valueType);
		
	}
	
	
	
	/**
	 * Writes a metric to the passed buffer
	 * @param buff The buffer to write to
	 * @param valueType The value type
	 * @param metricName The metric name
	 * @param timestamp The metric timestamp
	 * @param value The metric value
	 * @param tags The metric tags
	 * @return the number of bytes written
	 */
	public static int write(final ByteBuf buff, final ValueType valueType, final String metricName, final long timestamp, final long value, final Map<String, String> tags) {
		final int defSize = write(buff, valueType, metricName, timestamp, tags);
		buff.writeByte(1);
		buff.writeLong(value);
		return defSize + 9;
	}
	
	/**
	 * Writes a metric to the passed buffer
	 * @param buff The buffer to write to
	 * @param valueType The value type
	 * @param metricName The metric name
	 * @param timestamp The metric timestamp
	 * @param value The metric value
	 * @param tags The metric tags
	 * @return the number of bytes written
	 */
	public static int write(final ByteBuf buff, final ValueType valueType, final String metricName, final long timestamp, final double value, final Map<String, String> tags) {
		final int defSize = write(buff, valueType, metricName, timestamp, tags);
		buff.writeByte(0);
		buff.writeDouble(value);
		return defSize + 9;
	}
	
	
	/**
	 * Returns a byte array containing the serialized streammetric
	 * @return a byte array 
	 */
	@Override
	public byte[] toByteArray() {
		final ByteBuf buff = BufferManager.getInstance().directBuffer(byteSize);
		try {
			buff.writeByte(TYPE_CODE);
			writeByteArray(buff);
			if(isDoubleValue) {
				buff.writeByte(0);
				buff.writeDouble(doubleValue);
			} else {
				buff.writeByte(1);
				buff.writeLong(longValue);
			}
			return ByteBufUtil.getBytes(buff, 0, buff.readableBytes());
		} finally {
			try { buff.release(); } catch (Exception x) {/* No Op */}
		}
	}
	
	/**
	 * Returns this streamed metric serialized into a byte buf
	 * @return the byte buf
	 */
	@Override
	public ByteBuf toByteBuff() {
		final ByteBuf buff = BufferManager.getInstance().directBuffer(byteSize);
		buff.writeByte(TYPE_CODE);
		writeByteArray(buff);
		if(isDoubleValue) {
			buff.writeByte(0);
			buff.writeDouble(doubleValue);
		} else {
			buff.writeByte(1);
			buff.writeLong(longValue);
		}		
		return buff;
	}
	
	/**
	 * Writes this metric into the passed buffer
	 * @param buff The buffer to write this metric into
	 */
	@Override
	public void intoByteBuf(final ByteBuf buff) {
		buff.writeByte(TYPE_CODE);
		writeByteArray(buff);
		if(isDoubleValue) {
			buff.writeByte(0);
			buff.writeDouble(doubleValue);
		} else {
			buff.writeByte(1);
			buff.writeLong(longValue);
		}		
	}
	
	
	
	/**
	 * Creates a StreamedMetricValue from the passed buffer
	 * @param bytes The byte to read the StreamedMetric from
	 * @return the created StreamedMetric
	 */
	static StreamedMetricValue fromBuff(final ByteBuf buff) {
		final StreamedMetricValue sm = new StreamedMetricValue();
		sm.byteSize = buff.readableBytes() + 1;
		sm.readFromBuff(buff);
		final byte type = buff.readByte();
		if(type==0) {
			sm.isDoubleValue = true;
			sm.doubleValue = buff.readDouble();
		} else {
			sm.isDoubleValue = false;
			sm.longValue = buff.readLong();				
		}			
		return sm;
	}
	
	
//	static StreamedMetricValue readFromStream(final DataInputStream dis) {
//		return new StreamedMetricValue().read(dis);
//	}
	
	@Override
	StreamedMetricValue read(final DataInputStream dis) {		
		try {
			super.read(dis);
			final byte type = dis.readByte();
			if(type==0) {
				isDoubleValue = true;
				doubleValue = dis.readDouble();
			} else {
				isDoubleValue = false;
				longValue = dis.readLong();				
			}						
			return this;
		} catch (Exception ex) {
			throw new RuntimeException("Failed to read StreamedMetricValue from DataInputStream", ex);
		}
		
	}
	
	

}
