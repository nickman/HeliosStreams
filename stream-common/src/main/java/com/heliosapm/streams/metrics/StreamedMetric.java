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
package com.heliosapm.streams.metrics;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.heliosapm.streams.buffers.BufferManager;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.core.io.IORuntimeException;

/**
 * <p>Title: StreamedMetric</p>
 * <p>Description: A metric instance</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.StreamedMetric</code></p>
 */

public class StreamedMetric implements BytesMarshallable {
	/** The metric timestamp in ms. since the epoch */
	protected long timestamp = -1L;
	/** The metric name */
	protected String metricName = null;
	/** The metric instance key */
	protected transient volatile String metricKey = null;
	/** The metric tags */
	protected final Map<String, String> tags = new TreeMap<String, String>();
	/** The value type, if one was assigned */
	protected ValueType valueType = null;
	/** The estimated byte size */
	protected int byteSize = BASE_SIZE;
	
	static final int BASE_SIZE = 11;
	
	/** The type code for this metric type */
	public static final byte TYPE_CODE = 0;
	
	
	/** The Streamed Metric Serde */
	public static final Serde<StreamedMetric> SERDE = new Serde<StreamedMetric>() {
		/**
		 * {@inheritDoc}
		 * @see org.apache.kafka.common.serialization.Serde#deserializer()
		 */
		@Override
		public Deserializer<StreamedMetric> deserializer() {			
			return new StreamedMetricDeserializer();
		}
		
		/**
		 * {@inheritDoc}
		 * @see org.apache.kafka.common.serialization.Serde#serializer()
		 */
		@Override
		public Serializer<StreamedMetric> serializer() {			
			return new StreamedMetricSerializer();
		}

		@Override
		public void configure(final Map<String, ?> configs, final boolean isKey) {
			/* No Op */
		}

		@Override
		public void close() {
			/* No Op */			
		}
	};
	

	/**
	 * Creates a new StreamedMetric
	 * @param timestamp The timestamp in ms. since the epoch
	 * @param metricName The metric name
	 * @param tags The metric tags
	 */
	public StreamedMetric(final long timestamp, final String metricName, final Map<String, String> tags) {
		this.timestamp = timestamp;
		this.metricName = metricName.trim();		
		byteSize += this.metricName.length() + 2;
		if(tags!=null && !tags.isEmpty()) {
			for(Map.Entry<String, String> entry: tags.entrySet()) {
				final String key = entry.getKey().trim();
				final String val = entry.getValue().trim();				
				byteSize += key.length() + val.length() + 4;
				this.tags.put(key, val);
			}			
		}		
	}
	
	/**
	 * Creates a new StreamedMetric from a metric key
	 * @param timestamp The timestamp in ms. since the epoch
	 * @param metricKey The metric key
	 * @param value the long value
	 * @return the StreamedMetricValue
	 */
	public static StreamedMetricValue fromKey(final long timestamp, final String metricKey, final long value) {
		final String[] nameKeysPair = Utils.splitString(metricKey, ':', true);
		final Map<String, String> tags = tagsFromArray(0, Utils.splitString(nameKeysPair[1], ',', true));
		return new StreamedMetricValue(timestamp, value, nameKeysPair[0], tags);		
	}
	
	/**
	 * Creates a new StreamedMetric from a metric key
	 * @param timestamp The timestamp in ms. since the epoch
	 * @param metricKey The metric key
	 * @param value the doube value
	 * @return the StreamedMetricValue
	 */
	public static StreamedMetricValue fromKey(final long timestamp, final String metricKey, final double value) {
		final String[] nameKeysPair = Utils.splitString(metricKey, ':', true);
		final Map<String, String> tags = tagsFromArray(0, Utils.splitString(nameKeysPair[1], ',', true));
		return new StreamedMetricValue(timestamp, value, nameKeysPair[0], tags);		
	}
	
	/**
	 * Creates a new StreamedMetric with an auto assigned timestamp
	 * @param metricName The metric name
	 * @param tags The metric tags
	 */
	public StreamedMetric(final String metricName, final Map<String, String> tags) {
		this(System.currentTimeMillis(), metricName, tags);
	}
	

	
	StreamedMetric() {
		
	}
	
	
	/**
	 * Creates a new StreamedMetricValue from this StreamedMetric, the passed value and the passed timestamp
	 * @param timestamp the timestamp to associate with the new StreamedMetricValue 
	 * @param value the value 
	 * @return a new StreamedMetricValue
	 */
	public StreamedMetricValue forValue(final long timestamp, final long value) {
		if(isValued()) return (StreamedMetricValue)this;
		return new StreamedMetricValue(this, timestamp, value);
	}
	
	/**
	 * Creates a new StreamedMetricValue from this StreamedMetric and the passed value
	 * @param value the value
	 * @return a new StreamedMetricValue
	 */
	public StreamedMetricValue forValue(final long value) {
		if(isValued()) return (StreamedMetricValue)this;
		return new StreamedMetricValue(this, value);
	}
	
	/**
	 * Creates a new StreamedMetricValue from this StreamedMetric, the passed value and the passed timestamp
	 * @param timestamp the timestamp to associate with the new StreamedMetricValue 
	 * @param value the value 
	 * @return a new StreamedMetricValue
	 */
	public StreamedMetricValue forValue(final long timestamp, final double value) {
		if(isValued()) return (StreamedMetricValue)this;
		return new StreamedMetricValue(this, timestamp, value);
	}
	
	
	
	/**
	 * Creates a new StreamedMetricValue from this StreamedMetric and the passed value
	 * @param value the value
	 * @return a new StreamedMetricValue
	 */
	public StreamedMetricValue forValue(final double value) {
		if(isValued()) return (StreamedMetricValue)this;
		return new StreamedMetricValue(this, value);
	}
	
	
	/**
	 * Returns a byte array containing the serialized streammetric
	 * @return a byte array 
	 */
	public byte[] toByteArray() {
		final ByteBuf buff = BufferManager.getInstance().directBuffer(byteSize);
		try {
			buff.writeByte(TYPE_CODE);
			writeByteArray(buff);
			return ByteBufUtil.getBytes(buff, 0, buff.readableBytes());
					
		} finally {
			try { buff.release(); } catch (Exception x) {/* No Op */}
		}
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see net.openhft.chronicle.bytes.BytesMarshallable#writeMarshallable(net.openhft.chronicle.bytes.BytesOut)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void writeMarshallable(final BytesOut bytes) {
		bytes.writePosition();
		bytes.writeByte(TYPE_CODE);
		writeByteArray(bytes);		
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.openhft.chronicle.bytes.BytesMarshallable#readMarshallable(net.openhft.chronicle.bytes.BytesIn)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void readMarshallable(final BytesIn bytes) throws IORuntimeException {
		final byte v = bytes.readByte();
		if(v==0) {
			valueType = null;
		} else {
			valueType = ValueType.ordinal(v-1);
		}
		timestamp = bytes.readLong();		
		metricName = bytes.readUtf8();		
		final int tsize = bytes.readByte();			
		for(int i = 0; i < tsize; i++) {
			final String key = bytes.readUtf8();
			final String val = bytes.readUtf8();			
			tags.put(key, val);
		}				
	}
	
	
	/**
	 * Returns a byte array containing the serialized streammetric
	 * @param buff The buffer to write into
	 */
	void writeByteArray(final ByteBuf buff) {
		buff.writeByte(valueType==null ? 0 : valueType.ordinal()+1);
		buff.writeLong(timestamp);
		BufferManager.writeUTF(metricName, buff);			
		buff.writeByte(tags.size());
		for(Map.Entry<String, String> entry: tags.entrySet()) {
			BufferManager.writeUTF(entry.getKey(), buff);
			BufferManager.writeUTF(entry.getValue(), buff);
		}
	}
	
	void writeByteArray(final BytesOut buff) {
		buff.writeByte((byte)(valueType==null ? 0 : valueType.ordinal()+1));
		buff.writeLong(timestamp);
		buff.writeUtf8(metricName);			
		buff.writeByte((byte)tags.size());
		for(Map.Entry<String, String> entry: tags.entrySet()) {
			buff.writeUtf8(entry.getKey());
			buff.writeUtf8(entry.getValue());
		}
	}	
	
	
	/**
	 * Reads a streamed metric from the passed byte array
	 * @param bytes the byte array to read the streamed metric from
	 * @return the appropriate type of StreamedMetric
	 */
	public static StreamedMetric read(final byte[] bytes) {
		final ByteBuf buff = BufferManager.getInstance().directBuffer(bytes.length).writeBytes(bytes);
		try {
			final byte type = buff.readByte();
			switch(type) {
				case 0:
					return StreamedMetric.fromBuff(buff);
				case 1:
					return StreamedMetricValue.fromBuff(buff);
				default:
					throw new RuntimeException("Unrecognized metric type code [" + type + "]");
			}
		} finally {
			try { buff.release(); } catch (Exception x) {/* No Op */}
		}
			
	}
	
	/**
	 * Creates a StreamedMetric from the passed buffer
	 * @param bytes The bytes to read the StreamedMetric from
	 * @return the created StreamedMetric
	 */
	static StreamedMetric fromBuff(final ByteBuf buff) {
		final StreamedMetric sm = new StreamedMetric();
		sm.byteSize = buff.readableBytes() + 1;
		sm.readFromBuff(buff);
		return sm;
	}
	
	void readFromBuff(final ByteBuf buff) {
		final byte v = buff.readByte();
		if(v==0) {
			valueType = null;
		} else {
			valueType = ValueType.ordinal(v-1);
		}
		timestamp = buff.readLong();		
		metricName = BufferManager.readUTF(buff);		
		final int tsize = buff.readByte();			
		for(int i = 0; i < tsize; i++) {
			final String key = BufferManager.readUTF(buff);
			final String val = BufferManager.readUTF(buff);			
			tags.put(key, val);
		}				
	}
	
	
	/**
	 * Sets a value type
	 * @param vt The value type to set
	 * @return this metric
	 */
	public StreamedMetric setValueType(final ValueType vt) {
		if(!vt.valueless) throw new IllegalArgumentException("Invalid value type for StreamedMetric. Type is valued [" + vt.name + "]");
		this.valueType = vt;
		return this;
	}

	/**
	 * Returns the value type
	 * @return the value type
	 */
	public ValueType getValueType() {
		return valueType;
	}

	/**
	 * Returns the metric timestamp
	 * @return the timestamp
	 */
	public long getTimestamp() {
		return timestamp;
	}


	/**
	 * Returns the metric name
	 * @return the metricName
	 */
	public String getMetricName() {
		return metricName;
	}

	/**
	 * Returns a read only map of the metric tags
	 * @return the tags
	 */
	public Map<String, String> getTags() {
		return Collections.unmodifiableMap(tags);
	}
	
	/**
	 * Returns the metric instance key which is basically the metric name plus the tags
	 * @return the metric instance key
	 */
	public String metricKey() {
		if(metricKey==null) {
			synchronized(this) {
				if(metricKey==null) {
					final StringBuilder b = new StringBuilder(metricName);
					if(!tags.isEmpty()) {
						b.append(":");
						for(Map.Entry<String, String> entry: tags.entrySet()) {
							b.append(entry.getKey()).append("=").append(entry.getValue()).append(",");
						}
					}
					metricKey = b.deleteCharAt(b.length()-1).toString();							
				}
			}
		}
		return metricKey;
	}
	
	/**
	 * Indicates if this StreamedMetric has a value
	 * @return false since this type does not have a value
	 */
	public boolean isValued() {
		return false;
	}
	
	/**
	 * <p>Renders this StreamedMetric (or StreamedMetricValue) to a string compatible with being re-parsed back into 
	 * a StreamedMetric (or StreamedMetricValue) using {@link #fromString(String)}</p>
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		//[<value-type>,]<timestamp>, [<value>,] <metric-name>, <host>, <app> [,<tagkey1>=<tagvalue1>,<tagkeyn>=<tagvaluen>]
		final StringBuilder b = new StringBuilder(96);
		if(valueType!=null && valueType != ValueType.DIRECTED) {
			b.append(valueType.charCode).append(",");
		}
		b.append(timestamp).append(",");
		if(isValued()) {
			b.append(((StreamedMetricValue)this).getValue()).append(",");
		}
		b.append(metricName).append(",");
		for(Map.Entry<String, String> entry: tags.entrySet()) {
			b.append(entry.getKey()).append("=").append(entry.getValue()).append(",");
		}
		return b.deleteCharAt(b.length()-1).toString();
	}
	
	
	/**
	 * Renders this metric as an OpenTSDB <b>put</p> text line which is:
	 * <b><code>put &lt;metric&gt; &lt;timestamp&gt; &lt;value&gt; &lt;tagk1=tagv1[ tagk2=tagv2 ...tagkN=tagvN]&gt;</code></b>.
	 * @return the OpenTSDB rendered put line
	 */
	public String toOpenTSDBString() {
		final StringBuilder b = new StringBuilder(96).append("put ")
		.append(metricName).append(" ")
		.append(timestamp).append(" ");
		for(Map.Entry<String, String> entry: tags.entrySet()) {
			b.append(entry.getKey()).append("=").append(entry.getValue()).append(" ");
		}
		return b.toString();
	}
	
	

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((metricName == null) ? 0 : metricName.hashCode());
		result = prime * result + ((tags == null) ? 0 : tags.hashCode());
		result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
		return result;
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof StreamedMetric))
			return false;
		StreamedMetric other = (StreamedMetric) obj;
		if (metricName == null) {
			if (other.metricName != null)
				return false;
		} else if (!metricName.equals(other.metricName))
			return false;
		if (tags == null) {
			if (other.tags != null)
				return false;
		} else if (!tags.equals(other.tags))
			return false;
		if (timestamp != other.timestamp)
			return false;
		return true;
	}
	
	
	/** An empty str/str map const */
	public static final Map<String, String> EMPTY_STR_MAP = Collections.unmodifiableMap(new HashMap<String, String>(0));
	
	/**
	 * Converts the passed array of strings to a tags map.<ul>
	 * <li>Keys and values can be expressed seperately: &lt;<b><code>"key1", "value1", "key2", "value2"....</code></b>&gt;</li>  
	 * <li>or as a pair: &lt;<b><code>"key1=value1", "key2=value2"....</code></b>&gt;</li>
	 * <li>or both: &lt;<b><code>"key1", "value1", "key2=value2"....</code></b>&gt;</li>
	 * </ul>
	 * @param values The string array to convert
	 * @return a map of key value pairs
	 */
	public static Map<String, String> tagsFromArray(final String...values) {
		if(values==null || values.length==0) return EMPTY_STR_MAP;
		final Map<String, String> map = new HashMap<String, String>(8);
		for(int i = 0; i < values.length; i++) {
			if(values[i]==null || values[i].trim().isEmpty()) continue;
			final String s = values[i].trim();
			final int index = s.indexOf('=');
			if(index==-1) {				
				try {
					i++;
					map.put(s, values[i].trim());
				} catch (Exception ex) {
					throw new RuntimeException("Expected KVP value after key [" + s + "]");
				}
			} else {
				final String[] pair =  Utils.splitString(s, '=', true);
				map.put(pair[0], pair[1]);
			}
		}
		return map;
	}
	
	
	/**
	 * Creates a StreamedMetric from the passed string
	 * @param value The string to read from
	 * @return the created StreamedMetric 
	 */
	public static StreamedMetric fromString(final String value) {
		if(value==null || value.trim().isEmpty()) throw new IllegalArgumentException("The passed value was null");
		return fromArray(Utils.splitString(value, ',', true));
	}
	
	/**
	 * Returns a StreamedMetricValue based on this StreamedMetric using the passed value,
	 * unless this is already one, in which case this is returned 
	 * @param value The string to parse to create some form of a StreamedMetric
	 * @param nvalue The value to introduce if this is not a valued metric
	 * @return a StreamedMetricValue
	 */
	public static StreamedMetricValue streamedMetricValue(final String value, final long nvalue) {
		return StreamedMetric.fromString(value).forValue(nvalue);
	}
	
	/**
	 * Returns a StreamedMetricValue based on this StreamedMetric using the passed value,
	 * unless this is already one, in which case this is returned 
	 * @param value The string to parse to create some form of a StreamedMetric
	 * @param nvalue The value to introduce if this is not a valued metric
	 * @return a StreamedMetricValue
	 */
	public static StreamedMetricValue streamedMetricValue(final String value, final double nvalue) {
		return StreamedMetric.fromString(value).forValue(nvalue);
	}
	
	
	/**
	 * Converts all strings in the passed array to a map of tags, starting at the given offset
	 * <li>Keys and values can be expressed seperately: &lt;<b><code>"key1", "value1", "key2", "value2"....</code></b>&gt;</li>  
	 * <li>or as a pair: &lt;<b><code>"key1=value1", "key2=value2"....</code></b>&gt;</li>
	 * <li>or both: &lt;<b><code>"key1", "value1", "key2=value2"....</code></b>&gt;</li>
	 * </ul>
	 * @param offset The offset in the array to start at
	 * @param values The array of values to convert
	 * @return a map of tags
	 */
	public static Map<String, String> tagsFromArray(final int offset, final String...values) {
		final int len = values.length - offset;
		final String[] arr = new String[len];
		System.arraycopy(values, offset, arr, 0, len);
		return tagsFromArray(arr);
	}
	
	
	//   [<value-type>,]<timestamp>, [<value>,] <metric-name>, <host>, <app> [,<tagkey1>=<tagvalue1>,<tagkeyn>=<tagvaluen>]
	
	/**
	 * Builds a StreamedMetric from the passed array of values.
	 * @param values An array of values to create the streamed metric from
	 * @return a StreamedMetric instance
	 * FIXME: clean this up 
	 */
	public static StreamedMetric fromArray(final String...values) {
		if(values==null || values.length < 4) throw new IllegalArgumentException("The passed array is invalid [" + (values==null ? null : Arrays.toString(values)) + "]");
		final ValueType valueType = ValueType.decode(values[0]);
		for(int i = 0; i < values.length; i++) {
			values[i] = values[i].trim();
		}
		String metricName = null;		
		long timestamp = -1;
		long longValue = -1;
		double doubleValue = -1D;
		boolean isDouble = true;
		Number n = null;
		Map<String, String> tags = new HashMap<String, String>();		
		if(valueType==ValueType.DIRECTED) {
			timestamp = Utils.toMsTime(values[0]);
			n = Utils.numeric(values[1]);
			if(n!=null) {
				if(Utils.isDouble(values[1])) {
					doubleValue = n.doubleValue();
					isDouble = true;
				} else {
					longValue = n.longValue();
					isDouble = false;					
				}
				metricName = values[2];
				tags.put("host", values[3]);
				tags.put("app", values[4]);
				tags.putAll(tagsFromArray(5, values));
				return isDouble ? new StreamedMetricValue(timestamp, doubleValue, metricName, tags) : new StreamedMetricValue(timestamp, longValue, metricName, tags);
			}
			metricName = values[1];
			tags.put("host", values[2]);
			tags.put("app", values[3]);
			tags.putAll(tagsFromArray(4, values));
			return new StreamedMetric(timestamp, metricName, tags);
		}
		timestamp = Utils.toMsTime(values[1]);			
		if(valueType.valueless) {
			metricName = values[2];
			tags.put("host", values[3]);
			tags.put("app", values[4]);
			tags.putAll(tagsFromArray(5, values));
			return new StreamedMetric(timestamp, metricName, tags).setValueType(valueType);
		}
		isDouble = Utils.isDouble(values[2]);
		if(isDouble) {
			doubleValue = Double.parseDouble(values[2]);
		} else {
			longValue = Long.parseLong(values[2]);
		}
		metricName = values[3];
		tags.put("host", values[4]);
		tags.put("app", values[5]);
		tags.putAll(tagsFromArray(6, values));
		return isDouble ? new StreamedMetricValue(timestamp, doubleValue, metricName, tags).setValueType(valueType) : new StreamedMetricValue(timestamp, longValue, metricName, tags).setValueType(valueType);
		
	}
	
	
	

	

}
