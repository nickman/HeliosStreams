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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.heliosapm.streams.buffers.BufferManager;

import io.netty.buffer.ByteBuf;

/**
 * <p>Title: StreamedMetric</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.StreamedMetric</code></p>
 */

public class StreamedMetric {
	/** Indicates if the value is a double (true) or a long (false)  */
	protected boolean isDoubleValue = true;
	/** The long typed value */
	protected long longValue = -1L;
	/** The metric timestamp in ms. since the epoch */
	protected long timestamp = -1L;
	/** The double typed value */
	protected double doubleValue = -1L;
	/** The metric name */
	protected String metricName = null;
	/** The metric tags */
	protected final Map<String, String> tags = new HashMap<String, String>(8);
	/** The estimated byte size */
	protected int byteSize = BASE_SIZE;
	
	private static final int BASE_SIZE = 18;
	
	
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
	 * @param value The value of the metric
	 * @param metricName The metric name
	 * @param tags The metric tags
	 */
	public StreamedMetric(final long timestamp, final long value, final String metricName, final Map<String, String> tags) {		
		this.timestamp = timestamp;
		this.longValue = value;
		isDoubleValue = false;
		this.metricName = metricName.trim();
		byteSize += this.metricName.length();
		if(tags!=null && !tags.isEmpty()) {
			for(Map.Entry<String, String> entry: tags.entrySet()) {
				final String key = entry.getKey().trim();
				final String val = entry.getValue().trim();
				byteSize += key.length() + val.length() + 2;
				this.tags.put(key, val);
			}
		}		
	}
	
	/**
	 * Creates a new StreamedMetric with an auto assigned timestamp
	 * @param value The value of the metric
	 * @param metricName The metric name
	 * @param tags The metric tags
	 */
	public StreamedMetric(final long value, final String metricName, final Map<String, String> tags) {
		this(System.currentTimeMillis(), value, metricName, tags);
	}
	

	/**
	 * Creates a new StreamedMetric
	 * @param timestamp The timestamp in ms. since the epoch
	 * @param value The value of the metric
	 * @param metricName The metric name
	 * @param tags The metric tags
	 */
	public StreamedMetric(final long timestamp, final double value, final String metricName, final Map<String, String> tags) {		
		this.timestamp = timestamp;
		this.doubleValue = value;
		isDoubleValue = true;
		this.metricName = metricName.trim();
		byteSize += this.metricName.length();
		if(tags!=null && !tags.isEmpty()) {
			for(Map.Entry<String, String> entry: tags.entrySet()) {
				final String key = entry.getKey().trim();
				final String val = entry.getValue().trim();
				byteSize += key.length() + val.length() + 2;
				this.tags.put(key, val);
			}
		}
	}
	
	/**
	 * Creates a new StreamedMetric with an auto assigned timestamp
	 * @param value The value of the metric
	 * @param metricName The metric name
	 * @param tags The metric tags
	 */
	public StreamedMetric(final double value, final String metricName, final Map<String, String> tags) {
		this(System.currentTimeMillis(), value, metricName, tags);
	}
	
	private StreamedMetric() {
		
	}
	
	/**
	 * Returns a byte array containing the serialized streammetric
	 * @return a byte array 
	 */
	public byte[] toByteArray() {
		final ByteBuf buff = BufferManager.getInstance().heapBuffer(byteSize);
		try {
			buff.writeBoolean(isDoubleValue);
			buff.writeLong(timestamp);
			if(isDoubleValue) {
				buff.writeDouble(doubleValue);
			} else {
				buff.writeLong(longValue);
			}
			BufferManager.writeUTF(metricName, buff);			
			buff.writeByte(tags.size());
			for(Map.Entry<String, String> entry: tags.entrySet()) {
				BufferManager.writeUTF(entry.getKey(), buff);
				BufferManager.writeUTF(entry.getValue(), buff);
			}
			return buff.array();
		} finally {
			try { buff.release(); } catch (Exception x) {/* No Op */}
		}
	}
	
	/**
	 * Creates a StreamedMetric from the passed byte array
	 * @param bytes The bytes to read the StreamedMetric from
	 * @return the created StreamedMetric
	 */
	public static StreamedMetric fromByteArray(final byte[] bytes) {
		final StreamedMetric sm = new StreamedMetric();
		sm.byteSize = bytes.length;
		final ByteBuf buff = BufferManager.getInstance().heapBuffer(bytes.length).writeBytes(bytes);
		try {
			sm.isDoubleValue = buff.readBoolean();
			sm.timestamp = buff.readLong();
			if(sm.isDoubleValue) {
				sm.doubleValue = buff.readDouble();
			} else {
				sm.longValue = buff.readLong();
			}
			sm.metricName = BufferManager.readUTF(buff);
			final int tsize = buff.readByte();
			for(int i = 0; i < tsize; i++) {
				final String key = BufferManager.readUTF(buff);
				sm.tags.put(key, BufferManager.readUTF(buff));
			}
			return sm;
		} finally {
			try { buff.release(); } catch (Exception x) {/* No Op */}
		}
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
	 * Returns the metric timestamp
	 * @return the timestamp
	 */
	public long getTimestamp() {
		return timestamp;
	}

	/**
	 * Returns the double value
	 * @return the doubleValue
	 */
	public double getDoubleValue() {
		return doubleValue;
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
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return new StringBuilder(96)
			.append("[").append(timestamp).append("] ")
			.append(isDoubleValue ? doubleValue : longValue).append(" ")
			.append(metricName).append(" ")
			.append(tags)
			.toString();
	}
	
	
	/**
	 * Renders this metric as an OpenTSDB <b>put</p> text line which is:
	 * <b><code>put &lt;metric&gt; &lt;timestamp&gt; &lt;value&gt; &lt;tagk1=tagv1[ tagk2=tagv2 ...tagkN=tagvN]&gt;</code></b>.
	 * @return the OpenTSDB rendered put line
	 */
	public String toOpenTSDBString() {
		final StringBuilder b = new StringBuilder(96).append("put ")
		.append(metricName).append(" ")
		.append(timestamp).append(" ")
		.append(isDoubleValue ? doubleValue : longValue).append(" ");
		
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
	
	

	

	

}
