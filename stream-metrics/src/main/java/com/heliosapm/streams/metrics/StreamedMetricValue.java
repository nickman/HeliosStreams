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

import java.util.Map;

import com.heliosapm.streams.buffers.BufferManager;

import io.netty.buffer.ByteBuf;

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
		super(timestamp, value, metricName, tags);
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
		super(timestamp, value, metricName, tags);
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
	 * Indicates if this StreamedMetric has a value
	 * @return true since this type has a value
	 */
	public boolean isValued() {
		return true;
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
	 * Returns a byte array containing the serialized streammetric
	 * @return a byte array 
	 */
	public byte[] toByteArray() {
		final ByteBuf buff = BufferManager.getInstance().heapBuffer(byteSize);
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
			return buff.array();
		} finally {
			try { buff.release(); } catch (Exception x) {/* No Op */}
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
	
	

}
