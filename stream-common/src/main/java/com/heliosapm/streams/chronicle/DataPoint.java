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
package com.heliosapm.streams.chronicle;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.core.io.IORuntimeException;

/**
 * <p>Title: DataPoint</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.chronicle.DataPoint</code></p>
 */

public class DataPoint implements BytesMarshallable {
	public static final byte ZERO_BYTE = 0;
	public static final byte ONE_BYTE = 1;
	
	private static final ThreadLocal<DataPoint> DP = new ThreadLocal<DataPoint>() {
		/**
		 * {@inheritDoc}
		 * @see java.lang.ThreadLocal#initialValue()
		 */
		@Override
		protected DataPoint initialValue() {			
			return new DataPoint();
		}
	};
	
	String metricName = null;
	final Map<String, String> tags = new HashMap<String, String>();
	long timestamp = -1L;
	boolean doubleType = true;
	long longValue = -1L;
	double doubleValue = -1D;
	byte[] tsuid = null;
	
	public static DataPoint get() {
		return DP.get();
	}

	public static DataPoint getAndReset() {
		return DP.get().reset();
	}
	
	private DataPoint() {}
	
	private DataPoint(final DataPoint dp) {
		this.tags.putAll(dp.tags);
		this.metricName = dp.metricName;
		this.timestamp = dp.timestamp;
		this.doubleType = dp.doubleType;
		this.longValue = dp.longValue;
		this.doubleValue = dp.doubleValue;
		this.tsuid = dp.tsuid;
	}
	
	/**
	 * <p>Creates an independent deep clone of this data point</p>
	 * {@inheritDoc}
	 * @see java.lang.Object#clone()
	 */
	public DataPoint clone() {
		return new DataPoint(this);
	}

	/**
	 * Acquires the datapoint instance for the current thread and loads it with the passed event
	 * @param metric the metric name
	 * @param timestamp The timestamp
	 * @param value The value
	 * @param tags The metric tags
	 * @param tsuid The FQ tsuid
	 * @return the loaded datapoint
	 */
	public static DataPoint dp(final String metric, final long timestamp, final double value, final Map<String,String> tags, final byte[] tsuid) {
		final DataPoint dp = getAndReset();
		dp.metricName = metric;
		dp.timestamp = timestamp;
		dp.doubleType = true;
		dp.doubleValue = value;
		dp.tags.putAll(tags);
		dp.tsuid = tsuid;
		return dp;		
	}
	
	/**
	 * Acquires the datapoint instance for the current thread and loads it with the passed event
	 * @param metric the metric name
	 * @param timestamp The timestamp
	 * @param value The value
	 * @param tags The metric tags
	 * @param tsuid The FQ tsuid
	 * @return the loaded datapoint
	 */
	public static DataPoint dp(final String metric, final long timestamp, final long value, final Map<String,String> tags, final byte[] tsuid) {
		final DataPoint dp = getAndReset();
		dp.metricName = metric;
		dp.timestamp = timestamp;
		dp.doubleType = false;
		dp.longValue = value;
		dp.tags.putAll(tags);
		dp.tsuid = tsuid;
		return dp;		
	}
	
	
	public DataPoint reset() {
		tags.clear();
		return this;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.openhft.chronicle.bytes.BytesMarshallable#writeMarshallable(net.openhft.chronicle.bytes.BytesOut)
	 */
	@Override
	public void writeMarshallable(final BytesOut bytes) {		
		bytes.writeUtf8(metricName);
		bytes.writeInt(tags.size());
		for(Map.Entry<String, String> tag: tags.entrySet()) {
			bytes.writeUtf8(tag.getKey());
			bytes.writeUtf8(tag.getValue());
		}
		bytes.writeLong(timestamp);
		bytes.writeInt(tsuid.length);
		bytes.write(tsuid);
		if(doubleType) {
			bytes.writeByte(ZERO_BYTE);
			bytes.writeDouble(doubleValue);
		} else {
			bytes.writeByte(ONE_BYTE);
			bytes.writeLong(longValue);			
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.openhft.chronicle.bytes.BytesMarshallable#readMarshallable(net.openhft.chronicle.bytes.BytesIn)
	 */
	@Override
	public void readMarshallable(final BytesIn bytes) throws IORuntimeException {
		metricName = bytes.readUtf8();
		final int tagCount = bytes.readInt();
		for(int i = 0; i < tagCount; i++) {
			tags.put(bytes.readUtf8(), bytes.readUtf8());
		}
		
		timestamp = bytes.readLong();
		tsuid = new byte[bytes.readInt()];
		bytes.read(tsuid);
		doubleType = bytes.readByte()==ZERO_BYTE;
		if(doubleType) {
			doubleValue = bytes.readDouble();
		} else {
			longValue = bytes.readLong();
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {		
		return new StringBuilder(metricName)
			.append(":")
			.append(tags)
			.append(", ts:").append(new Date(timestamp))
			.append(", v:").append(doubleType ? doubleValue : longValue)
			.append(", tsuid:").append(DatatypeConverter.printHexBinary(tsuid))
			.toString();
	}

	/**
	 * Returns the
	 * @return the metricName
	 */
	public String getMetricName() {
		return metricName;
	}

	/**
	 * Returns the
	 * @return the tags
	 */
	public Map<String, String> getTags() {
		return tags;
	}

	/**
	 * Returns the
	 * @return the timestamp
	 */
	public long getTimestamp() {
		return timestamp;
	}

	/**
	 * Returns the
	 * @return the doubleType
	 */
	public boolean isDoubleType() {
		return doubleType;
	}

	/**
	 * Returns the
	 * @return the longValue
	 */
	public long getLongValue() {
		return longValue;
	}

	/**
	 * Returns the
	 * @return the doubleValue
	 */
	public double getDoubleValue() {
		return doubleValue;
	}

	/**
	 * Returns the
	 * @return the tsuid
	 */
	public byte[] getTsuid() {
		return tsuid;
	}
	
	
	// String metric, long timestamp, double value, Map<String,String> tags, byte[] tsuid
}
