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
package com.heliosapm.streams.opentsdb.mocks.datapoints;

import java.nio.charset.Charset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;


import net.opentsdb.core.TSDB;

/**
 * <p>Title: DataPoint</p>
 * <p>Description: Encapsulates a syntehtic data point for submission to the TSDB.</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.tsdbex.datapoints.DataPoint</code></p>
 */

public abstract class DataPoint implements net.opentsdb.core.DataPoint {
	/** The metric name, a non-empty string. */	
	public final String metricName;
	/** The metric tags. Must be non empty */
	public final Map<String, String> tags;
	/** The timestamp associated with the value. */
	public final long timestamp;
	
	/** The UTF8 character set */
	public static final Charset UTF8 = Charset.forName("UTF8");
	
	protected static final AtomicLong serial = new AtomicLong();
	
	/**
	 * Creates a new DataPoint
	 * @param metricName The metric name, a non-empty string.
	 * @param tags The metric tags. Must be non empty
	 * @param timestamp  The timestamp associated with the value.
	 */
	public DataPoint(String metricName, Map<String, String> tags, long timestamp) {
		super();
		this.metricName = metricName;
		this.tags = tags;
		this.timestamp = timestamp;
	}
	
	/**
	 * Creates a new DataPoint for the current time
	 * @param metricName The metric name, a non-empty string.
	 * @param tags The metric tags. Must be non empty
	 */
	public DataPoint(String metricName, Map<String, String> tags) {
		this.metricName = metricName;
		this.tags = tags;
		this.timestamp = System.currentTimeMillis();
	}
	
	/**
	 * Creates a new DataPoint for the current time
	 * @param metricName The metric name, a non-empty string.
	 * @param tags The metric tags. Must be a non empty, even numbered number of non empty strings
	 */
	public DataPoint(String metricName, String...tags) {
		if(tags==null || tags.length==0) throw new IllegalArgumentException("Passed tags array was null or empty");
		if(tags.length%2!=0) throw new IllegalArgumentException("Passed tags array had an odd number of tags");
		
		this.metricName = metricName;
		this.timestamp = System.currentTimeMillis();
		Map<String, String> map = new LinkedHashMap<String, String>();
		for(int i = 0; i < tags.length;) {
			map.put(tags[i++], tags[i++]);
		}
		this.tags = map;
	}
	
	
	
	/**
	 * Creates a new DataPoint
	 * @param value The value for this datapoint
	 * @param metricName The metric name, a non-empty string.
	 * @param tags The metric tags. Must be non empty
	 * @param timestamp  The timestamp associated with the value.
	 * @return A correctly typed data point in accordance with the size of the value
	 */
	public static DataPoint newDataPoint(Number value, String metricName, Map<String, String> tags, long timestamp) {
		// Long < Float < Double
		double v = value.doubleValue();
		if(v <= Long.MAX_VALUE) {
			return new LongDataPoint(value.longValue(), metricName, tags, timestamp);
		} else if(v <= Float.MAX_VALUE) {
			return new FloatDataPoint(value.floatValue(), metricName, tags, timestamp);
		} else {
			return new DoubleDataPoint(v, metricName, tags, timestamp);
		}
	}
	
	/**
	 * Publishes this datapoint the passed TSDB
	 * @param tsdb the TSDB to publish to
	 */
	public abstract void publish(TSDB tsdb);
	
	/**
	 * Retrieves the datapoint's value
	 * @return the datapoint's value
	 */
	public abstract Number getValue();

	
	/**
	 * Generates a unique key for this data point
	 * @return a unique key for this data point
	 */
	public String getKey() {
		StringBuilder b = new StringBuilder(metricName);
		b.append("/").append(timestamp).append(":");
		for(Map.Entry<String, String> entry: tags.entrySet()) {
			b.append("[").append(entry.getKey()).append("/").append(entry.getValue()).append("]");
		}
		return b.toString();
	}

	/**
	 * Creates a new DataPoint
	 * @param value The float value for this datapoint
	 * @param metricName The metric name, a non-empty string.
	 * @param tags The metric tags. Must be non empty
	 * @return A correctly typed data point in accordance with the size of the value
	 */
	public static DataPoint newDataPoint(Number value, String metricName, Map<String, String> tags) {
		double v = value.doubleValue();
		if(v <= Long.MAX_VALUE) {
			return new LongDataPoint(value.longValue(), metricName, tags);
		} else if(v <= Float.MAX_VALUE) {
			return new FloatDataPoint(value.floatValue(), metricName, tags);
		} else {
			return new DoubleDataPoint(v, metricName, tags);
		}
	}
	

	

	/**
	 * Creates a new DataPoint
	 * @param value The float value for this datapoint
	 * @param metricName The metric name, a non-empty string.
	 * @param tags The metric tags. Must be a non empty, even numbered number of non empty strings
	 * @return A correctly typed data point in accordance with the size of the value
	 */
	public static DataPoint newDataPoint(Number value, String metricName, String... tags) {
		double v = value.doubleValue();
		if(v <= Long.MAX_VALUE) {
			return new LongDataPoint(value.longValue(), metricName, tags);
		} else if(v <= Float.MAX_VALUE) {
			return new FloatDataPoint(value.floatValue(), metricName, tags);
		} else {
			return new DoubleDataPoint(v, metricName, tags);
		}
	}
	

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((metricName == null) ? 0 : metricName.hashCode());
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
		if (getClass() != obj.getClass())
			return false;
		DataPoint other = (DataPoint) obj;
		if (metricName == null) {
			if (other.metricName != null)
				return false;
		} else if (!metricName.equals(other.metricName))
			return false;
		if (tags == null) {
			if (other.tags != null)
				return false;
		} else { //if (!tags.equals(other.tags))
			if(tags.size() != other.tags.size()) return false;
			for(Map.Entry<String, String> entry: tags.entrySet()) {
				String otherValue = other.tags.get(entry.getKey());
				if(!entry.getValue().equals(otherValue)) return false;
			}
		}
		if (timestamp != other.timestamp)
			return false;
		return true;
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return getKey() + ":" + getValue();
	}
	
	
	

}
