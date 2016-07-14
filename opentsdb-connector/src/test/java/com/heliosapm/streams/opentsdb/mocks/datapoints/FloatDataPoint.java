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

import java.util.Map;

import net.opentsdb.core.TSDB;


/**
 * <p>Title: FloatDataPoint</p>
 * <p>Description: A float value data point</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.tsdbex.datapoints.FloatDataPoint</code></p>
 */

public class FloatDataPoint extends DataPoint {
	/** The float value */
	public final float value;
	/**
	 * Creates a new FloatDataPoint
	 * @param value The float value for this datapoint
	 * @param metricName The metric name, a non-empty string.
	 * @param tags The metric tags. Must be non empty
	 * @param timestamp  The timestamp associated with the value.
	 */
	public FloatDataPoint(float value, String metricName, Map<String, String> tags, long timestamp) {
		super(metricName, tags, timestamp);
		this.value = value;
	}

	/**
	 * Creates a new FloatDataPoint
	 * @param value The float value for this datapoint
	 * @param metricName The metric name, a non-empty string.
	 * @param tags The metric tags. Must be non empty
	 */
	public FloatDataPoint(float value, String metricName, Map<String, String> tags) {
		super(metricName, tags);
		this.value = value;
	}

	/**
	 * Creates a new FloatDataPoint
	 * @param value The float value for this datapoint
	 * @param metricName The metric name, a non-empty string.
	 * @param tags The metric tags. Must be a non empty, even numbered number of non empty strings
	 */
	public FloatDataPoint(float value, String metricName, String... tags) {
		super(metricName, tags);
		this.value = value;
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.helios.tsdb.plugins.datapoints.DataPoint#getValue()
	 */
	public Float getValue() {
		return value;
	}
	
	
	/**
	 * Publishes this datapoint the passed TSDB
	 * @param tsdb the TSDB to publish to
	 */
	public void publish(TSDB tsdb) {
		tsdb.addPoint(metricName, timestamp, value, tags);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.core.DataPoint#doubleValue()
	 */
	@Override
	public double doubleValue() {
		return value;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.core.DataPoint#isInteger()
	 */
	@Override
	public boolean isInteger() {
		return false;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.core.DataPoint#longValue()
	 */
	@Override
	public long longValue() {
		return (long)value;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.core.DataPoint#timestamp()
	 */
	@Override
	public long timestamp() {
		return timestamp;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.core.DataPoint#toDouble()
	 */
	@Override
	public double toDouble() {
		return value;
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + Float.floatToIntBits(value);
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
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		FloatDataPoint other = (FloatDataPoint) obj;
		if (Float.floatToIntBits(value) != Float.floatToIntBits(other.value))
			return false;
		return true;
	}
	


}
