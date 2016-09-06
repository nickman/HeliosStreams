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
package com.heliosapm.streams.tracing.writers;

import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.heliosapm.streams.metrics.StreamedMetric;

/**
 * <p>Title: JMXStreamedMetric</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tracing.writers.JMXStreamedMetric</code></p>
 */

public class JMXStreamedMetric implements JMXStreamedMetricMBean {
	/** The metric definition */
	protected final StreamedMetric sm;
	/** The most recent submission timestamp */
	protected final AtomicLong timestamp = new AtomicLong(-1L);
	/** The most recent submission value */
	protected final AtomicReference<Number> value = new AtomicReference<Number>(null);
	/** The number of observed submissions */
	protected final AtomicLong observed = new AtomicLong(1L);
	
	
	/**
	 * Creates a new JMXStreamedMetric
	 * @param sm The initial streamed metric to initialize the register
	 */
	public JMXStreamedMetric(final StreamedMetric sm) {
		this.sm = sm;
		timestamp.set(sm.getTimestamp());
		value.set(sm.forValue(1L).getValueNumber());
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.writers.JMXStreamedMetricMBean#getDate()
	 */
	public Date getDate() {
		return new Date(timestamp.get());
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.writers.JMXStreamedMetricMBean#getTimestamp()
	 */
	public long getTimestamp() {
		return timestamp.get();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.writers.JMXStreamedMetricMBean#getValue()
	 */
	public Number getValue() {
		return value.get();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.writers.JMXStreamedMetricMBean#getObservedCount()
	 */
	public long getObservedCount() {
		return observed.get();
	}
	
	void update(final StreamedMetric sm) {
		timestamp.set(sm.getTimestamp());
		value.set(sm.forValue(1L).getValueNumber());
		observed.incrementAndGet();
	}
}
