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
package com.heliosapm.streams.metrics.router.nodes;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <p>Title: AggregatingMetricCounter</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.router.nodes.AggregatingMetricCounter</code></p>
 */

public class AggregatingMetricCounter implements Aggregatable<AggregatingMetricCounter> {
	final String key;
	final AtomicLong count = new AtomicLong(0L);
	final long timestamp;
	
	/**
	 * Creates a new AggregatingMetricCounter
	 */
	public AggregatingMetricCounter(final String key, final long timestamp, final long initialCount) {
		this.key = key;
		this.timestamp = timestamp;
		count.addAndGet(initialCount);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.router.nodes.Aggregatable#timestamp(java.util.concurrent.TimeUnit)
	 */
	@Override
	public long timestamp(final TimeUnit unit) {
		return unit.convert(timestamp, TimeUnit.MILLISECONDS);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.router.nodes.Aggregatable#get()
	 */
	@Override
	public AggregatingMetricCounter get() {		
		return this;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.router.nodes.Aggregatable#aggregateInto(com.heliosapm.streams.metrics.router.nodes.Aggregatable)
	 */
	@Override
	public void aggregateInto(final AggregatingMetricCounter from) {
		count.addAndGet(from.count.get());
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.router.nodes.Aggregatable#newInstance(com.heliosapm.streams.metrics.router.nodes.Aggregatable)
	 */
	@Override
	public AggregatingMetricCounter newInstance(Aggregatable<AggregatingMetricCounter> from) {
		return new AggregatingMetricCounter(key, timestamp, count.get());
	}

	/**
	 * Returns the key
	 * @return the key
	 */
	public String getKey() {
		return key;
	}
	
	/**
	 * Returns the count
	 * @return the count
	 */
	public long getCount() {
		return count.get();
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {		
		//return "Metric:" + key + "[" + count.get() + "] - " + new Date(timestamp);
		return "" + count.get();
	}

}
