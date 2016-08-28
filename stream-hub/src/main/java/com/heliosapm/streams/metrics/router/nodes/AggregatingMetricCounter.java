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
 * <p>Description: A minimal aggregator/aggregatable for testing</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.router.nodes.AggregatingMetricCounter</code></p>
 */

public class AggregatingMetricCounter implements Aggregator<AggregatingMetricCounter> {
	final String key;
	final AtomicLong count = new AtomicLong(0L);
	final long timestamp;
	
	/** A shareable aggregator */
	public static final AggregatingMetricCounter AGGREGATOR = new AggregatingMetricCounter();
	
	/**
	 * Creates a new AggregatingMetricCounter
	 * @param key The key
	 * @param timestamp The timestamp in ms.
	 * @param initialCount The initial count
	 */
	public AggregatingMetricCounter(final String key, final long timestamp, final long initialCount) {
		this.key = key;
		this.timestamp = timestamp;
		count.addAndGet(initialCount);
	}
	
	private AggregatingMetricCounter() {
		this.key = null;
		this.timestamp = -1L;
	}
	

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.router.nodes.Aggregator#timestamp(java.util.concurrent.TimeUnit, java.lang.Object)
	 */
	@Override
	public long timestamp(final TimeUnit unit, final AggregatingMetricCounter t) {
		return unit.convert(timestamp, TimeUnit.MILLISECONDS);
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.router.nodes.Aggregator#aggregateInto(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void aggregateInto(final AggregatingMetricCounter to, final AggregatingMetricCounter from) {
		to.count.addAndGet(from.count.get());
		
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
