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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.cliffc.high_scale_lib.NonBlockingHashMap;

import com.heliosapm.utils.unsafe.UnsafeAdapter;
import com.heliosapm.utils.unsafe.UnsafeAdapter.SpinLock;

/**
 * <p>Title: WindowRange</p>
 * <p>Description: A time range within which a metric's timestamp may lie</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.router.nodes.WindowRange</code></p>
 */

public class WindowRange<K,V,A extends Aggregator<V>> {
	/** The duration of the range in seconds */
	protected final long rangeSize;
	/** The duration of the range in ms */
	protected final long rangeSizeMs;
	
	/** The aggregator */
	protected final A aggregator;
	/** The current range */
	protected final long[] range;
	/** The accumulation for the current range */
	protected final NonBlockingHashMap<K,V> currentAggregations = new NonBlockingHashMap<K,V>();
	/** The empty results if no aggregation is available */
	protected final Map<K,V> empty = Collections.unmodifiableMap(Collections.emptyMap());
	/** A counter of late arrivals that could not be aggregated */
	protected final LongAdder lateArrivals = new LongAdder();
	/** A spin lock to ensure correct ordering */
	protected final SpinLock lock = UnsafeAdapter.allocateSpinLock();
	

	/**
	 * Creates a new WindowRange
	 * @param rangeSize The duration of the range in seconds
	 */
	public WindowRange(final long rangeSize, final A aggregator) {
		
		if(rangeSize < 1L) throw new IllegalArgumentException("Invalid range size: [" + rangeSize + "]");
		if(aggregator==null) throw new IllegalArgumentException("Passed aggregator was null");
		this.rangeSize = rangeSize;
		rangeSizeMs = TimeUnit.SECONDS.toMillis(rangeSize);
		this.aggregator = aggregator;
		range = new long[]{0L, rangeSizeMs};
	}
	
	public Map<K,V> aggregate(final K key, final V value) {
		try {
			lock.xlock();
			final long ts = aggregator.timestamp(TimeUnit.MILLISECONDS, value);
			final RangeEval re = eval(ts);
			switch(re) {
			case FIRST:
				range[0] = rangeFor(ts);
				range[1] = range[0] + rangeSizeMs;
				return aggregate(key, value);			
			case BEFORE:
				lateArrivals.increment();
				break;
			case AFTER:
				range[0] = rangeFor(ts);
				range[1] = range[0] + rangeSizeMs;
				final Map<K,V> oldPeriod = flush();
				within(key, value);
				return oldPeriod;
			case WITHIN:
				within(key, value);
				break;
				
			}			
			return empty;
		} finally {
			lock.xunlock();
		}
	}
	
	protected Map<K,V> flush() {
		final Map<K,V> map = new HashMap<K,V>(currentAggregations);
		currentAggregations.clear();
		return map;
	}
	
	/**
	 * Computes the start time of the window for the passed ms based timestamp
	 * @param timestampMs The timestamp in ms
	 * @return the start of the time window
	 */
	public long rangeFor(final long timestampMs) {
		return timestampMs - (timestampMs%rangeSizeMs); 
	}
	
	
	protected void within(final K key, final V value) {
		final V prior = currentAggregations.put(key, value);
		if(prior!=null) {
			aggregator.aggregateInto(value, prior);
		}
	}
	
	protected RangeEval eval(final long timestampMs) {
		if(range[0]==0L) return RangeEval.FIRST;
		if(timestampMs < range[0]) return RangeEval.BEFORE;
		if(timestampMs > range[1]) return RangeEval.AFTER;
		return RangeEval.WITHIN;
	}
	
	
	
}
