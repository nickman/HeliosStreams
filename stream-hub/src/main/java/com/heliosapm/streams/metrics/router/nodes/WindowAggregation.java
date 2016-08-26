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

import java.util.concurrent.Callable;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;

/**
 * <p>Title: WindowAggregation</p>
 * <p>Description: Aggregation construct to accumulate stream events ("off-line") for a specified window of time.</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.router.nodes.WindowAggregation</code></p>
 * @param <K> The aggregation type's key
 * @param <V> The aggregation type's value
 */

public class WindowAggregation<K, V extends Aggregatable> implements Runnable {
	/** The length of the window in seconds */
	private final long windowDuration;	
	/** The earliest timestamp accepted by this window, which is initialized to the first submission and 
	 * updated to the end of expiring windows (plus one) as they expire 
	 */
	private final AtomicLong earliestTimestamp = new AtomicLong(0L);
	/** A map of aggregation maps keyed by the start time of the time window being aggregated */
	private final NonBlockingHashMapLong<NonBlockingHashMap<K, V>> delayIndex = new NonBlockingHashMapLong<NonBlockingHashMap<K, V>>();
	/** The delay queue which supplies expiring aggregation maps as their time window expires */
	private final DelayQueue<TimestampDelay> delayQueue = new DelayQueue<TimestampDelay>();
	/** The keep running flag */
	private final AtomicBoolean running = new AtomicBoolean(true);
	/** Instance logger */
	private final Logger log;
	
	@SuppressWarnings("unchecked")
	private final V placeholder = (V) new Aggregatable() {
		@Override
		public long timestamp(TimeUnit unit) {
			return 0;
		}

		@Override
		public void aggregateInto(final Aggregatable from) {
		}
	};
	
	/**
	 * Creates a new WindowAggregation
	 * @param windowDuration The length (time) of the aggregaton window in seconds
	 */
	public WindowAggregation(final long windowDuration) {
		this.windowDuration = windowDuration;
		log = LogManager.getLogger(getClass().getName() + "-" + windowDuration);
	}
	
	public boolean aggregate(final K key, final V value) {
		if(!running.get()) throw new IllegalStateException("This WindowAggregation was stopped");
		final long ts = windowStartTime(value.timestamp(TimeUnit.SECONDS));
		if(!earliestTimestamp.compareAndSet(0L, ts)) {
			if(ts < earliestTimestamp.get()) return false;
		}
		final NonBlockingHashMap<K, V> aggrMap = delayIndex.get(ts, new Callable<NonBlockingHashMap<K, V>>() {
			@Override
			public NonBlockingHashMap<K, V> call() throws Exception {
				delayQueue.put(new TimestampDelay(ts));
				return new NonBlockingHashMap<K, V>();
			}
		});
		V aggr = aggrMap.putIfAbsent(key, placeholder);
		if(aggr==null || aggr==placeholder) {
			aggrMap.replace(key, value);
		} else {
			aggr.aggregateInto(value);
		}
		return true;
	}
	
	/**
	 * Computes the start time of the window for the passed second based timestamp
	 * @param timestampSecs The timestamp in seconds
	 * @return the start of the time window
	 */
	public long windowStartTime(final long timestampSecs) {
		return timestampSecs - (timestampSecs%windowDuration); 
	}
	
	/**
	 * Closes this aggregation
	 */
	public void close() {
//		if(timerHandle!=null) try { timerHandle.cancel(false); } catch (Exception x) {/* No Op */}
//		store.clear();
	}
	
	public void run() {
		log.info("Expiration thread started");
		while(running.get()) {
			try {
				final TimestampDelay td = delayQueue.take();
			} catch (InterruptedException iex) {
				if(running.get()) {
					if(Thread.interrupted()) Thread.interrupted();
				} else {
					break;
				}
			} catch (Exception ex) {
				log.error("Expiration thread error",ex);
			}
		}
		log.info("Expiration thread ended");
	}

	static class TimestampDelay implements Delayed {
		/** The timestamp representing the start of a time window in seconds */
		private final long timestamp;
		/** The timestamp in ms. */
		private final long timestampMs;
		
		/**
		 * Creates a new TimestampDelay
		 * @param timestamp The timestamp representing the start of a time window in seconds
		 */
		public TimestampDelay(final long timestamp) {
			this.timestamp = timestamp;
			this.timestampMs = TimeUnit.SECONDS.toMillis(timestamp);
		}

		/**
		 * {@inheritDoc}
		 * @see java.lang.Comparable#compareTo(java.lang.Object)
		 */
		@Override
		public int compareTo(final Delayed o) {			
			return timestamp < o.getDelay(TimeUnit.SECONDS) ? -1 : 1;
		}

		/**
		 * {@inheritDoc}
		 * @see java.util.concurrent.Delayed#getDelay(java.util.concurrent.TimeUnit)
		 */
		@Override
		public long getDelay(final TimeUnit unit) {
			return TimeUnit.MILLISECONDS.convert(timestampMs - System.currentTimeMillis(), unit);
		}
		
	}

}
