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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * <p>Title: WindowAggregation</p>
 * <p>Description: Aggregation construct to accumulate stream events ("off-line") for a specified window of time.</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.router.nodes.WindowAggregation</code></p>
 * @param <K> The aggregation type's key
 * @param <V> The aggregation type's value
 */

public class WindowAggregation<K, V extends Aggregatable<V>> implements Runnable {
	
	 
	
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
	/** The idle key retention time */
	private final long idleRetention;
	/** Indicates if idle key retention is enabled */
	private final boolean idleRetentionEnabled;
	/** The idle keys map */
	private final Cache<K, Long> idleKeys;
	/** The executor in which completed aggregations are returned */
	private final Executor executor;
	/** Instance logger */
	private final Logger log;
	
	@SuppressWarnings("unchecked")
	private final V placeholder = (V) new Aggregatable<V>() {
		@Override
		public long timestamp(TimeUnit unit) {
			return 0;
		}

		@Override
		public void aggregateInto(final Aggregatable<V> from) {
		}

		@Override
		public V newInstance(Aggregatable<V> from) {			
			return null;
		}
	};
	
	/**
	 * Creates a new WindowAggregation
	 * @param windowDuration The length (time) of the aggregaton window in seconds
	 * @param idleRetention An optional period of time in seconds for which idle keys are retained and made available.
	 * If the retention is less than 1, no keys are retained.
	 */
	public WindowAggregation(final long windowDuration, final Executor executor, final long idleRetention) {
		if(executor==null) throw new IllegalArgumentException("The passed executor was null");
		this.windowDuration = windowDuration;
		this.idleRetention = idleRetention;
		this.executor = executor;
		idleRetentionEnabled = idleRetention<1L;
		idleKeys = idleRetentionEnabled ? CacheBuilder.newBuilder()
			.expireAfterWrite(idleRetention, TimeUnit.SECONDS)
			.initialCapacity(1024)
			.build() : null;
		log = LogManager.getLogger(getClass().getName() + "-" + windowDuration);
	}
	
	/**
	 * Returns the idle keys
	 * @return the idle keys
	 */
	public Set<K> getIdleKeys() {
		if(!idleRetentionEnabled) throw new IllegalStateException("Idle Key Retention not enabled for this window");
		return new HashSet<K>(idleKeys.asMap().keySet());
	}
	
	/**
	 * Aggregates the passed key/value pair 
	 * @param key The key
	 * @param value The value to aggregate
	 * @return true if the pair was aggregated, false the timestamp of the passed value was too late to be aggregated
	 */
	public boolean aggregate(final K key, final V value) {
		if(!running.get()) throw new IllegalStateException("This WindowAggregation was stopped");
		if(idleRetentionEnabled) idleKeys.invalidate(key);
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
				final NonBlockingHashMap<K, V> period = delayIndex.remove(td.timestamp);
				if(period!=null) {
					
				} else {
					log.warn("Missing delay index for window [{}]",  td.timestamp);
				}
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

	class TimestampDelay implements Delayed {
		/** The timestamp representing the start of a time window in seconds */
		private final long timestamp;
		/** The timestamp in ms. */
		private final long timestampMs;
		/** The time this delay expired */
		private final long expireTime;
		
		/**
		 * Creates a new TimestampDelay
		 * @param timestamp The timestamp representing the start of a time window in seconds
		 */
		public TimestampDelay(final long timestamp) {
			this.timestamp = timestamp;
			this.timestampMs = TimeUnit.SECONDS.toMillis(timestamp);
			this.expireTime = System.currentTimeMillis() + this.timestampMs;
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
