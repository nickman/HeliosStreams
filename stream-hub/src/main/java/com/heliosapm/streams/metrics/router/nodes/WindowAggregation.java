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
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.apache.kafka.streams.KeyValue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.cliffc.high_scale_lib.NonBlockingHashSet;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.jmx.JMXManagedThreadPool;
import com.heliosapm.utils.time.SystemClock;

/**
 * <p>Title: WindowAggregation</p>
 * <p>Description: Aggregation construct to accumulate stream events ("off-line") for a specified window of time.</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.router.nodes.WindowAggregation</code></p>
 * @param <K> The aggregation type's key
 * @param <V> The aggregation aggregator type
 * @param <T> The aggregation type's value
 */

public class WindowAggregation<K, V extends Aggregatable<T>, T> implements Runnable {
	
	private static final int CORES = Runtime.getRuntime().availableProcessors();
	/** Unique WindowAggregation instances keyed by idle retention within window duration */
	private static final NonBlockingHashMapLong<NonBlockingHashMapLong<WindowAggregation>> instances = new NonBlockingHashMapLong<NonBlockingHashMapLong<WindowAggregation>>();
	/** The shared executor for invoking expiration callbacks */
	private static final ThreadPoolExecutor executor = JMXManagedThreadPool.builder()
			.corePoolSize(CORES)
			.keepAliveTimeMs(60000)
			.maxPoolSize(CORES*2)
			.objectName(JMXHelper.objectName("com.heliosapm.streams.metrics.router:service=WindowAggregationThreadPool"))
			.poolName("WindowAggregationThreadPool")
			.prestart(CORES)
			.queueSize(1)
			.build();
	 
	
	/** The length of the window in seconds */
	private final long windowDuration;	
	/** The earliest timestamp accepted by this window, which is initialized to the first submission and 
	 * updated to the end of expiring windows (plus one) as they expire 
	 */
	private final AtomicLong earliestTimestamp = new AtomicLong(0L);
	/** A map of aggregation maps keyed by the start time of the time window being aggregated */
	private final NonBlockingHashMapLong<NonBlockingHashMap<K, T>> delayIndex = new NonBlockingHashMapLong<NonBlockingHashMap<K, T>>();
	/** The delay queue which supplies expiring aggregation maps as their time window expires */
	private final DelayQueue<TimestampDelay> delayQueue = new DelayQueue<TimestampDelay>();
	/** The keep running flag */
	private final AtomicBoolean running = new AtomicBoolean(true);
	/** The caller supplied expiration callback handler */
	private final NonBlockingHashSet<WindowAggregationAction<K, T>> actions = new NonBlockingHashSet<WindowAggregationAction<K, T>>();
//	/** The outbound stream of expired aggregations */
//	private final Stream<KeyValue<K,V>> outbound = Stream.generate(this);
//	/** The internal stream of expired aggregations */
//	private final AtomicReference<Stream<Iterator<Map.Entry<K,V>>>> internalOutbound = new AtomicReference<Stream<Iterator<Map.Entry<K,V>>>>(Stream.empty());

	/** The idle key retention time */
	private final long idleRetention;
	/** Indicates if idle key retention is enabled */
	private final boolean idleRetentionEnabled;
	/** The idle keys map */
	private final Cache<K, Long> idleKeys;
	/** Instance logger */
	private final Logger log;
	
	/** An empty set of idle keys for callbacks when retention is disabled */
	private final Set<K> emptyIdleKeys = Collections.unmodifiableSet(Collections.emptySet());
	/** The run thread for this window */
	private final Thread runThread;
	
	
	
	
	/**
	 * Acquires a WindowAggregation for the specified duration and idle key retention
	 * @param windowDuration The length (time) of the aggregaton window in seconds
	 * @param idleRetention An optional period of time in seconds for which idle keys are retained and made available.
	 * @return the WindowAggregation
	 */
	@SuppressWarnings("rawtypes")
	public static <K,V extends Aggregatable<T>, T> WindowAggregation<K,V,T> getInstance(final long windowDuration, final long idleRetention) {
		final long _idleRetention = idleRetention<1L ? 0L : idleRetention;
		final NonBlockingHashMapLong<WindowAggregation> map = instances.get(_idleRetention, new Callable<NonBlockingHashMapLong<WindowAggregation>>() {
			@Override
			public NonBlockingHashMapLong<WindowAggregation> call() throws Exception {				
				return new NonBlockingHashMapLong<WindowAggregation>();
			}
		});
		return map.get(windowDuration, new Callable<WindowAggregation>() {
			@Override
			public WindowAggregation call() throws Exception {
				return new WindowAggregation(windowDuration, _idleRetention);
			}
		});
	}
	
	/**
	 * Creates a new WindowAggregation
	 * @param windowDuration The length (time) of the aggregaton window in seconds
	 * @param idleRetention An optional period of time in seconds for which idle keys are retained and made available.
	 * If the retention is less than 1, no keys are retained.
	 */
	private WindowAggregation(final long windowDuration, final long idleRetention) {
		this.windowDuration = windowDuration;
		this.idleRetention = idleRetention;
		idleRetentionEnabled = idleRetention<1L;
		idleKeys = idleRetentionEnabled ? CacheBuilder.newBuilder()
			.expireAfterWrite(idleRetention, TimeUnit.SECONDS)
			.initialCapacity(1024)
			.build() : null;
		log = LogManager.getLogger(getClass().getName() + "-" + windowDuration);
		runThread = new Thread(this, "WindowAggregationThread[" + windowDuration + "," + idleRetention + "]");
		runThread.setDaemon(true);
		runThread.start();		
	}
	
	/**
	 * Adds an action to handle expired window callbacks
	 * @param action The action to register
	 * @return this window aggregation
	 */
	public WindowAggregation<K,V,T> addAction(final WindowAggregationAction<K,T> action) {
		if(action!=null) {
			actions.add(action);
		}
		return this;
	}
	
	/**
	 * Removes a registered action
	 * @param action the action to remove
	 * @return this window aggregation
	 */
	public WindowAggregation<K,V, T> removeAction(final WindowAggregationAction<K, V> action) {
		if(action!=null) {
			actions.remove(action);
		}
		return this;
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
		final NonBlockingHashMap<K, T> aggrMap = delayIndex.get(ts, new Callable<NonBlockingHashMap<K, T>>() {
			@Override
			public NonBlockingHashMap<K, T> call() throws Exception {
				delayQueue.put(new TimestampDelay(ts));
				return new NonBlockingHashMap<K, T>();
			}
		});
		final StringBuilder b = new StringBuilder();
		final T t = value.get();
		b.append("in:").append(t).append(", prior:");
		final T prior = aggrMap.put(key, t);
		if(prior!=null) {
			b.append(prior).append(", acc:");
			value.aggregateInto(prior);
			b.append(t);
		}
		if("metric.0".equals(key)) {
			log(b);
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
	
	public static void log(Object msg) {
		System.out.println("[" + Thread.currentThread() + "]" + msg);
	}
	
	public static void main(String[] args) {
		
		final Random R = new Random(System.currentTimeMillis());
		final AtomicBoolean run = new AtomicBoolean(true);
		final WindowAggregationAction<String, AggregatingMetricCounter> action = new WindowAggregationAction<String, AggregatingMetricCounter>() {
			@Override
			public void onExpire(final Stream<KeyValue<String, AggregatingMetricCounter>> aggregatedStream, final Set<String> expiredKeys) {
				aggregatedStream					
					.forEach((m) -> log(m));
				if(!expiredKeys.isEmpty()) {
					log("Expired Keys:" + expiredKeys);
				}
				log("================================================");
			}
		};
		final WindowAggregation<String, AggregatingMetricCounter, AggregatingMetricCounter> wa 
			= WindowAggregation.getInstance(10L, 0L);
		wa.addAction(action);
		while(run.get()) {
			for(int x = 0; x < 12; x++) {
				final long ts = System.currentTimeMillis();
				final StringBuilder b = new StringBuilder("{");
				for(int i = 0; i < 5; i++) {
					final String key = "metric." + i;
					final int val = Math.abs(R.nextInt(100));
					b.append(val).append(",");
					wa.aggregate(key, new AggregatingMetricCounter(key, ts, val));
				}
				log(b.deleteCharAt(b.length()-1).append("}"));
				SystemClock.sleep(1000);
			}
			break;
			
		}
		
		
		StdInCommandHandler.getInstance().registerCommand("stop", new Runnable(){
			@Override
			public void run() {
				run.set(false);
				log("Loop Stopped");
				
			}
		}).run();
//		WindowAggregation wa = new WindowAggregation(10, -1L, null);
//		wa.test();
	}
	
	
	/**
	 * Closes this aggregation
	 */
	public void close() {
		actions.clear();
		running.set(false);
		runThread.interrupt();		
		instances.get(idleRetention).remove(windowDuration);
	}
	
	
	public void run() {
		log.info("Expiration thread started");
		while(running.get()) {
			try {
				final TimestampDelay td = delayQueue.take();
				final NonBlockingHashMap<K, T> period = delayIndex.remove(td.key);
				if(actions.isEmpty()) {
					log.warn("No actions registered !");
					continue;
				}
				if(period!=null) {
					executor.execute(new Runnable(){
						public void run() {
							final Set<KeyValue<K,T>> set = new HashSet<KeyValue<K,T>>(period.size());
							for(Map.Entry<K, T> entry: period.entrySet()) {
								set.add(new KeyValue<K,T>(entry.getKey(), entry.getValue()));
							}
							final Set<K> idle = idleRetentionEnabled ? getIdleKeys() : emptyIdleKeys;
							for(WindowAggregationAction<K, T> action: actions) {
								action.onExpire(new HashSet<KeyValue<K,T>>(set).stream(), new HashSet<K>(idle));
							}
							set.clear();
						}
					});
				} else {
					log.warn("Missing delay index for window [{}]",  td.key);
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
		private final long key;
		/** The timestamp in ms. */
		private final long timestampMs;
		/** The time this delay expired */
		private final long expireTime;
		
		/**
		 * Creates a new TimestampDelay
		 * @param windowKey The timestamp representing the start of a time window in seconds
		 */
		public TimestampDelay(final long windowKey) {
			this.key = windowKey;
			this.timestampMs = System.currentTimeMillis();
			this.expireTime = TimeUnit.SECONDS.toMillis(windowDuration) + this.timestampMs;
//			System.out.println("Expires at:" + new Date(expireTime));
		}

		/**
		 * {@inheritDoc}
		 * @see java.lang.Comparable#compareTo(java.lang.Object)
		 */
		@Override
		public int compareTo(final Delayed o) {			
			return expireTime < o.getDelay(TimeUnit.MILLISECONDS) ? -1 : 1;
		}

		/**
		 * {@inheritDoc}
		 * @see java.util.concurrent.Delayed#getDelay(java.util.concurrent.TimeUnit)
		 */
		@Override
		public long getDelay(final TimeUnit unit) {
			return expireTime - System.currentTimeMillis();
		}
		
		/**
		 * {@inheritDoc}
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return "TimestampDeay[window:" + windowDuration + ", now:" + new Date() + ", expires:" + new Date(expireTime) + "]";
		}
		
	}

	/**
	 * Returns the
	 * @return the windowDuration
	 */
	public long getWindowDuration() {
		return windowDuration;
	}

	/**
	 * Returns the
	 * @return the running
	 */
	public AtomicBoolean getRunning() {
		return running;
	}

	/**
	 * Returns the
	 * @return the idleRetention
	 */
	public long getIdleRetention() {
		return idleRetention;
	}

	/**
	 * Returns the
	 * @return the idleRetentionEnabled
	 */
	public boolean isIdleRetentionEnabled() {
		return idleRetentionEnabled;
	}

}


//private void test() {
//	executor.execute(new Runnable(){
//		public void run() {
//			final StringBuilder b = new StringBuilder("Queue:").append(delayQueue.size());
//			final List<TimestampDelay> delays = new ArrayList<TimestampDelay>(delayQueue);
//			if(!delays.isEmpty()) {
//				b.append("[");
//				for(TimestampDelay td: delays) {
//					b.append(td).append(",");
//				}
//				b.deleteCharAt(b.length()-1).append("]");
//			}
//			System.out.println("--------- " + b);
//			SystemClock.sleep(1000);
//			executor.execute(this);
//		}
//	});
//	
//	try {
//		for(int i = 0; i < 5; i++) {
//			SystemClock.sleep(500);
//			TimestampDelay td = new TimestampDelay(System.currentTimeMillis()/1000);
//			delayQueue.put(td);
//		}
//		for(int i = 0; i < 5; i++) {
//			TimestampDelay td = delayQueue.take();
//			System.out.println(td);
//		}
//		
//	} catch (Exception ex) {
//		ex.printStackTrace(System.err);
//		System.exit(-1);
//	}
//}

