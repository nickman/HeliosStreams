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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.cliffc.high_scale_lib.NonBlockingHashSet;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.jmx.export.naming.SelfNaming;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.heliosapm.utils.config.ConfigurationHelper;
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
@ManagedResource
public class WindowAggregation<K, V extends Aggregator<T>, T> implements Runnable, Supplier<Set<KeyValue<K,T>>>, SelfNaming, DisposableBean {
	
	private static final int CORES = Runtime.getRuntime().availableProcessors();
	/** Unique WindowAggregation instances keyed by idle retention within window duration */
	private static final NonBlockingHashMap<AggregatorWindowKey, WindowAggregation> instances = new NonBlockingHashMap<AggregatorWindowKey, WindowAggregation>();
	/** The shared executor for invoking expiration callbacks */
	private static final ThreadPoolExecutor executor = JMXManagedThreadPool.builder()
			.corePoolSize(CORES)
			.keepAliveTimeMs(60000)
			.maxPoolSize(CORES*2)
			.objectName(JMXHelper.objectName("com.heliosapm.streams.metrics.router:service=WindowAggregationThreadPool"))
			.poolName("WindowAggregationThreadPool")
			.prestart(CORES)
			.queueSize(ConfigurationHelper.getIntSystemThenEnvProperty("streams.windowaggregation.threadpool.queuesize", 128))
			.build();
	 
	
	/** The length of the window in seconds */
	private final long windowDuration;
	/** The JMX ObjectName for this window */
	private final ObjectName objectName;
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
	/** The aggregator */
	private final Aggregator<T> aggregator;
	/** The idle key retention time */
	private final long idleRetention;
	/** Indicates if idle key retention is enabled */
	private final boolean idleRetentionEnabled;
	/** The idle keys map */
	private final Cache<K, Long> idleKeys;
	/** Instance logger */
	private final Logger log;
	/** The key for this window */
	private final AggregatorWindowKey<V,T> key;
	/** Flag indicating if this window resets on each period expiration */
	private final boolean resetting;
	/** The output stream */
	Stream<Set<KeyValue<K,T>>> outStream = Stream.generate(this);
	/** Flag indicating if the outStream has been requested */
	private final AtomicBoolean outStreamRequested = new AtomicBoolean(false); 
	/** A queue for outbound expirations */
	private final ArrayBlockingQueue<Set<KeyValue<K,T>>> expirationQueue = new ArrayBlockingQueue<Set<KeyValue<K,T>>>(1024, false); 
	
	/** An empty set of idle keys for callbacks when retention is disabled */
	private final Set<K> emptyIdleKeys = Collections.unmodifiableSet(Collections.emptySet());
	/** The run thread for this window */
	private final Thread runThread;
	/** A placeholder for inserting new periods into the delay index */
	private final NonBlockingHashMap<K, T> PLACEHOLDER = new NonBlockingHashMap<K, T>();
	/** A count of created periods */
	private final LongAdder newPeriodCount = new LongAdder();
	/** A count of low level aggregation executions */
	private final LongAdder aggregationCount = new LongAdder();
	/** A count of late arrival discards */
	private final LongAdder discardCount = new LongAdder();
	
//	private final SpinLock lock = UnsafeAdapter.allocateSpinLock();
	
	
	/**
	 * Acquires a WindowAggregation for the specified duration and idle key retention
	 * @param windowDuration The length (time) of the aggregaton window in seconds
	 * @param idleRetention An optional period of time in seconds for which idle keys are retained and made available.
	 * @param aggregator The aggregator instance 
	 * @return the WindowAggregation
	 * @param <K> The aggregation type's key
	 * @param <V> The aggregation aggregator type
	 * @param <T> The aggregation type's value
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static <K,V extends Aggregator<T>, T> WindowAggregation<K,V,T> getInstance(final long windowDuration, final long idleRetention, final boolean resetting, final V aggregator) {
		final long _idleRetention = idleRetention<1L ? 0L : idleRetention;
		if(aggregator==null) throw new IllegalArgumentException("The passed aggregator was null");
		final AggregatorWindowKey key = new AggregatorWindowKey<V,T>(windowDuration, _idleRetention, resetting, (Class<V>) aggregator.getClass());
		return instances.get(key, new Callable<WindowAggregation>() {
			@Override
			public WindowAggregation<K,V,T> call() throws Exception {				
				return new WindowAggregation<K,V,T>(windowDuration, _idleRetention, resetting, aggregator, key);
			}
		});
	}
	
	/**
	 * Creates a new WindowAggregation
	 * @param windowDuration The length (time) of the aggregaton window in seconds
	 * @param idleRetention An optional period of time in seconds for which idle keys are retained and made available.
	 * If the retention is less than 1, no keys are retained.
	 * @param resetting indicates if this window resets on each period expiration
	 * @param aggregator The aggregator
	 * @param key this window's key
	 */
	private WindowAggregation(final long windowDuration, final long idleRetention, final boolean resetting, final Aggregator<T> aggregator, final AggregatorWindowKey<V,T> key) {
		this.windowDuration = windowDuration;
		this.idleRetention = idleRetention;
		this.key = key;
		objectName = key.toObjectName();
		this.resetting = resetting;
		idleRetentionEnabled = idleRetention<1L;
		idleKeys = idleRetentionEnabled ? CacheBuilder.newBuilder()
			.expireAfterWrite(idleRetention, TimeUnit.SECONDS)
			.initialCapacity(1024)
			.build() : null;
		log = LogManager.getLogger(getClass().getName() + "-" + windowDuration + aggregator.getClass().getSimpleName());
		this.aggregator = aggregator;
		runThread = new Thread(this, "WindowAggregationThread[" + aggregator.getClass().getSimpleName() + ", " + windowDuration + "," + idleRetention + "]");
		runThread.setDaemon(true);
		runThread.start();		
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see org.springframework.beans.factory.DisposableBean#destroy()
	 */
	@Override
	public void destroy() throws Exception {
		close();		
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.springframework.jmx.export.naming.SelfNaming#getObjectName()
	 */
	@Override
	public ObjectName getObjectName() throws MalformedObjectNameException {
		return objectName;
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
	 * {@inheritDoc}
	 * @see java.util.function.Supplier#get()
	 */
	@Override
	public Set<KeyValue<K, T>> get() {
		try {
			return expirationQueue.take();
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
	
	/**
	 * Returns the idle keys
	 * @return the idle keys
	 */
	public Set<K> getIdleKeys() {
		if(!idleRetentionEnabled) throw new IllegalStateException("Idle Key Retention not enabled for this window");
		return new HashSet<K>(idleKeys.asMap().keySet());
	}
	
//	final Collector<KeyValue<K,T>, ?, Collection<ProducerRecord<K,T>>> kvToPr(final String topic) {
//		return 
//		Collector.of(ArrayList::new, ArrayList::add, (kv) -> {
//			return new ProducerRecord(topic, kv.key, kv.value);
//		});
//		
//	}
//	

	public void aggregateAndPublish(final KStream<K,T> in, final Producer<K,T> kafkaProducer, final String topic) {
		in.foreach((k,t) -> aggregate(k,t));		
		addAction(new WindowAggregationAction<K, T>() {
			@Override
			public void onExpire(final Stream<KeyValue<K, T>> aggregatedStream, final Set<K> expiredKeys) {
				aggregatedStream.map(kv -> new ProducerRecord<K,T>(topic, kv.key, kv.value))
					.map(pr -> kafkaProducer.send(pr));
						
			}
		});
	}
	
	public Stream<KeyValue<K,T>> stream(final KStreamBuilder builder, final Serde<K> keySerde, final Serde<T> valueSerde, final String...fromTopics) {
		builder.stream(keySerde, valueSerde, fromTopics)
			.foreach((k,t) -> aggregate(k,t));
		return outStream().flatMap(setOfKt -> setOfKt.stream());
	}
	
	
	public Stream<Set<KeyValue<K,T>>> outStream() {
		outStreamRequested.set(true);
		return outStream;
	}
	
	
	
	/**
	 * Aggregates the passed key/value pair 
	 * @param key The key
	 * @param value The value to aggregate
	 * @return true if the pair was aggregated, false the timestamp of the passed value was too late to be aggregated
	 */
	public boolean aggregate(final K key, final T value) {
		if(!running.get()) throw new IllegalStateException("This WindowAggregation was stopped");
		if(idleRetentionEnabled) idleKeys.invalidate(key);
		final long ts = windowStartTime(aggregator.timestamp(TimeUnit.SECONDS, value));
		if(!earliestTimestamp.compareAndSet(0L, ts)) {
			if(ts < earliestTimestamp.get()) {
				discardCount.increment();
				return false;
			}
		}
		NonBlockingHashMap<K, T> aggrMap = delayIndex.putIfAbsent(ts, PLACEHOLDER);
		if(aggrMap==null || aggrMap==PLACEHOLDER) {
			delayQueue.put(new TimestampDelay(ts));
			aggrMap = new NonBlockingHashMap<K, T>();
			delayIndex.replace(ts, aggrMap);
			newPeriodCount.increment();
		}
//		try {
//			lock.xlock(true);
//			aggrMap = delayIndex.get(ts, new Callable<NonBlockingHashMap<K, T>>() {
//				@Override
//				public NonBlockingHashMap<K, T> call() throws Exception {
//					delayQueue.put(new TimestampDelay(ts));
//					return new NonBlockingHashMap<K, T>();
//				}
//			});
//		} finally {
//			lock.xunlock();
//		}
		final T prior = aggrMap.put(key, value);
		if(prior!=null) {
			aggregator.aggregateInto(value, prior);
			aggregationCount.increment();
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
			= WindowAggregation.getInstance(10L, 0L, true, AggregatingMetricCounter.AGGREGATOR);
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
		instances.remove(key);
	}
	
	
	/**
	 * <p>Runs the expiration</p>
	 * {@inheritDoc}
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		log.info("Expiration thread started");
		while(running.get()) {
			try {
				final TimestampDelay td;
				final NonBlockingHashMap<K, T> period;
				try {
//					lock.xlock();
					td = delayQueue.take();
					// ========================================================================
					// TODO: implement non-resetting
					// tricky since we will need to block incoming 
					// while we make defensive copies of Ts in NonBlockingHashMap<K, T> period 
					// ========================================================================
					period = delayIndex.remove(td.key);
				} finally {
//					lock.xunlock();
				}
//				if(actions.isEmpty()) {
//					log.warn("No actions registered !");
//					continue;
//				}
				if(period!=null) {
					//log.info("Processing Aggregation Period: [{}]", new Date(td.timestampMs));
					log.debug("Processing Aggregation Period: [{}]", td.key);
					executor.execute(new Runnable(){
						@Override
						public void run() {
							final Set<KeyValue<K,T>> set = new HashSet<KeyValue<K,T>>(period.size());
							for(Map.Entry<K, T> entry: period.entrySet()) {
								set.add(new KeyValue<K,T>(entry.getKey(), entry.getValue()));
							}
							final Set<K> idle = idleRetentionEnabled ? getIdleKeys() : emptyIdleKeys;
							if(outStreamRequested.get()) {
								if(!expirationQueue.offer(new HashSet<KeyValue<K,T>>(set))) {
									log.warn("ExpirationQueue Full !!!");
								}
							}
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
		
		
		
		
		private TimestampDelay() {
			this.key = -1L;
			this.timestampMs = -1L;
			this.expireTime = -1L;
			
		}
		
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

		/**
		 * {@inheritDoc}
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + (int) (key ^ (key >>> 32));
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
			TimestampDelay other = (TimestampDelay) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (key != other.key)
				return false;
			return true;
		}

		private WindowAggregation getOuterType() {
			return WindowAggregation.this;
		}
		
		
		
	}

	/**
	 * Returns the window duration in seconds
	 * @return the window duration in seconds
	 */
	@ManagedAttribute(description="The window duration in seconds")
	public long getWindowDuration() {
		return windowDuration;
	}

	/**
	 * Indicates if this window is running or stopped
	 * @return true if running, false if stopped
	 */
	@ManagedAttribute(description="Indicates if this window is running or stopped")
	public boolean getRunning() {
		return running.get();
	}

	/**
	 * Returns the idle retention time in ms.
	 * @return the idle retention time
	 */
	@ManagedAttribute(description="The idle key retention time in ms")
	public long getIdleRetention() {
		return idleRetention;
	}

	/**
	 * Indicates if idle key retention is enabled
	 * @return true if idle key retention is enabled, false otherwise
	 */
	@ManagedAttribute(description="Indicates if idle key retention is enabled")
	public boolean isIdleRetentionEnabled() {
		return idleRetentionEnabled;
	}

	/**
	 * Returns the earliest timestamp
	 * @return the earliest timestamp
	 */
	@ManagedAttribute(description="The earliest timestamp")
	public long getEarliestTimestamp() {
		return earliestTimestamp.get();
	}
	
	/**
	 * Returns the earliest timestamp as a date
	 * @return the earliest timestamp as a date
	 */
	@ManagedAttribute(description="The earliest timestamp as a date")
	public Date getEarliestDate() {
		return new Date(earliestTimestamp.get());
	}
	

	/**
	 * Returns the number of currently aggregating periods
	 * @return the number of currently aggregating periods
	 */
	@ManagedAttribute(description="The total number of periods being aggregated")
	public int getDelayIndex() {
		return delayIndex.size();
	}
	
	
	/**
	 * Returns the number of late arrival discards
	 * @return the number of late arrival discards
	 */
	@ManagedAttribute(description="The number of late arrival discards")
	public long getDiscardCount() {
		return discardCount.longValue();
	}
	
	/**
	 * Returns the total number of keys being aggregated
	 * @return the total number of keys being aggregated
	 */
	@ManagedAttribute(description="The total number of keys being aggregated")
	public int getTotalKeyCount() {
		return delayIndex.values().stream().mapToInt(p -> p.size()).sum();
	}

	/**
	 * Returns the number of registered actions
	 * @return the number of registered actions
	 */
	@ManagedAttribute(description="The number of registered actions")
	public int getActionCount() {
		return actions.size();
	}

	/**
	 * Returns the aggregator class name
	 * @return the aggregator class name
	 */
	@ManagedAttribute(description="The aggregator class name")
	public String getAggregator() {
		return aggregator.getClass().getName();
	}

	/**
	 * Returns the window key
	 * @return the window key
	 */
	@ManagedAttribute(description="The aggregation window key")
	public String getKey() {
		return key.toString();
	}

	/**
	 * Indicates if the time periods in this window reset when expired
	 * @return true if the time periods in this window reset when expired, false otherwise
	 */
	@ManagedAttribute(description="Indicates if the time periods in this window reset when expired")
	public boolean isResetting() {
		return resetting;
	}

	/**
	 * Returns the size of the expiration queue
	 * @return the size of the expiration queue
	 */
	@ManagedAttribute(description="The size of the expiration queue")
	public int getExpirationQueueDepth() {
		return expirationQueue.size();
	}

	/**
	 * Returns the total number of created periods
	 * @return the total number of created periods
	 */
	@ManagedAttribute(description="The total number of created periods")
	public long getNewPeriodCount() {
		return newPeriodCount.longValue();
	}

	/**
	 * Returns the total number of aggregation executions
	 * @return the total number of aggregation executions
	 */
	@ManagedAttribute(description="The total number of aggregation executions")
	public long getAggregationCount() {
		return aggregationCount.longValue();
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

