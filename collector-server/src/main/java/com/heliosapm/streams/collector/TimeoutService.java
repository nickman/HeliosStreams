/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.heliosapm.streams.collector;

import java.util.Set;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.jmx.JMXHelper;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;

/**
 * <p>Title: TimeoutService</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.TimeoutService</code></p>
 */

public class TimeoutService implements TimeoutServiceMBean, ThreadFactory, Timer {
	/** The singleton instance */
	private static volatile TimeoutService instance;
	/** The singleton instance ctor lock */
	private static final Object lock = new Object();
	
	/** The config key for the tick duration in ms. */
	public static final String CONFIG_TICK_DURATION = "timeoutservice.tick.duration";
	/** The default tick duration in ms. */
	public static final long DEFAULT_TICK_DURATION = 100;
	/** The config key for the ticks per wheel */
	public static final String CONFIG_TICK_COUNT = "timeoutservice.tick.count";
	/** The default ticks per wheel */
	public static final int DEFAULT_TICK_COUNT = 512;

	/** The created thread serial factory */
	private final AtomicInteger serial = new AtomicInteger(0);
	/** Instance logger */
	private final Logger log = LogManager.getLogger(getClass());
	/** The timer */
	private final HashedWheelTimer timer;
	/** The timer tick duration in ms. */
	private final long tickDuration;
	/** The timer ticks per wheel */
	private final int tickCount;
	/** A gauge of pending timeouts */
	protected final AtomicInteger pendingTimeouts = new AtomicInteger();
	/** A counter of timeouts */
	protected final LongAdder timeouts = new LongAdder();
	/** A counter of on time cancellations */
	protected final LongAdder cancellations = new LongAdder();
	/** The timeout thread's thread group */
	protected final ThreadGroup timeoutThreadGroup = new ThreadGroup("HeliosTimeoutService");
			
			
	/**
	 * Acquires and returns the TimeoutService singleton instance
	 * @return the TimeoutService singleton instance
	 */
	public static TimeoutService getInstance() {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					instance = new TimeoutService();
				}
			}
		}
		return instance;
	}
	
	
	private TimeoutService() {
		tickDuration = ConfigurationHelper.getLongSystemThenEnvProperty(CONFIG_TICK_DURATION, DEFAULT_TICK_DURATION);
		tickCount = ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_TICK_COUNT, DEFAULT_TICK_COUNT);
		timer = new HashedWheelTimer(this, tickDuration, TimeUnit.MILLISECONDS, tickCount);
		timer.start();
		JMXHelper.registerMBean(this, OBJECT_NAME);
		log.info("TimeoutService started");
	}


	/**
	 * {@inheritDoc}
	 * @see java.util.concurrent.ThreadFactory#newThread(java.lang.Runnable)
	 */
	@Override
	public Thread newThread(final Runnable r) {
		final Thread t = new Thread(timeoutThreadGroup, r, "HeliosTimeoutService#" + serial.incrementAndGet());
		t.setDaemon(true);
		t.setPriority(Thread.MAX_PRIORITY);
		return t;
	}
	
	private class WrappedTimeout implements Timeout {
		private final Timeout timeout;

		/**
		 * Creates a new WrappedTimeout
		 * @param timeout the timeout to wrap
		 */
		public WrappedTimeout(final Timeout timeout) {
			this.timeout = timeout;
			pendingTimeouts.incrementAndGet();
		}

		/**
		 * Returns the hashedwheel timer that created this timeout
		 * @return this timeout service
		 * @see io.netty.util.Timeout#timer()
		 */
		@Override
		public Timer timer() {
			return instance;
		}

		/**
		 * Returns the task this timeout will run
		 * @return the task this timeout will run
		 * @see io.netty.util.Timeout#task()
		 */
		@Override
		public TimerTask task() {
			return timeout.task();
		}

		/**
		 * Indicates if this task is expired
		 * @return true if this task is expired, false otherwise
		 * @see io.netty.util.Timeout#isExpired()
		 */
		@Override
		public boolean isExpired() {
			return timeout.isExpired();
		}

		/**
		 * Indicates if this task is cancelled
		 * @return true if this task is cancelled, false otherwise
		 * @see io.netty.util.Timeout#isCancelled()
		 */
		@Override
		public boolean isCancelled() {
			return timeout.isCancelled();
		}

		/**
		 * Cancels this task
		 * @return true if the task was cancelled, false if the task was cancelled or already executed
		 * @see io.netty.util.Timeout#cancel()
		 */
		@Override
		public boolean cancel() {
			final boolean cancelled = timeout.cancel();
			if(cancelled) {
				pendingTimeouts.decrementAndGet();
				cancellations.increment();
			}
			return timeout.cancel();
		}
		
		
	}
	
	
	/**
	 * Schedules a task to run on the timeout period
	 * @param delay the timeout period
	 * @param unit the timeout unit
	 * @param onTimeout the task to execute if the timer expires
	 * @return the timeout handle
	 */
	public Timeout timeout(final long delay, final TimeUnit unit, final Runnable onTimeout) {
		return new WrappedTimeout(timer.newTimeout(new TimerTask(){
			@Override
			public void run(final Timeout timeout) throws Exception {
				try {
					onTimeout.run();
				} finally {
					pendingTimeouts.decrementAndGet();
					timeouts.increment();
				}				
			}			
		}, delay, unit));
	}
	
	/**
	 * Schedules a task to run on the timeout period in ms.
	 * @param delay the timeout period
	 * @param onTimeout the task to execute if the timer expires
	 * @return the timeout handle
	 */
	public Timeout timeout(final long delay, final Runnable onTimeout) {
		return timeout(delay, TimeUnit.MILLISECONDS, onTimeout);
	}
	


	/**
	 * {@inheritDoc}
	 * @see io.netty.util.Timer#newTimeout(io.netty.util.TimerTask, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public Timeout newTimeout(final TimerTask task, final long delay, final TimeUnit unit) {
		final WrappedTimeout[] t = new WrappedTimeout[1];
		t[0] = new WrappedTimeout(timer.newTimeout(new TimerTask(){
			@Override
			public void run(final Timeout timeout) throws Exception {
				try {
					task.run(t[0]);
				} finally {
					pendingTimeouts.decrementAndGet();
					timeouts.increment();
				}				
			}			
		}, delay, unit));
		return t[0];
	}


	/**
	 * <p>Overriden to no op</p>
	 * {@inheritDoc}
	 * @see io.netty.util.Timer#stop()
	 */
	@Override
	public Set<Timeout> stop() {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.TimeoutServiceMBean#getCancellations()
	 */
	@Override
	public long getCancellations() {
		return cancellations.longValue();
	}
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.TimeoutServiceMBean#getTimeouts()
	 */
	@Override
	public long getTimeouts() {
		return timeouts.longValue();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.TimeoutServiceMBean#getActiveThreads()
	 */
	@Override
	public int getActiveThreads() {
		return timeoutThreadGroup.activeCount();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.TimeoutServiceMBean#getPending()
	 */
	@Override
	public int getPending() {
		return pendingTimeouts.get();
	}
	

}
