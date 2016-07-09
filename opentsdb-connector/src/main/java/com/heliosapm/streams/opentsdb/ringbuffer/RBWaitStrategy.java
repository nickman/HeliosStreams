package com.heliosapm.streams.opentsdb.ringbuffer;

import java.util.EnumSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.heliosapm.utils.config.ConfigurationHelper;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.LiteBlockingWaitStrategy;
import com.lmax.disruptor.PhasedBackoffWaitStrategy;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;


/**
 * <p>Title: RBWaitStrategy</p>
 * <p>Description: Functional enum for creating configurable {@link WaitStrategy} instances from config</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.tsdbex.plugins.reactor.RBWaitStrategy</code></p>
 */
public enum RBWaitStrategy implements WaitStrategyFactory {
//	/** A pair of slow and fast wait strategies to dynamically adapt to a given application load */
//	AGILE(new AgileWaitingFactory()),
	/** Blocking strategy that uses a lock and condition variable for EventProcessors waiting on a barrier. This strategy can be used when throughput and low-latency are not as important as CPU resource. */
	BLOCK(new BlockingWaitStrategy()),
	/** Busy Spin strategy that uses a busy spin loop for EventProcessors waiting on a barrier. This strategy will use CPU resource to avoid syscalls which can introduce latency jitter. It is best used when threads can be bound to specific CPU cores. */
	BUSY(new BusySpinWaitStrategy()),
	/** Variation of the BlockingWaitStrategy that attempts to elide conditional wake-ups when the lock is uncontended.   */
	LITE(new LiteBlockingWaitStrategy()),
//	/** Wait strategy that uses {@link java.util.concurrent.locks.LockSupport#parkNanos(long)} while waiting for a slot */
//	PARK(new ParkWaitFactory()),
	/** This strategy can be used when throughput and low-latency are not as important as CPU resource. Spins, then yields, then waits using the configured fallback WaitStrategy. */
	PHASED(new PhasedWaitingFactory()),
	/** Sleeping strategy that initially spins, then uses a Thread.yield(), and eventually for the minimum number of nanos the OS and JVM will allow while the EventProcessors are waiting on a barrier. This strategy is a good compromise between performance and CPU resource. Latency spikes can occur after quiet periods. */
	SLEEP(new SleepWaitFactory()),
	/** Yielding strategy that uses a Thread.yield() for EventProcessors waiting on a barrier after an initially spinning. This strategy is a good compromise between performance and CPU resource without incurring significant latency spikes. */
	YIELD(new com.lmax.disruptor.YieldingWaitStrategy()),
	/** Wait strategy that blocks with a timeout while waiting for a slot */
	TIMEOUT(new TimeoutWaitingFactory());
	
	private static final ThreadLocal<Set<RBWaitStrategy>> loopback = new ThreadLocal<Set<RBWaitStrategy>>() {
		@Override
		protected Set<RBWaitStrategy> initialValue() {			
			return EnumSet.noneOf(RBWaitStrategy.class);
		}
	};

	private RBWaitStrategy(final WaitStrategyFactory factory) {
		this.factory = factory;
		this.strategy = null;
	}
	
	private RBWaitStrategy(final WaitStrategy strategy) {
		this.factory = null;
		this.strategy = strategy;
	}
	
	
	private final WaitStrategyFactory factory;
	private final WaitStrategy strategy;
	
	/**
	 * Returns the configured wait strategy
	 * @param properties The optional config properties
	 * @return the configured wait strategy
	 */
	public static WaitStrategy getConfiguredStrategy(final Properties properties) {
		return ConfigurationHelper.getEnumUpperSystemThenEnvProperty(RBWaitStrategy.class, CONF_WAIT_STRAT, DEFAULT_WAIT_STRAT, properties).waitStrategy(properties);
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.ringbuffer.WaitStrategyFactory#waitStrategy(java.util.Properties)
	 */
	@Override
	public WaitStrategy waitStrategy(final Properties properties) {
		try {
			return strategy==null ? factory.waitStrategy(properties) : strategy;
		} finally {
			loopback.remove();
		}
	}
	

	/**
	 * Decodes the passed name to the corresponding RBWaitStrategy, trimming and upper-casing the passed value
	 * @param name The name to decode
	 * @return the decoded RBWaitStrategy
	 */
	public static RBWaitStrategy forName(final String name) {
		if(name==null || name.trim().isEmpty()) throw new IllegalArgumentException("The passed RBWaitStrategy was null or empty");
		final String _name = name.toUpperCase().trim();
		try {
			return valueOf(_name);
		} catch (Exception ex) {
			throw new IllegalArgumentException("Invalid RBWaitStrategy name: [" + _name + "]");
		}
	}
	
	/**
	 * Decodes the passed name to the corresponding RBWaitStrategy, trimming and upper-casing the passed value.
	 * If the name cannot be decoded, returns the default, {@link #BLOCK}.
	 * @param name The name to decode
	 * @return the decoded RBWaitStrategy
	 */
	public static RBWaitStrategy forNameOrDefault(final String name) {
		try {
			return forName(name);
		} catch (Exception ex) {
			return BLOCK;
		}		
	}
	

	static class PhasedWaitingFactory implements WaitStrategyFactory {
		@Override
		public WaitStrategy waitStrategy(final Properties properties) {
			if(!loopback.get().add(PHASED)) {
				throw new IllegalArgumentException("Recursive condition detected for PhasedWaitingFactory", new Throwable());
			}			
			final long spin = ConfigurationHelper.getLongSystemThenEnvProperty(CONF_PHASED_SPIN, DEFAULT_PHASED_SPIN, properties);
			final long yield = ConfigurationHelper.getLongSystemThenEnvProperty(CONF_PHASED_YIELD, DEFAULT_PHASED_YIELD, properties);
			final TimeUnit unit = ConfigurationHelper.getEnumUpperSystemThenEnvProperty(TimeUnit.class, CONF_PHASED_UNIT, DEFAULT_PHASED_UNIT, properties);
			final WaitStrategy fallback = ConfigurationHelper.getEnumUpperSystemThenEnvProperty(RBWaitStrategy.class, CONF_AGILE_SLOW, DEFAULT_AGILE_SLOW, properties).waitStrategy(properties);
			return new PhasedBackoffWaitStrategy(spin, yield, unit, fallback);
		}
	}
	
	
	
	static class TimeoutWaitingFactory implements WaitStrategyFactory {
		@Override
		public WaitStrategy waitStrategy(final Properties properties) {
			final TimeUnit unit = ConfigurationHelper.getEnumUpperSystemThenEnvProperty(TimeUnit.class, CONF_TIMEOUT_UNIT, DEFAULT_TIMEOUT_UNIT, properties);
			final long timeout = ConfigurationHelper.getLongSystemThenEnvProperty(CONF_TIMEOUT_TIME, DEFAULT_TIMEOUT_TIME, properties);			
			return new TimeoutBlockingWaitStrategy(timeout, unit);
		}
	}
	
	
	static class SleepWaitFactory implements WaitStrategyFactory {
		@Override
		public WaitStrategy waitStrategy(final Properties properties) {
			final int retries = ConfigurationHelper.getIntSystemThenEnvProperty(CONF_SLEEP_RETRIES, DEFAULT_SLEEP_RETRIES, properties);
			return new SleepingWaitStrategy(retries);
		}
	}

	
}