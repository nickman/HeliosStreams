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
package com.heliosapm.streams.hystrix;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

import com.heliosapm.utils.lang.StringHelper;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommand.Setter;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandProperties;
import com.netflix.hystrix.HystrixCommandProperties.ExecutionIsolationStrategy;
import com.netflix.hystrix.HystrixThreadPoolKey;
import com.netflix.hystrix.HystrixThreadPoolProperties;

/**
 * <p>Title: HystrixCommandFactory</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.hystrix.HystrixCommandFactory</code></p>
 */

public class HystrixCommandFactory {
	/** The singleton instance */
	private static volatile HystrixCommandFactory instance = null;
	/** The singleton instance ctor lock */
	private static final Object lock = new Object();

	/** Property pattern for building keys */
	public static final Pattern TOKE_KEY_PATTERN = Pattern.compile("\\$\\$\\{(.*?)(?::(.*?))??\\}");
	
//	protected final Cache<>
	
	/**
	 * Acquires and returns the HystrixCommandFactory singleton instance
	 * @return the HystrixCommandFactory singleton instance
	 */
	public static HystrixCommandFactory getInstance() {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					instance = new HystrixCommandFactory();
				}
			}
		}
		return instance;
	}
	
	/**
	 * Creates the HystrixCommandFactory singleton
	 */
	private HystrixCommandFactory() {
	}
	
	/**
	 * Resolves all <b><code>${key:default}</code></b> tokens in the passed expression
	 * and returns the resolved value
	 * @param expression The expression to resolve
	 * @param tokens An optional map of additional token decodes
	 * @return the resolved string
	 */
	public static String resolve(final CharSequence expression, final Map<String, Object> tokens) {
		if(expression==null) throw new IllegalArgumentException("The passed expression was null");		
		final Properties p = new Properties();
		if(tokens!=null) p.putAll(tokens);
		return StringHelper.resolveTokens(expression, p);
	}
	
	/**
	 * Resolves all <b><code>${key:default}</code></b> tokens in the passed expression
	 * and returns the resolved value
	 * @param expression The expression to resolve
	 * @return the resolved string
	 */
	public static String resolve(final CharSequence expression) {
		return resolve(expression, null);
	}	

	/**
	 * <p>Title: HystrixCommandDefinition</p>
	 * <p>Description: </p> 
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>com.heliosapm.streams.hystrix.HystrixCommandFactory.HystrixCommandDefinition</code></p>
	 * @param <T> The expected type of the command return value
	 */
	public class HystrixCommandDefinition<T> {
		protected final HystrixCommand.Setter setter; 
		protected final Map<String, Object> tokens;
		protected final StringBuilder defKey = new StringBuilder();
		protected final HystrixCommandProperties.Setter commandPropertySetter = HystrixCommandProperties.defaultSetter();
		protected final HystrixThreadPoolProperties.Setter threadPoolPropertySetter = HystrixThreadPoolProperties.defaultSetter();
		
		/**
		 * Creates a new HystrixCommandDefinition
		 * @param groupKey the group key or group key expression
		 * @param tokens An optional map of tokens used to resolve expressions
		 */
		public HystrixCommandDefinition(final CharSequence groupKey, final Map<String, Object> tokens) {
			this.tokens = tokens==null ? Collections.emptyMap() : tokens;
			final String resolvedGroupKey = resolve(groupKey, tokens);
			defKey.append(resolvedGroupKey);
			setter = HystrixCommand.Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey(resolvedGroupKey));
		}
		
		/**
		 * Creates a new HystrixCommandDefinition
		 * @param groupKey the group key or group key expression
		 */
		public HystrixCommandDefinition(final CharSequence groupKey) {
			this(groupKey, null);
		}
		
		
		/**
		 * Builds the command and returns it
		 * @param executor The callable to execute in the returned command
		 * @return the command The built command
		 */
		public HystrixCommand<T> build(final Callable<T> executor) {
			if(executor==null) throw new IllegalArgumentException("The passed executor callable was null");
			return new HystrixCommand<T>(setter) {
				@Override
				protected T run() throws Exception {
					return executor.call();
				}
				
			};
		}

		/**
		 * Specifies the HystrixCommandKey
		 * @param commandKey The command key or command key expression
		 * @return this definition
		 * @see com.netflix.hystrix.HystrixCommand.Setter#andCommandKey(com.netflix.hystrix.HystrixCommandKey)
		 */
		public HystrixCommandDefinition<T> andCommandKey(final CharSequence commandKey) {
			final String resolvedCommandKey = resolve(commandKey, tokens);
			defKey.append(resolvedCommandKey);
			setter.andCommandKey(HystrixCommandKey.Factory.asKey(resolvedCommandKey));
			return this;
		}

		/**
		 * Specifies the HystrixThreadPoolKey
		 * @param threadPoolKey The thread pool key or thread pool key expression 
		 * @return this definition
		 * @see com.netflix.hystrix.HystrixCommand.Setter#andThreadPoolKey(com.netflix.hystrix.HystrixThreadPoolKey)
		 */
		public HystrixCommandDefinition<T> andThreadPoolKey(final CharSequence threadPoolKey) {
			final String resolvedThreadPoolKey = resolve(threadPoolKey, tokens);
			defKey.append(resolvedThreadPoolKey);
			setter.andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey(resolvedThreadPoolKey));
			return this;
		}

		/**
		 * Sets the command properties
		 * @param commandPropertiesDefaults
		 * @return
		 * @see com.netflix.hystrix.HystrixCommand.Setter#andCommandPropertiesDefaults(com.netflix.hystrix.HystrixCommandProperties.Setter)
		 */
		public Setter andCommandPropertiesDefaults(
				com.netflix.hystrix.HystrixCommandProperties.Setter commandPropertiesDefaults) {
			return setter.andCommandPropertiesDefaults(commandPropertiesDefaults);
		}

		/**
		 * @param threadPoolPropertiesDefaults
		 * @return
		 * @see com.netflix.hystrix.HystrixCommand.Setter#andThreadPoolPropertiesDefaults(com.netflix.hystrix.HystrixThreadPoolProperties.Setter)
		 */
		public Setter andThreadPoolPropertiesDefaults(
				com.netflix.hystrix.HystrixThreadPoolProperties.Setter threadPoolPropertiesDefaults) {
			return setter.andThreadPoolPropertiesDefaults(threadPoolPropertiesDefaults);
		}

		/**
		 * @param value
		 * @return
		 * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withCircuitBreakerEnabled(boolean)
		 */
		public com.netflix.hystrix.HystrixCommandProperties.Setter withCircuitBreakerEnabled(boolean value) {
			return commandPropertySetter.withCircuitBreakerEnabled(value);
		}

		/**
		 * @param value
		 * @return
		 * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withCircuitBreakerErrorThresholdPercentage(int)
		 */
		public com.netflix.hystrix.HystrixCommandProperties.Setter withCircuitBreakerErrorThresholdPercentage(
				int value) {
			return commandPropertySetter.withCircuitBreakerErrorThresholdPercentage(value);
		}

		/**
		 * @param value
		 * @return
		 * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withCircuitBreakerForceClosed(boolean)
		 */
		public com.netflix.hystrix.HystrixCommandProperties.Setter withCircuitBreakerForceClosed(boolean value) {
			return commandPropertySetter.withCircuitBreakerForceClosed(value);
		}

		/**
		 * @param value
		 * @return
		 * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withCircuitBreakerForceOpen(boolean)
		 */
		public com.netflix.hystrix.HystrixCommandProperties.Setter withCircuitBreakerForceOpen(boolean value) {
			return commandPropertySetter.withCircuitBreakerForceOpen(value);
		}

		/**
		 * @param value
		 * @return
		 * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withCircuitBreakerRequestVolumeThreshold(int)
		 */
		public com.netflix.hystrix.HystrixCommandProperties.Setter withCircuitBreakerRequestVolumeThreshold(int value) {
			return commandPropertySetter.withCircuitBreakerRequestVolumeThreshold(value);
		}

		/**
		 * @param value
		 * @return
		 * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withCircuitBreakerSleepWindowInMilliseconds(int)
		 */
		public com.netflix.hystrix.HystrixCommandProperties.Setter withCircuitBreakerSleepWindowInMilliseconds(
				int value) {
			return commandPropertySetter.withCircuitBreakerSleepWindowInMilliseconds(value);
		}

		/**
		 * @param value
		 * @return
		 * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withExecutionIsolationSemaphoreMaxConcurrentRequests(int)
		 */
		public com.netflix.hystrix.HystrixCommandProperties.Setter withExecutionIsolationSemaphoreMaxConcurrentRequests(
				int value) {
			return commandPropertySetter.withExecutionIsolationSemaphoreMaxConcurrentRequests(value);
		}

		/**
		 * @param value
		 * @return
		 * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withExecutionIsolationStrategy(com.netflix.hystrix.HystrixCommandProperties.ExecutionIsolationStrategy)
		 */
		public com.netflix.hystrix.HystrixCommandProperties.Setter withExecutionIsolationStrategy(
				ExecutionIsolationStrategy value) {
			return commandPropertySetter.withExecutionIsolationStrategy(value);
		}

		/**
		 * @param value
		 * @return
		 * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withExecutionIsolationThreadInterruptOnTimeout(boolean)
		 */
		public com.netflix.hystrix.HystrixCommandProperties.Setter withExecutionIsolationThreadInterruptOnTimeout(
				boolean value) {
			return commandPropertySetter.withExecutionIsolationThreadInterruptOnTimeout(value);
		}

		/**
		 * @param value
		 * @return
		 * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withExecutionTimeoutInMilliseconds(int)
		 */
		public com.netflix.hystrix.HystrixCommandProperties.Setter withExecutionTimeoutInMilliseconds(int value) {
			return commandPropertySetter.withExecutionTimeoutInMilliseconds(value);
		}

		/**
		 * @param value
		 * @return
		 * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withExecutionTimeoutEnabled(boolean)
		 */
		public com.netflix.hystrix.HystrixCommandProperties.Setter withExecutionTimeoutEnabled(boolean value) {
			return commandPropertySetter.withExecutionTimeoutEnabled(value);
		}

		/**
		 * @param value
		 * @return
		 * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withFallbackIsolationSemaphoreMaxConcurrentRequests(int)
		 */
		public com.netflix.hystrix.HystrixCommandProperties.Setter withFallbackIsolationSemaphoreMaxConcurrentRequests(
				int value) {
			return commandPropertySetter.withFallbackIsolationSemaphoreMaxConcurrentRequests(value);
		}

		/**
		 * @param value
		 * @return
		 * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withFallbackEnabled(boolean)
		 */
		public com.netflix.hystrix.HystrixCommandProperties.Setter withFallbackEnabled(boolean value) {
			return commandPropertySetter.withFallbackEnabled(value);
		}

		/**
		 * @param value
		 * @return
		 * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withMetricsHealthSnapshotIntervalInMilliseconds(int)
		 */
		public com.netflix.hystrix.HystrixCommandProperties.Setter withMetricsHealthSnapshotIntervalInMilliseconds(
				int value) {
			return commandPropertySetter.withMetricsHealthSnapshotIntervalInMilliseconds(value);
		}

		/**
		 * @param value
		 * @return
		 * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withMetricsRollingPercentileBucketSize(int)
		 */
		public com.netflix.hystrix.HystrixCommandProperties.Setter withMetricsRollingPercentileBucketSize(int value) {
			return commandPropertySetter.withMetricsRollingPercentileBucketSize(value);
		}

		/**
		 * @param value
		 * @return
		 * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withMetricsRollingPercentileEnabled(boolean)
		 */
		public com.netflix.hystrix.HystrixCommandProperties.Setter withMetricsRollingPercentileEnabled(boolean value) {
			return commandPropertySetter.withMetricsRollingPercentileEnabled(value);
		}

		/**
		 * @param value
		 * @return
		 * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withMetricsRollingPercentileWindowInMilliseconds(int)
		 */
		public com.netflix.hystrix.HystrixCommandProperties.Setter withMetricsRollingPercentileWindowInMilliseconds(
				int value) {
			return commandPropertySetter.withMetricsRollingPercentileWindowInMilliseconds(value);
		}

		/**
		 * @param value
		 * @return
		 * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withMetricsRollingPercentileWindowBuckets(int)
		 */
		public com.netflix.hystrix.HystrixCommandProperties.Setter withMetricsRollingPercentileWindowBuckets(
				int value) {
			return commandPropertySetter.withMetricsRollingPercentileWindowBuckets(value);
		}

		/**
		 * @param value
		 * @return
		 * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withMetricsRollingStatisticalWindowInMilliseconds(int)
		 */
		public com.netflix.hystrix.HystrixCommandProperties.Setter withCommandMetricsRollingStatisticalWindowInMilliseconds(
				int value) {
			return commandPropertySetter.withMetricsRollingStatisticalWindowInMilliseconds(value);
		}

		/**
		 * @param value
		 * @return
		 * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withMetricsRollingStatisticalWindowBuckets(int)
		 */
		public com.netflix.hystrix.HystrixCommandProperties.Setter withCommandMetricsRollingStatisticalWindowBuckets(
				int value) {
			return commandPropertySetter.withMetricsRollingStatisticalWindowBuckets(value);
		}

		/**
		 * @param value
		 * @return
		 * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withRequestCacheEnabled(boolean)
		 */
		public com.netflix.hystrix.HystrixCommandProperties.Setter withRequestCacheEnabled(boolean value) {
			return commandPropertySetter.withRequestCacheEnabled(value);
		}

		/**
		 * @param value
		 * @return
		 * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withRequestLogEnabled(boolean)
		 */
		public com.netflix.hystrix.HystrixCommandProperties.Setter withRequestLogEnabled(boolean value) {
			return commandPropertySetter.withRequestLogEnabled(value);
		}

		/**
		 * @param value
		 * @return
		 * @see com.netflix.hystrix.HystrixThreadPoolProperties.Setter#withCoreSize(int)
		 */
		public com.netflix.hystrix.HystrixThreadPoolProperties.Setter withCoreSize(int value) {
			return threadPoolPropertySetter.withCoreSize(value);
		}

		/**
		 * @param value
		 * @return
		 * @see com.netflix.hystrix.HystrixThreadPoolProperties.Setter#withKeepAliveTimeMinutes(int)
		 */
		public com.netflix.hystrix.HystrixThreadPoolProperties.Setter withKeepAliveTimeMinutes(int value) {
			return threadPoolPropertySetter.withKeepAliveTimeMinutes(value);
		}

		/**
		 * @param value
		 * @return
		 * @see com.netflix.hystrix.HystrixThreadPoolProperties.Setter#withMaxQueueSize(int)
		 */
		public com.netflix.hystrix.HystrixThreadPoolProperties.Setter withMaxQueueSize(int value) {
			return threadPoolPropertySetter.withMaxQueueSize(value);
		}

		/**
		 * @param value
		 * @return
		 * @see com.netflix.hystrix.HystrixThreadPoolProperties.Setter#withQueueSizeRejectionThreshold(int)
		 */
		public com.netflix.hystrix.HystrixThreadPoolProperties.Setter withQueueSizeRejectionThreshold(int value) {
			return threadPoolPropertySetter.withQueueSizeRejectionThreshold(value);
		}

		/**
		 * @param value
		 * @return
		 * @see com.netflix.hystrix.HystrixThreadPoolProperties.Setter#withMetricsRollingStatisticalWindowInMilliseconds(int)
		 */
		public com.netflix.hystrix.HystrixThreadPoolProperties.Setter withMetricsRollingStatisticalWindowInMilliseconds(
				int value) {
			return threadPoolPropertySetter.withMetricsRollingStatisticalWindowInMilliseconds(value);
		}

		/**
		 * @param value
		 * @return
		 * @see com.netflix.hystrix.HystrixThreadPoolProperties.Setter#withMetricsRollingStatisticalWindowBuckets(int)
		 */
		public com.netflix.hystrix.HystrixThreadPoolProperties.Setter withMetricsRollingStatisticalWindowBuckets(
				int value) {
			return threadPoolPropertySetter.withMetricsRollingStatisticalWindowBuckets(value);
		}
	}
	
}
