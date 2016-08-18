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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.lang.StringHelper;
import com.netflix.hystrix.HystrixCommand;
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

public class HystrixCommandFactory<T> {
	/** The singleton instance */
	private static volatile HystrixCommandFactory<?> instance = null;
	/** The singleton instance ctor lock */
	private static final Object lock = new Object();
	
	/** The cache of command factories keyed by the factory key */
	protected final Cache<String, HystrixCommandFactory<T>.HystrixCommandBuilder> commandFactories = CacheBuilder.newBuilder()
		.concurrencyLevel(Runtime.getRuntime().availableProcessors())
		.initialCapacity(128)
		.build();
	
	/**
	 * Acquires and returns the HystrixCommandFactory singleton instance
	 * @return the HystrixCommandFactory singleton instance
	 */
	@SuppressWarnings("unchecked")
	public static <T> HystrixCommandFactory<T> getInstance() {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					instance = new HystrixCommandFactory<T>();
				}
			}
		}
		return (HystrixCommandFactory<T>) instance;
	}
	
	/**
	 * Creates the HystrixCommandFactory singleton
	 */
	private HystrixCommandFactory() {
	}
	
	/**
	 * Creates and returns a new {@link HystrixCommandBuilder}
	 * @param componentKey The config key for the specific component we're configuring a command builder for
	 * @param groupKey The the group key or group key expression
	 * @param tokens an optional map of tokens for resolving expressions
	 * @return the builder
	 */
	public HystrixCommandBuilder builder(String componentKey, final CharSequence groupKey, final Map<String, Object> tokens) {
		return new HystrixCommandBuilder(componentKey, groupKey, tokens);
	}
	
	/**
	 * Creates and returns a new {@link HystrixCommandBuilder}
	 * @param componentKey The config key for the specific component we're configuring a command builder for
	 * @param groupKey The the group key or group key expression
	 * @return the builder
	 */
	public HystrixCommandBuilder builder(String componentKey, final CharSequence groupKey) {
		return new HystrixCommandBuilder(componentKey, groupKey);
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
	 * <p>Title: HystrixCommandBuilder</p>
	 * <p>Description: A fluent and json friendly {@link HystrixCommand} builder</p> 
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>com.heliosapm.streams.hystrix.HystrixCommandFactory.HystrixCommandBuilder</code></p>
	 * @param <R> The expected type of the command return value
	 * TODO:  implement JSON de/serializer
	 */
	public class HystrixCommandBuilder {
		protected final HystrixCommand.Setter setter; 
		protected final Map<String, Object> tokens;
		protected final StringBuilder defKey = new StringBuilder();
		protected final HystrixCommandProperties.Setter commandPropertySetter = HystrixCommandProperties.defaultSetter();
		protected final HystrixThreadPoolProperties.Setter threadPoolPropertySetter = HystrixThreadPoolProperties.defaultSetter();	
		protected volatile String builderKey = null;
		
		/**
		 * Creates a new HystrixCommandDefinition
		 * @param groupKey the group key or group key expression
		 * @param tokens An optional map of tokens used to resolve expressions
		 */
		private HystrixCommandBuilder(final String componentKey, final CharSequence groupKey, final Map<String, Object> tokens) {
			this.tokens = tokens==null ? Collections.emptyMap() : tokens;
			final String resolvedGroupKey = HystrixCommandFactory.resolve(groupKey, tokens);
			defKey.append("GroupKey=").append(resolvedGroupKey).append(",");
			setter = HystrixCommand.Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey(resolvedGroupKey));
			final HystrixCommandConfigDefault config = new HystrixCommandConfigDefault(componentKey==null ? "" : componentKey.trim());
			loadDefaults(config);
		}
		
		/**
		 * Creates a new HystrixCommandDefinition
		 * @param groupKey the group key or group key expression
		 */
		private HystrixCommandBuilder(final String componentKey, final CharSequence groupKey) {
			this(componentKey, groupKey, null);
		}
		
		public String getKey() {
			if(builderKey==null) {
				builderKey = defKey.deleteCharAt(defKey.length()-1).toString();
			}
			return builderKey;
		}
		
		/**
		 * Builds the command and returns it
		 * @param executor The callable to execute in the returned command
		 * @return the command The built command
		 */
		private HystrixCommandFactory<T>.HystrixCommandBuilder build() {			
			final String key = getKey();
			final Callable<HystrixCommandFactory<T>.HystrixCommandBuilder> builderBuilder = new Callable<HystrixCommandFactory<T>.HystrixCommandBuilder>() {
				@Override
				public HystrixCommandFactory<T>.HystrixCommandBuilder call() throws Exception {
					setter.andCommandPropertiesDefaults(commandPropertySetter);
					setter.andThreadPoolPropertiesDefaults(threadPoolPropertySetter);					
					final ManagedHystrixCommandFactory managed = new ManagedHystrixCommandFactory(setter, getKey(), commandPropertySetter, threadPoolPropertySetter); 
					if(!JMXHelper.isRegistered(managed.getObjectName())) {
						JMXHelper.registerMBean(managed.getObjectName(), managed);
					}
					return (HystrixCommandFactory<T>.HystrixCommandBuilder) HystrixCommandBuilder.this;
				}
			};
			try {
				return commandFactories.get(key, builderBuilder);
			} catch (Exception ex) {
				throw new RuntimeException("Failed to retrieve CommandFactory", ex);
			}
		}
		
		/**
		 * Creates a new {@link HystrixCommand} to run the passed executuion
		 * @param executor The execution the {@link HystrixCommand} will wrap 
		 * @param fallback The optional fallback callable
		 * @return the command
		 */
		public HystrixCommand<T> commandFor(final Callable<T> executor, final Callable<T> fallback) {
			if(executor==null) throw new IllegalArgumentException("The passed executor callable was null");
			final HystrixCommandFactory<T>.HystrixCommandBuilder builder = build();
			final HystrixCommand<T> command;
			if(fallback!=null) {
				commandPropertySetter.withFallbackEnabled(true);
				command = new HystrixCommand<T>(builder.setter) {
					@Override
					protected T run() throws Exception {
						return executor.call();
					}
					@Override
					protected T getFallback() {
						try {
							return fallback.call();
						} catch (Exception ex) {
							throw new RuntimeException("Fallback failed on [" + getCommandGroup().name() + "/" + getCommandKey().name() + "]", ex);
						}
					}					
				};
			} else {
				commandPropertySetter.withFallbackEnabled(false);
				command = new HystrixCommand<T>(builder.setter) {
					@Override
					protected T run() throws Exception {
						return executor.call();
					}
				};				
			}
			return command;
		}
		
		/**
		 * Creates a new {@link HystrixCommand} without a fallback to run the passed executuion
		 * @param executor The execution the {@link HystrixCommand} will wrap 
		 * @return the command
		 */
		public HystrixCommand<T> commandFor(final Callable<T> executor) {
			return commandFor(executor, null);
		}
		

		/**
		 * Specifies the HystrixCommandKey
		 * @param commandKey The command key or command key expression
		 * @return this definition
		 * @see com.netflix.hystrix.HystrixCommand.Setter#andCommandKey(com.netflix.hystrix.HystrixCommandKey)
		 */
		public HystrixCommandBuilder andCommandKey(final CharSequence commandKey) {
			final String resolvedCommandKey = HystrixCommandFactory.resolve(commandKey, tokens);			
			defKey.append("CommandKey=").append(resolvedCommandKey).append(",");
			setter.andCommandKey(HystrixCommandKey.Factory.asKey(resolvedCommandKey));
			return this;
		}

		/**
		 * Specifies the HystrixThreadPoolKey
		 * @param threadPoolKey The thread pool key or thread pool key expression 
		 * @return this definition
		 * @see com.netflix.hystrix.HystrixCommand.Setter#andThreadPoolKey(com.netflix.hystrix.HystrixThreadPoolKey)
		 */
		public HystrixCommandBuilder andThreadPoolKey(final CharSequence threadPoolKey) {
			final String resolvedThreadPoolKey = HystrixCommandFactory.resolve(threadPoolKey, tokens);			
			defKey.append("ThreadPoolKey=").append(resolvedThreadPoolKey).append(",");
			setter.andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey(resolvedThreadPoolKey));
			return this;
		}
		


	    /**
	     * Sets the thread pool's metrics Rolling Statistical Window In Milliseconds setting
	     * @param value The value to set
	     * @return this builder
	     * @see com.netflix.hystrix.HystrixThreadPoolProperties.Setter#withMetricsRollingStatisticalWindowInMilliseconds(int)
	     */
	    public HystrixCommandBuilder threadMetricsRollingStatisticalWindowInMilliseconds(final int value) {
	        threadPoolPropertySetter.withMetricsRollingStatisticalWindowInMilliseconds(value);
	        return this;
	    }        


	    /**
	     * Sets the thread pool's metrics Rolling Statistical Window Buckets setting
	     * @param value The value to set
	     * @return this builder
	     * @see com.netflix.hystrix.HystrixThreadPoolProperties.Setter#withMetricsRollingStatisticalWindowBuckets(int)
	     */
	    public HystrixCommandBuilder threadMetricsRollingStatisticalWindowBuckets(final int value) {
	        threadPoolPropertySetter.withMetricsRollingStatisticalWindowBuckets(value);
	        return this;
	    }        


	    /**
	     * Sets the thread pool's core Size setting
	     * @param value The value to set
	     * @return this builder
	     * @see com.netflix.hystrix.HystrixThreadPoolProperties.Setter#withCoreSize(int)
	     */
	    public HystrixCommandBuilder coreSize(final int value) {
	        threadPoolPropertySetter.withCoreSize(value);
	        return this;
	    }        


	    /**
	     * Sets the thread pool's keep Alive Time Minutes setting
	     * @param value The value to set
	     * @return this builder
	     * @see com.netflix.hystrix.HystrixThreadPoolProperties.Setter#withKeepAliveTimeMinutes(int)
	     */
	    public HystrixCommandBuilder keepAliveTimeMinutes(final int value) {
	        threadPoolPropertySetter.withKeepAliveTimeMinutes(value);
	        return this;
	    }        


	    /**
	     * Sets the thread pool's max Queue Size setting
	     * @param value The value to set
	     * @return this builder
	     * @see com.netflix.hystrix.HystrixThreadPoolProperties.Setter#withMaxQueueSize(int)
	     */
	    public HystrixCommandBuilder maxQueueSize(final int value) {
	        threadPoolPropertySetter.withMaxQueueSize(value);
	        return this;
	    }        


	    /**
	     * Sets the thread pool's queue Size Rejection Threshold setting
	     * @param value The value to set
	     * @return this builder
	     * @see com.netflix.hystrix.HystrixThreadPoolProperties.Setter#withQueueSizeRejectionThreshold(int)
	     */
	    public HystrixCommandBuilder queueSizeRejectionThreshold(final int value) {
	        threadPoolPropertySetter.withQueueSizeRejectionThreshold(value);
	        return this;
	    }        


		

	    /**
	     * Sets the circuit's BreakerEnabled setting
	     * @param value The value to set
	     * @return this builder
	     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withCircuitBreakerEnabled(boolean)
	     */
	    public HystrixCommandBuilder circuitBreakerEnabled(final boolean value) {
	        commandPropertySetter.withCircuitBreakerEnabled(value);
	        return this;
	    }        


	    /**
	     * Sets the circuit's BreakerErrorThresholdPercentage setting
	     * @param value The value to set
	     * @return this builder
	     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withCircuitBreakerErrorThresholdPercentage(int)
	     */
	    public HystrixCommandBuilder circuitBreakerErrorThresholdPercentage(final int value) {
	        commandPropertySetter.withCircuitBreakerErrorThresholdPercentage(value);
	        return this;
	    }        


	    /**
	     * Sets the circuit's BreakerForceClosed setting
	     * @param value The value to set
	     * @return this builder
	     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withCircuitBreakerForceClosed(boolean)
	     */
	    public HystrixCommandBuilder circuitBreakerForceClosed(final boolean value) {
	        commandPropertySetter.withCircuitBreakerForceClosed(value);
	        return this;
	    }        


	    /**
	     * Sets the circuit's BreakerForceOpen setting
	     * @param value The value to set
	     * @return this builder
	     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withCircuitBreakerForceOpen(boolean)
	     */
	    public HystrixCommandBuilder circuitBreakerForceOpen(final boolean value) {
	        commandPropertySetter.withCircuitBreakerForceOpen(value);
	        return this;
	    }        


	    /**
	     * Sets the circuit's BreakerRequestVolumeThreshold setting
	     * @param value The value to set
	     * @return this builder
	     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withCircuitBreakerRequestVolumeThreshold(int)
	     */
	    public HystrixCommandBuilder circuitBreakerRequestVolumeThreshold(final int value) {
	        commandPropertySetter.withCircuitBreakerRequestVolumeThreshold(value);
	        return this;
	    }        


	    /**
	     * Sets the circuit's BreakerSleepWindowInMilliseconds setting
	     * @param value The value to set
	     * @return this builder
	     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withCircuitBreakerSleepWindowInMilliseconds(int)
	     */
	    public HystrixCommandBuilder circuitBreakerSleepWindowInMilliseconds(final int value) {
	        commandPropertySetter.withCircuitBreakerSleepWindowInMilliseconds(value);
	        return this;
	    }        


	    /**
	     * Sets the execution's IsolationSemaphoreMaxConcurrentRequests setting
	     * @param value The value to set
	     * @return this builder
	     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withExecutionIsolationSemaphoreMaxConcurrentRequests(int)
	     */
	    public HystrixCommandBuilder executionIsolationSemaphoreMaxConcurrentRequests(final int value) {
	        commandPropertySetter.withExecutionIsolationSemaphoreMaxConcurrentRequests(value);
	        return this;
	    }        


	    /**
	     * Sets the execution's IsolationStrategy setting
	     * @param value The value to set
	     * @return this builder
	     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withExecutionIsolationStrategy(com.netflix.hystrix.HystrixCommandProperties$ExecutionIsolationStrategy)
	     */
	    public HystrixCommandBuilder executionIsolationStrategy(final ExecutionIsolationStrategy value) {
	        commandPropertySetter.withExecutionIsolationStrategy(value);
	        return this;
	    }        


	    /**
	     * Sets the execution's IsolationThreadInterruptOnTimeout setting
	     * @param value The value to set
	     * @return this builder
	     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withExecutionIsolationThreadInterruptOnTimeout(boolean)
	     */
	    public HystrixCommandBuilder executionIsolationThreadInterruptOnTimeout(final boolean value) {
	        commandPropertySetter.withExecutionIsolationThreadInterruptOnTimeout(value);
	        return this;
	    }        


	    /**
	     * Sets the execution's TimeoutInMilliseconds setting
	     * @param value The value to set
	     * @return this builder
	     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withExecutionTimeoutInMilliseconds(int)
	     */
	    public HystrixCommandBuilder executionTimeoutInMilliseconds(final int value) {
	        commandPropertySetter.withExecutionTimeoutInMilliseconds(value);
	        return this;
	    }        


	    /**
	     * Sets the execution's TimeoutEnabled setting
	     * @param value The value to set
	     * @return this builder
	     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withExecutionTimeoutEnabled(boolean)
	     */
	    public HystrixCommandBuilder executionTimeoutEnabled(final boolean value) {
	        commandPropertySetter.withExecutionTimeoutEnabled(value);
	        return this;
	    }        


	    /**
	     * Sets the fallback's IsolationSemaphoreMaxConcurrentRequests setting
	     * @param value The value to set
	     * @return this builder
	     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withFallbackIsolationSemaphoreMaxConcurrentRequests(int)
	     */
	    public HystrixCommandBuilder fallbackIsolationSemaphoreMaxConcurrentRequests(final int value) {
	        commandPropertySetter.withFallbackIsolationSemaphoreMaxConcurrentRequests(value);
	        return this;
	    }        


	    /**
	     * Sets the fallback's Enabled setting
	     * @param value The value to set
	     * @return this builder
	     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withFallbackEnabled(boolean)
	     */
	    public HystrixCommandBuilder fallbackEnabled(final boolean value) {
	        commandPropertySetter.withFallbackEnabled(value);
	        return this;
	    }        


	    /**
	     * Sets the metrics's HealthSnapshotIntervalInMilliseconds setting
	     * @param value The value to set
	     * @return this builder
	     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withMetricsHealthSnapshotIntervalInMilliseconds(int)
	     */
	    public HystrixCommandBuilder metricsHealthSnapshotIntervalInMilliseconds(final int value) {
	        commandPropertySetter.withMetricsHealthSnapshotIntervalInMilliseconds(value);
	        return this;
	    }        


	    /**
	     * Sets the metrics's RollingPercentileBucketSize setting
	     * @param value The value to set
	     * @return this builder
	     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withMetricsRollingPercentileBucketSize(int)
	     */
	    public HystrixCommandBuilder metricsRollingPercentileBucketSize(final int value) {
	        commandPropertySetter.withMetricsRollingPercentileBucketSize(value);
	        return this;
	    }        


	    /**
	     * Sets the metrics's RollingPercentileEnabled setting
	     * @param value The value to set
	     * @return this builder
	     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withMetricsRollingPercentileEnabled(boolean)
	     */
	    public HystrixCommandBuilder metricsRollingPercentileEnabled(final boolean value) {
	        commandPropertySetter.withMetricsRollingPercentileEnabled(value);
	        return this;
	    }        


	    /**
	     * Sets the metrics's RollingPercentileWindowInMilliseconds setting
	     * @param value The value to set
	     * @return this builder
	     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withMetricsRollingPercentileWindowInMilliseconds(int)
	     */
	    public HystrixCommandBuilder metricsRollingPercentileWindowInMilliseconds(final int value) {
	        commandPropertySetter.withMetricsRollingPercentileWindowInMilliseconds(value);
	        return this;
	    }        


	    /**
	     * Sets the metrics's RollingPercentileWindowBuckets setting
	     * @param value The value to set
	     * @return this builder
	     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withMetricsRollingPercentileWindowBuckets(int)
	     */
	    public HystrixCommandBuilder metricsRollingPercentileWindowBuckets(final int value) {
	        commandPropertySetter.withMetricsRollingPercentileWindowBuckets(value);
	        return this;
	    }        


	    /**
	     * Sets the metrics's RollingStatisticalWindowInMilliseconds setting
	     * @param value The value to set
	     * @return this builder
	     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withMetricsRollingStatisticalWindowInMilliseconds(int)
	     */
	    public HystrixCommandBuilder metricsRollingStatisticalWindowInMilliseconds(final int value) {
	        commandPropertySetter.withMetricsRollingStatisticalWindowInMilliseconds(value);
	        return this;
	    }        


	    /**
	     * Sets the metrics's RollingStatisticalWindowBuckets setting
	     * @param value The value to set
	     * @return this builder
	     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withMetricsRollingStatisticalWindowBuckets(int)
	     */
	    public HystrixCommandBuilder metricsRollingStatisticalWindowBuckets(final int value) {
	        commandPropertySetter.withMetricsRollingStatisticalWindowBuckets(value);
	        return this;
	    }        


	    /**
	     * Sets the request's CacheEnabled setting
	     * @param value The value to set
	     * @return this builder
	     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withRequestCacheEnabled(boolean)
	     */
	    public HystrixCommandBuilder requestCacheEnabled(final boolean value) {
	        commandPropertySetter.withRequestCacheEnabled(value);
	        return this;
	    }        


	    /**
	     * Sets the request's LogEnabled setting
	     * @param value The value to set
	     * @return this builder
	     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withRequestLogEnabled(boolean)
	     */
	    public HystrixCommandBuilder requestLogEnabled(final boolean value) {
	        commandPropertySetter.withRequestLogEnabled(value);
	        return this;
	    }
	    
	    
	    private void loadDefaults(final HystrixCommandConfigDefault config) {
	    	commandPropertySetter.withCircuitBreakerEnabled(config.circuitBreakerEnabled);
	    	commandPropertySetter.withCircuitBreakerErrorThresholdPercentage(config.circuitBreakerErrorThresholdPercentage);
	    	commandPropertySetter.withCircuitBreakerForceClosed(config.circuitBreakerForceClosed);
	    	commandPropertySetter.withCircuitBreakerForceOpen(config.circuitBreakerForceOpen);
	    	commandPropertySetter.withCircuitBreakerRequestVolumeThreshold(config.circuitBreakerRequestVolumeThreshold);
	    	commandPropertySetter.withCircuitBreakerSleepWindowInMilliseconds(config.circuitBreakerSleepWindowInMilliseconds);
	    	commandPropertySetter.withExecutionIsolationSemaphoreMaxConcurrentRequests(config.executionIsolationSemaphoreMaxConcurrentRequests);
	    	commandPropertySetter.withExecutionIsolationStrategy(config.executionIsolationStrategy);
	    	commandPropertySetter.withExecutionIsolationThreadInterruptOnTimeout(config.executionIsolationThreadInterruptOnTimeout);
	    	commandPropertySetter.withExecutionTimeoutInMilliseconds(config.executionTimeoutInMilliseconds);
	    	commandPropertySetter.withExecutionTimeoutEnabled(config.executionTimeoutEnabled);
	    	commandPropertySetter.withFallbackIsolationSemaphoreMaxConcurrentRequests(config.fallbackIsolationSemaphoreMaxConcurrentRequests);
	    	commandPropertySetter.withFallbackEnabled(config.fallbackEnabled);
	    	commandPropertySetter.withMetricsHealthSnapshotIntervalInMilliseconds(config.metricsHealthSnapshotIntervalInMilliseconds);
	    	commandPropertySetter.withMetricsRollingPercentileBucketSize(config.metricsRollingPercentileBucketSize);
	    	commandPropertySetter.withMetricsRollingPercentileEnabled(config.metricsRollingPercentileEnabled);
	    	commandPropertySetter.withMetricsRollingPercentileWindowInMilliseconds(config.metricsRollingPercentileWindowInMilliseconds);
	    	commandPropertySetter.withMetricsRollingPercentileWindowBuckets(config.metricsRollingPercentileWindowBuckets);
	    	commandPropertySetter.withMetricsRollingStatisticalWindowInMilliseconds(config.metricsRollingStatisticalWindowInMilliseconds);
	    	commandPropertySetter.withMetricsRollingStatisticalWindowBuckets(config.metricsRollingStatisticalWindowBuckets);
	    	commandPropertySetter.withRequestCacheEnabled(config.requestCacheEnabled);
	    	commandPropertySetter.withRequestLogEnabled(config.requestLogEnabled);
	    	
	    	threadPoolPropertySetter.withMetricsRollingStatisticalWindowInMilliseconds(config.metricsRollingStatisticalWindowInMilliseconds);
	    	threadPoolPropertySetter.withMetricsRollingStatisticalWindowBuckets(config.metricsRollingStatisticalWindowBuckets);
	    	threadPoolPropertySetter.withCoreSize(config.coreSize);
	    	threadPoolPropertySetter.withKeepAliveTimeMinutes(config.keepAliveTimeMinutes);
	    	threadPoolPropertySetter.withMaxQueueSize(config.maxQueueSize);
	    	threadPoolPropertySetter.withQueueSizeRejectionThreshold(config.queueSizeRejectionThreshold);
	    	
	    }
		

	}	
	
	
/*

threadPoolPropertySetter.withMetricsRollingStatisticalWindowInMilliseconds(config.metricsRollingStatisticalWindowInMilliseconds);
threadPoolPropertySetter.withMetricsRollingStatisticalWindowBuckets(config.metricsRollingStatisticalWindowBuckets);
threadPoolPropertySetter.withCoreSize(config.coreSize);
threadPoolPropertySetter.withKeepAliveTimeMinutes(config.keepAliveTimeMinutes);
threadPoolPropertySetter.withMaxQueueSize(config.maxQueueSize);
threadPoolPropertySetter.withQueueSizeRejectionThreshold(config.queueSizeRejectionThreshold);

commandPropertySetter.withCircuitBreakerEnabled(config.circuitBreakerEnabled);
commandPropertySetter.withCircuitBreakerErrorThresholdPercentage(config.circuitBreakerErrorThresholdPercentage);
commandPropertySetter.withCircuitBreakerForceClosed(config.circuitBreakerForceClosed);
commandPropertySetter.withCircuitBreakerForceOpen(config.circuitBreakerForceOpen);
commandPropertySetter.withCircuitBreakerRequestVolumeThreshold(config.circuitBreakerRequestVolumeThreshold);
commandPropertySetter.withCircuitBreakerSleepWindowInMilliseconds(config.circuitBreakerSleepWindowInMilliseconds);
commandPropertySetter.withExecutionIsolationSemaphoreMaxConcurrentRequests(config.executionIsolationSemaphoreMaxConcurrentRequests);
commandPropertySetter.withExecutionIsolationStrategy(config.executionIsolationStrategy);
commandPropertySetter.withExecutionIsolationThreadInterruptOnTimeout(config.executionIsolationThreadInterruptOnTimeout);
commandPropertySetter.withExecutionIsolationThreadTimeoutInMilliseconds(config.executionIsolationThreadTimeoutInMilliseconds);
commandPropertySetter.withExecutionTimeoutInMilliseconds(config.executionTimeoutInMilliseconds);
commandPropertySetter.withExecutionTimeoutEnabled(config.executionTimeoutEnabled);
commandPropertySetter.withFallbackIsolationSemaphoreMaxConcurrentRequests(config.fallbackIsolationSemaphoreMaxConcurrentRequests);
commandPropertySetter.withFallbackEnabled(config.fallbackEnabled);
commandPropertySetter.withMetricsHealthSnapshotIntervalInMilliseconds(config.metricsHealthSnapshotIntervalInMilliseconds);
commandPropertySetter.withMetricsRollingPercentileBucketSize(config.metricsRollingPercentileBucketSize);
commandPropertySetter.withMetricsRollingPercentileEnabled(config.metricsRollingPercentileEnabled);
commandPropertySetter.withMetricsRollingPercentileWindowInMilliseconds(config.metricsRollingPercentileWindowInMilliseconds);
commandPropertySetter.withMetricsRollingPercentileWindowBuckets(config.metricsRollingPercentileWindowBuckets);
commandPropertySetter.withMetricsRollingStatisticalWindowInMilliseconds(config.metricsRollingStatisticalWindowInMilliseconds);
commandPropertySetter.withMetricsRollingStatisticalWindowBuckets(config.metricsRollingStatisticalWindowBuckets);
commandPropertySetter.withRequestCacheEnabled(config.requestCacheEnabled);
commandPropertySetter.withRequestLogEnabled(config.requestLogEnabled);





 */
	
}
