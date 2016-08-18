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

import com.heliosapm.utils.config.ConfigurationHelper;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandProperties.ExecutionIsolationStrategy;

/**
 * <p>Title: HystrixCommandConfigDefault</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.hystrix.HystrixCommandConfigDefault</code></p>
 */

public class HystrixCommandConfigDefault {
	
    // ======================================================================================================================
    //		Thread pool circuit breaker defaults
    // ======================================================================================================================

    /** The config key suffix for hystrix CircuitBreakerEnabled */
    public static final String CONFIG_CIRCUIT_BREAKER_ENABLED = "circuit.breaker.enabled";
    /** The default hystrix CircuitBreakerEnabled */
    public static final boolean DEFAULT_CIRCUIT_BREAKER_ENABLED = true;

    /** The config key suffix for hystrix CircuitBreakerErrorThresholdPercentage */
    public static final String CONFIG_CIRCUIT_BREAKER_ERROR_THRESHOLD_PERCENTAGE = "circuit.breaker.error.threshold.percentage";
    /** The default hystrix CircuitBreakerErrorThresholdPercentage */
    public static final int DEFAULT_CIRCUIT_BREAKER_ERROR_THRESHOLD_PERCENTAGE = 5;

    /** The config key suffix for hystrix CircuitBreakerForceClosed */
    public static final String CONFIG_CIRCUIT_BREAKER_FORCE_CLOSED = "circuit.breaker.force.closed";
    /** The default hystrix CircuitBreakerForceClosed */
    public static final boolean DEFAULT_CIRCUIT_BREAKER_FORCE_CLOSED = false;

    /** The config key suffix for hystrix CircuitBreakerForceOpen */
    public static final String CONFIG_CIRCUIT_BREAKER_FORCE_OPEN = "circuit.breaker.force.open";
    /** The default hystrix CircuitBreakerForceOpen */
    public static final boolean DEFAULT_CIRCUIT_BREAKER_FORCE_OPEN = false;

    /** The config key suffix for hystrix CircuitBreakerRequestVolumeThreshold */
    public static final String CONFIG_CIRCUIT_BREAKER_REQUEST_VOLUME_THRESHOLD = "circuit.breaker.request.volume.threshold";
    /** The default hystrix CircuitBreakerRequestVolumeThreshold */
    public static final int DEFAULT_CIRCUIT_BREAKER_REQUEST_VOLUME_THRESHOLD = 5;

    /** The config key suffix for hystrix CircuitBreakerSleepWindowInMilliseconds */
    public static final String CONFIG_CIRCUIT_BREAKER_SLEEP_WINDOW_IN_MILLISECONDS = "circuit.breaker.sleep.window.in.milliseconds";
    /** The default hystrix CircuitBreakerSleepWindowInMilliseconds */
    public static final int DEFAULT_CIRCUIT_BREAKER_SLEEP_WINDOW_IN_MILLISECONDS = 5;

    /** The config key suffix for hystrix ExecutionIsolationSemaphoreMaxConcurrentRequests */
    public static final String CONFIG_EXECUTION_ISOLATION_SEMAPHORE_MAX_CONCURRENT_REQUESTS = "execution.isolation.semaphore.max.concurrent.requests";
    /** The default hystrix ExecutionIsolationSemaphoreMaxConcurrentRequests */
    public static final int DEFAULT_EXECUTION_ISOLATION_SEMAPHORE_MAX_CONCURRENT_REQUESTS = 5;

    /** The config key suffix for hystrix ExecutionIsolationStrategy */
    public static final String CONFIG_EXECUTION_ISOLATION_STRATEGY = "execution.isolation.strategy";
    /** The default hystrix ExecutionIsolationStrategy */
    public static final ExecutionIsolationStrategy DEFAULT_EXECUTION_ISOLATION_STRATEGY = ExecutionIsolationStrategy.THREAD;

    /** The config key suffix for hystrix ExecutionIsolationThreadInterruptOnTimeout */
    public static final String CONFIG_EXECUTION_ISOLATION_THREAD_INTERRUPT_ON_TIMEOUT = "execution.isolation.thread.interrupt.on.timeout";
    /** The default hystrix ExecutionIsolationThreadInterruptOnTimeout */
    public static final boolean DEFAULT_EXECUTION_ISOLATION_THREAD_INTERRUPT_ON_TIMEOUT = false;

    /** The config key suffix for hystrix ExecutionIsolationThreadTimeoutInMilliseconds */
    public static final String CONFIG_EXECUTION_ISOLATION_THREAD_TIMEOUT_IN_MILLISECONDS = "execution.isolation.thread.timeout.in.milliseconds";
    /** The default hystrix ExecutionIsolationThreadTimeoutInMilliseconds */
    public static final int DEFAULT_EXECUTION_ISOLATION_THREAD_TIMEOUT_IN_MILLISECONDS = 5;

    /** The config key suffix for hystrix ExecutionTimeoutInMilliseconds */
    public static final String CONFIG_EXECUTION_TIMEOUT_IN_MILLISECONDS = "execution.timeout.in.milliseconds";
    /** The default hystrix ExecutionTimeoutInMilliseconds */
    public static final int DEFAULT_EXECUTION_TIMEOUT_IN_MILLISECONDS = 5;

    /** The config key suffix for hystrix ExecutionTimeoutEnabled */
    public static final String CONFIG_EXECUTION_TIMEOUT_ENABLED = "execution.timeout.enabled";
    /** The default hystrix ExecutionTimeoutEnabled */
    public static final boolean DEFAULT_EXECUTION_TIMEOUT_ENABLED = false;

    /** The config key suffix for hystrix FallbackIsolationSemaphoreMaxConcurrentRequests */
    public static final String CONFIG_FALLBACK_ISOLATION_SEMAPHORE_MAX_CONCURRENT_REQUESTS = "fallback.isolation.semaphore.max.concurrent.requests";
    /** The default hystrix FallbackIsolationSemaphoreMaxConcurrentRequests */
    public static final int DEFAULT_FALLBACK_ISOLATION_SEMAPHORE_MAX_CONCURRENT_REQUESTS = 5;

    /** The config key suffix for hystrix FallbackEnabled */
    public static final String CONFIG_FALLBACK_ENABLED = "fallback.enabled";
    /** The default hystrix FallbackEnabled */
    public static final boolean DEFAULT_FALLBACK_ENABLED = false;

    /** The config key suffix for hystrix MetricsHealthSnapshotIntervalInMilliseconds */
    public static final String CONFIG_METRICS_HEALTH_SNAPSHOT_INTERVAL_IN_MILLISECONDS = "metrics.health.snapshot.interval.in.milliseconds";
    /** The default hystrix MetricsHealthSnapshotIntervalInMilliseconds */
    public static final int DEFAULT_METRICS_HEALTH_SNAPSHOT_INTERVAL_IN_MILLISECONDS = 5;

    /** The config key suffix for hystrix MetricsRollingPercentileBucketSize */
    public static final String CONFIG_METRICS_ROLLING_PERCENTILE_BUCKET_SIZE = "metrics.rolling.percentile.bucket.size";
    /** The default hystrix MetricsRollingPercentileBucketSize */
    public static final int DEFAULT_METRICS_ROLLING_PERCENTILE_BUCKET_SIZE = 5;

    /** The config key suffix for hystrix MetricsRollingPercentileEnabled */
    public static final String CONFIG_METRICS_ROLLING_PERCENTILE_ENABLED = "metrics.rolling.percentile.enabled";
    /** The default hystrix MetricsRollingPercentileEnabled */
    public static final boolean DEFAULT_METRICS_ROLLING_PERCENTILE_ENABLED = false;

    /** The config key suffix for hystrix MetricsRollingPercentileWindowInMilliseconds */
    public static final String CONFIG_METRICS_ROLLING_PERCENTILE_WINDOW_IN_MILLISECONDS = "metrics.rolling.percentile.window.in.milliseconds";
    /** The default hystrix MetricsRollingPercentileWindowInMilliseconds */
    public static final int DEFAULT_METRICS_ROLLING_PERCENTILE_WINDOW_IN_MILLISECONDS = 5;

    /** The config key suffix for hystrix MetricsRollingPercentileWindowBuckets */
    public static final String CONFIG_METRICS_ROLLING_PERCENTILE_WINDOW_BUCKETS = "metrics.rolling.percentile.window.buckets";
    /** The default hystrix MetricsRollingPercentileWindowBuckets */
    public static final int DEFAULT_METRICS_ROLLING_PERCENTILE_WINDOW_BUCKETS = 5;

    /** The config key suffix for hystrix MetricsRollingStatisticalWindowInMilliseconds */
    public static final String CONFIG_METRICS_ROLLING_STATISTICAL_WINDOW_IN_MILLISECONDS = "metrics.rolling.statistical.window.in.milliseconds";
    /** The default hystrix MetricsRollingStatisticalWindowInMilliseconds */
    public static final int DEFAULT_METRICS_ROLLING_STATISTICAL_WINDOW_IN_MILLISECONDS = 5;

    /** The config key suffix for hystrix MetricsRollingStatisticalWindowBuckets */
    public static final String CONFIG_METRICS_ROLLING_STATISTICAL_WINDOW_BUCKETS = "metrics.rolling.statistical.window.buckets";
    /** The default hystrix MetricsRollingStatisticalWindowBuckets */
    public static final int DEFAULT_METRICS_ROLLING_STATISTICAL_WINDOW_BUCKETS = 5;

    /** The config key suffix for hystrix RequestCacheEnabled */
    public static final String CONFIG_REQUEST_CACHE_ENABLED = "request.cache.enabled";
    /** The default hystrix RequestCacheEnabled */
    public static final boolean DEFAULT_REQUEST_CACHE_ENABLED = false;

    /** The config key suffix for hystrix RequestLogEnabled */
    public static final String CONFIG_REQUEST_LOG_ENABLED = "request.log.enabled";
    /** The default hystrix RequestLogEnabled */
    public static final boolean DEFAULT_REQUEST_LOG_ENABLED = false;
    
    // ======================================================================================================================
    //		Thread pool config defaults
    // ======================================================================================================================


    /** The config key suffix for hystrix thread pool metrics Rolling Statistical Window In Milliseconds */
    public static final String CONFIG_THREAD_STATISTICAL_WINDOW_IN_MILLISECONDS = "thread.rolling.statistical.window.in.milliseconds";
    /** The default hystrix thread pool metrics Rolling Statistical Window In Milliseconds */
    public static final int DEFAULT_THREAD_STATISTICAL_WINDOW_IN_MILLISECONDS = 5;                

    /** The config key suffix for hystrix thread pool metrics Rolling Statistical Window Buckets */
    public static final String CONFIG_THREAD_STATISTICAL_WINDOW_BUCKETS = "thread.rolling.statistical.window.buckets";
    /** The default hystrix thread pool metrics Rolling Statistical Window Buckets */
    public static final int DEFAULT_THREAD_STATISTICAL_WINDOW_BUCKETS = 5;                

    /** The config key suffix for hystrix thread pool core Size */
    public static final String CONFIG_CORE_SIZE = "pool.core.size";
    /** The default hystrix thread pool core Size */
    public static final int DEFAULT_CORE_SIZE = 5;                

    /** The config key suffix for hystrix thread pool keep Alive Time Minutes */
    public static final String CONFIG_KEEP_ALIVE_TIME_MINUTES = "pool.keep.alive.time.minutes";
    /** The default hystrix thread pool keep Alive Time Minutes */
    public static final int DEFAULT_KEEP_ALIVE_TIME_MINUTES = 5;                

    /** The config key suffix for hystrix thread pool max Queue Size */
    public static final String CONFIG_MAX_QUEUE_SIZE = "pool.max.queue.size";
    /** The default hystrix thread pool max Queue Size */
    public static final int DEFAULT_MAX_QUEUE_SIZE = 5;                

    /** The config key suffix for hystrix thread pool queue Size Rejection Threshold */
    public static final String CONFIG_QUEUE_SIZE_REJECTION_THRESHOLD = "queue.size.rejection.threshold";
    /** The default hystrix thread pool queue Size Rejection Threshold */
    public static final int DEFAULT_QUEUE_SIZE_REJECTION_THRESHOLD = 5;                

    // ======================================================================================================================
    
    
	/** The configured circuit Breaker Enabled */
	public final boolean circuitBreakerEnabled;
	/** The configured circuit Breaker Error Threshold Percentage */
	public final int circuitBreakerErrorThresholdPercentage;
	/** The configured circuit Breaker Force Closed */
	public final boolean circuitBreakerForceClosed;
	/** The configured circuit Breaker Force Open */
	public final boolean circuitBreakerForceOpen;
	/** The configured circuit Breaker Request Volume Threshold */
	public final int circuitBreakerRequestVolumeThreshold;
	/** The configured circuit Breaker Sleep Window In Milliseconds */
	public final int circuitBreakerSleepWindowInMilliseconds;
	/** The configured execution Isolation Semaphore Max Concurrent Requests */
	public final int executionIsolationSemaphoreMaxConcurrentRequests;
	/** The configured execution Isolation Strategy */
	public final ExecutionIsolationStrategy executionIsolationStrategy;
	/** The configured execution Isolation Thread Interrupt On Timeout */
	public final boolean executionIsolationThreadInterruptOnTimeout;
	/** The configured execution Isolation Thread Timeout In Milliseconds */
	public final int executionIsolationThreadTimeoutInMilliseconds;
	/** The configured execution Timeout In Milliseconds */
	public final int executionTimeoutInMilliseconds;
	/** The configured execution Timeout Enabled */
	public final boolean executionTimeoutEnabled;
	/** The configured fallback Isolation Semaphore Max Concurrent Requests */
	public final int fallbackIsolationSemaphoreMaxConcurrentRequests;
	/** The configured fallback Enabled */
	public final boolean fallbackEnabled;
	/** The configured metrics Health Snapshot Interval In Milliseconds */
	public final int metricsHealthSnapshotIntervalInMilliseconds;
	/** The configured metrics Rolling Percentile Bucket Size */
	public final int metricsRollingPercentileBucketSize;
	/** The configured metrics Rolling Percentile Enabled */
	public final boolean metricsRollingPercentileEnabled;
	/** The configured metrics Rolling Percentile Window In Milliseconds */
	public final int metricsRollingPercentileWindowInMilliseconds;
	/** The configured metrics Rolling Percentile Window Buckets */
	public final int metricsRollingPercentileWindowBuckets;
	/** The configured metrics Rolling Statistical Window In Milliseconds */
	public final int metricsRollingStatisticalWindowInMilliseconds;
	/** The configured metrics Rolling Statistical Window Buckets */
	public final int metricsRollingStatisticalWindowBuckets;
	/** The configured request Cache Enabled */
	public final boolean requestCacheEnabled;
	/** The configured request Log Enabled */
	public final boolean requestLogEnabled;
    
	

    /** The configured thread pool metrics Rolling Statistical Window In Milliseconds */
    public final int threadRollingStatisticalWindowInMilliseconds;
    /** The configured thread pool metrics Rolling Statistical Window Buckets */
    public final int threadRollingStatisticalWindowBuckets;
    /** The configured thread pool core Size */
    public final int coreSize;
    /** The configured thread pool keep Alive Time Minutes */
    public final int keepAliveTimeMinutes;
    /** The configured thread pool max Queue Size */
    public final int maxQueueSize;
    /** The configured thread pool queue Size Rejection Threshold */
    public final int queueSizeRejectionThreshold;
	
     

	/**
	 * Creates a new HystrixCommandConfigDefault
	 */
	public HystrixCommandConfigDefault(final String componentPrefix) {
		
		threadRollingStatisticalWindowInMilliseconds = ConfigurationHelper.getIntSystemThenEnvProperty(componentPrefix + "." + CONFIG_THREAD_STATISTICAL_WINDOW_IN_MILLISECONDS, ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_THREAD_STATISTICAL_WINDOW_IN_MILLISECONDS, DEFAULT_THREAD_STATISTICAL_WINDOW_IN_MILLISECONDS));
		threadRollingStatisticalWindowBuckets = ConfigurationHelper.getIntSystemThenEnvProperty(componentPrefix + "." + CONFIG_THREAD_STATISTICAL_WINDOW_BUCKETS, ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_THREAD_STATISTICAL_WINDOW_BUCKETS, DEFAULT_THREAD_STATISTICAL_WINDOW_BUCKETS));
		coreSize = ConfigurationHelper.getIntSystemThenEnvProperty(componentPrefix + "." + CONFIG_CORE_SIZE, ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_CORE_SIZE, DEFAULT_CORE_SIZE));
		keepAliveTimeMinutes = ConfigurationHelper.getIntSystemThenEnvProperty(componentPrefix + "." + CONFIG_KEEP_ALIVE_TIME_MINUTES, ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_KEEP_ALIVE_TIME_MINUTES, DEFAULT_KEEP_ALIVE_TIME_MINUTES));
		maxQueueSize = ConfigurationHelper.getIntSystemThenEnvProperty(componentPrefix + "." + CONFIG_MAX_QUEUE_SIZE, ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_MAX_QUEUE_SIZE, DEFAULT_MAX_QUEUE_SIZE));
		queueSizeRejectionThreshold = ConfigurationHelper.getIntSystemThenEnvProperty(componentPrefix + "." + CONFIG_QUEUE_SIZE_REJECTION_THRESHOLD, ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_QUEUE_SIZE_REJECTION_THRESHOLD, DEFAULT_QUEUE_SIZE_REJECTION_THRESHOLD));
		
		
		circuitBreakerEnabled = ConfigurationHelper.getBooleanSystemThenEnvProperty(componentPrefix + "." + CONFIG_CIRCUIT_BREAKER_ENABLED, ConfigurationHelper.getBooleanSystemThenEnvProperty(CONFIG_CIRCUIT_BREAKER_ENABLED, DEFAULT_CIRCUIT_BREAKER_ENABLED));
		circuitBreakerErrorThresholdPercentage = ConfigurationHelper.getIntSystemThenEnvProperty(componentPrefix + "." + CONFIG_CIRCUIT_BREAKER_ERROR_THRESHOLD_PERCENTAGE, ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_CIRCUIT_BREAKER_ERROR_THRESHOLD_PERCENTAGE, DEFAULT_CIRCUIT_BREAKER_ERROR_THRESHOLD_PERCENTAGE));
		circuitBreakerForceClosed = ConfigurationHelper.getBooleanSystemThenEnvProperty(componentPrefix + "." + CONFIG_CIRCUIT_BREAKER_FORCE_CLOSED, ConfigurationHelper.getBooleanSystemThenEnvProperty(CONFIG_CIRCUIT_BREAKER_FORCE_CLOSED, DEFAULT_CIRCUIT_BREAKER_FORCE_CLOSED));
		circuitBreakerForceOpen = ConfigurationHelper.getBooleanSystemThenEnvProperty(componentPrefix + "." + CONFIG_CIRCUIT_BREAKER_FORCE_OPEN, ConfigurationHelper.getBooleanSystemThenEnvProperty(CONFIG_CIRCUIT_BREAKER_FORCE_OPEN, DEFAULT_CIRCUIT_BREAKER_FORCE_OPEN));
		circuitBreakerRequestVolumeThreshold = ConfigurationHelper.getIntSystemThenEnvProperty(componentPrefix + "." + CONFIG_CIRCUIT_BREAKER_REQUEST_VOLUME_THRESHOLD, ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_CIRCUIT_BREAKER_REQUEST_VOLUME_THRESHOLD, DEFAULT_CIRCUIT_BREAKER_REQUEST_VOLUME_THRESHOLD));
		circuitBreakerSleepWindowInMilliseconds = ConfigurationHelper.getIntSystemThenEnvProperty(componentPrefix + "." + CONFIG_CIRCUIT_BREAKER_SLEEP_WINDOW_IN_MILLISECONDS, ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_CIRCUIT_BREAKER_SLEEP_WINDOW_IN_MILLISECONDS, DEFAULT_CIRCUIT_BREAKER_SLEEP_WINDOW_IN_MILLISECONDS));
		executionIsolationSemaphoreMaxConcurrentRequests = ConfigurationHelper.getIntSystemThenEnvProperty(componentPrefix + "." + CONFIG_EXECUTION_ISOLATION_SEMAPHORE_MAX_CONCURRENT_REQUESTS, ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_EXECUTION_ISOLATION_SEMAPHORE_MAX_CONCURRENT_REQUESTS, DEFAULT_EXECUTION_ISOLATION_SEMAPHORE_MAX_CONCURRENT_REQUESTS));
		executionIsolationThreadInterruptOnTimeout = ConfigurationHelper.getBooleanSystemThenEnvProperty(componentPrefix + "." + CONFIG_EXECUTION_ISOLATION_THREAD_INTERRUPT_ON_TIMEOUT, ConfigurationHelper.getBooleanSystemThenEnvProperty(CONFIG_EXECUTION_ISOLATION_THREAD_INTERRUPT_ON_TIMEOUT, DEFAULT_EXECUTION_ISOLATION_THREAD_INTERRUPT_ON_TIMEOUT));
		executionIsolationThreadTimeoutInMilliseconds = ConfigurationHelper.getIntSystemThenEnvProperty(componentPrefix + "." + CONFIG_EXECUTION_ISOLATION_THREAD_TIMEOUT_IN_MILLISECONDS, ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_EXECUTION_ISOLATION_THREAD_TIMEOUT_IN_MILLISECONDS, DEFAULT_EXECUTION_ISOLATION_THREAD_TIMEOUT_IN_MILLISECONDS));
		executionTimeoutInMilliseconds = ConfigurationHelper.getIntSystemThenEnvProperty(componentPrefix + "." + CONFIG_EXECUTION_TIMEOUT_IN_MILLISECONDS, ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_EXECUTION_TIMEOUT_IN_MILLISECONDS, DEFAULT_EXECUTION_TIMEOUT_IN_MILLISECONDS));
		executionTimeoutEnabled = ConfigurationHelper.getBooleanSystemThenEnvProperty(componentPrefix + "." + CONFIG_EXECUTION_TIMEOUT_ENABLED, ConfigurationHelper.getBooleanSystemThenEnvProperty(CONFIG_EXECUTION_TIMEOUT_ENABLED, DEFAULT_EXECUTION_TIMEOUT_ENABLED));
		fallbackIsolationSemaphoreMaxConcurrentRequests = ConfigurationHelper.getIntSystemThenEnvProperty(componentPrefix + "." + CONFIG_FALLBACK_ISOLATION_SEMAPHORE_MAX_CONCURRENT_REQUESTS, ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_FALLBACK_ISOLATION_SEMAPHORE_MAX_CONCURRENT_REQUESTS, DEFAULT_FALLBACK_ISOLATION_SEMAPHORE_MAX_CONCURRENT_REQUESTS));
		fallbackEnabled = ConfigurationHelper.getBooleanSystemThenEnvProperty(componentPrefix + "." + CONFIG_FALLBACK_ENABLED, ConfigurationHelper.getBooleanSystemThenEnvProperty(CONFIG_FALLBACK_ENABLED, DEFAULT_FALLBACK_ENABLED));
		metricsHealthSnapshotIntervalInMilliseconds = ConfigurationHelper.getIntSystemThenEnvProperty(componentPrefix + "." + CONFIG_METRICS_HEALTH_SNAPSHOT_INTERVAL_IN_MILLISECONDS, ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_METRICS_HEALTH_SNAPSHOT_INTERVAL_IN_MILLISECONDS, DEFAULT_METRICS_HEALTH_SNAPSHOT_INTERVAL_IN_MILLISECONDS));
		metricsRollingPercentileBucketSize = ConfigurationHelper.getIntSystemThenEnvProperty(componentPrefix + "." + CONFIG_METRICS_ROLLING_PERCENTILE_BUCKET_SIZE, ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_METRICS_ROLLING_PERCENTILE_BUCKET_SIZE, DEFAULT_METRICS_ROLLING_PERCENTILE_BUCKET_SIZE));
		metricsRollingPercentileEnabled = ConfigurationHelper.getBooleanSystemThenEnvProperty(componentPrefix + "." + CONFIG_METRICS_ROLLING_PERCENTILE_ENABLED, ConfigurationHelper.getBooleanSystemThenEnvProperty(CONFIG_METRICS_ROLLING_PERCENTILE_ENABLED, DEFAULT_METRICS_ROLLING_PERCENTILE_ENABLED));
		metricsRollingPercentileWindowInMilliseconds = ConfigurationHelper.getIntSystemThenEnvProperty(componentPrefix + "." + CONFIG_METRICS_ROLLING_PERCENTILE_WINDOW_IN_MILLISECONDS, ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_METRICS_ROLLING_PERCENTILE_WINDOW_IN_MILLISECONDS, DEFAULT_METRICS_ROLLING_PERCENTILE_WINDOW_IN_MILLISECONDS));
		metricsRollingPercentileWindowBuckets = ConfigurationHelper.getIntSystemThenEnvProperty(componentPrefix + "." + CONFIG_METRICS_ROLLING_PERCENTILE_WINDOW_BUCKETS, ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_METRICS_ROLLING_PERCENTILE_WINDOW_BUCKETS, DEFAULT_METRICS_ROLLING_PERCENTILE_WINDOW_BUCKETS));
		metricsRollingStatisticalWindowInMilliseconds = ConfigurationHelper.getIntSystemThenEnvProperty(componentPrefix + "." + CONFIG_METRICS_ROLLING_STATISTICAL_WINDOW_IN_MILLISECONDS, ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_METRICS_ROLLING_STATISTICAL_WINDOW_IN_MILLISECONDS, DEFAULT_METRICS_ROLLING_STATISTICAL_WINDOW_IN_MILLISECONDS));
		metricsRollingStatisticalWindowBuckets = ConfigurationHelper.getIntSystemThenEnvProperty(componentPrefix + "." + CONFIG_METRICS_ROLLING_STATISTICAL_WINDOW_BUCKETS, ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_METRICS_ROLLING_STATISTICAL_WINDOW_BUCKETS, DEFAULT_METRICS_ROLLING_STATISTICAL_WINDOW_BUCKETS));
		requestCacheEnabled = ConfigurationHelper.getBooleanSystemThenEnvProperty(componentPrefix + "." + CONFIG_REQUEST_CACHE_ENABLED, ConfigurationHelper.getBooleanSystemThenEnvProperty(CONFIG_REQUEST_CACHE_ENABLED, DEFAULT_REQUEST_CACHE_ENABLED));
		requestLogEnabled = ConfigurationHelper.getBooleanSystemThenEnvProperty(componentPrefix + "." + CONFIG_REQUEST_LOG_ENABLED, ConfigurationHelper.getBooleanSystemThenEnvProperty(CONFIG_REQUEST_LOG_ENABLED, DEFAULT_REQUEST_LOG_ENABLED));
		executionIsolationStrategy = ConfigurationHelper.getEnumSystemThenEnvProperty(ExecutionIsolationStrategy.class, componentPrefix + "." + CONFIG_EXECUTION_ISOLATION_STRATEGY, ConfigurationHelper.getEnumUpperSystemThenEnvProperty(ExecutionIsolationStrategy.class, CONFIG_EXECUTION_ISOLATION_STRATEGY, DEFAULT_EXECUTION_ISOLATION_STRATEGY));
	}

}
