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

/**
 * <p>Title: ManagedHystrixCommandFactoryMBean</p>
 * <p>Description: JMX MBean interface for registered {@link HystrixCommandFactory} instances</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.hystrix.HystrixCommandFactoryMBean</code></p>
 */

public interface ManagedHystrixCommandFactoryMBean {
	/** The JMX Domain Name */
	public static final String JMX_DOMAIN = "com.heliosapm.streams.hystrix.commands";
	/** The JMX ObjectName template */
	public static final String OBJECT_NAME_TEMPLATE = JMX_DOMAIN + ":group=%s,command=%s,threadpool=%s";
	

    /**
     * Sets the thread pool's metrics Rolling Statistical Window In Milliseconds setting
     * @param value The value to set
     * @see com.netflix.hystrix.HystrixThreadPoolProperties.Setter#withMetricsRollingStatisticalWindowInMilliseconds(int)
     */
    public void setThreadRollingStatisticalWindowInMilliseconds(final Integer value);


    /**
     * Sets the thread pool's metrics Rolling Statistical Window Buckets setting
     * @param value The value to set
     * @see com.netflix.hystrix.HystrixThreadPoolProperties.Setter#withMetricsRollingStatisticalWindowBuckets(int)
     */
    public void setThreadRollingStatisticalWindowBuckets(final Integer value);


    /**
     * Sets the thread pool's core Size setting
     * @param value The value to set
     * @see com.netflix.hystrix.HystrixThreadPoolProperties.Setter#withCoreSize(int)
     */
    public void setCoreSize(final Integer value);


    /**
     * Sets the thread pool's keep Alive Time Minutes setting
     * @param value The value to set
     * @see com.netflix.hystrix.HystrixThreadPoolProperties.Setter#withKeepAliveTimeMinutes(int)
     */
    public void setKeepAliveTimeMinutes(final Integer value);


    /**
     * Sets the thread pool's max Queue Size setting
     * @param value The value to set
     * @see com.netflix.hystrix.HystrixThreadPoolProperties.Setter#withMaxQueueSize(int)
     */
    public void setMaxQueueSize(final Integer value);


    /**
     * Sets the thread pool's queue Size Rejection Threshold setting
     * @param value The value to set
     * @see com.netflix.hystrix.HystrixThreadPoolProperties.Setter#withQueueSizeRejectionThreshold(int)
     */
    public void setQueueSizeRejectionThreshold(final Integer value);

	

    /**
     * Sets the command's circuit Breaker Enabled setting
     * @param value The value to set
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withCircuitBreakerEnabled(boolean)
     */
    public void setCircuitBreakerEnabled(final Boolean value);


    /**
     * Sets the command's circuit Breaker Error Threshold Percentage setting
     * @param value The value to set
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withCircuitBreakerErrorThresholdPercentage(int)
     */
    public void setCircuitBreakerErrorThresholdPercentage(final Integer value);


    /**
     * Sets the command's circuit Breaker Force Closed setting
     * @param value The value to set
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withCircuitBreakerForceClosed(boolean)
     */
    public void setCircuitBreakerForceClosed(final Boolean value);


    /**
     * Sets the command's circuit Breaker Force Open setting
     * @param value The value to set
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withCircuitBreakerForceOpen(boolean)
     */
    public void setCircuitBreakerForceOpen(final Boolean value);


    /**
     * Sets the command's circuit Breaker Request Volume Threshold setting
     * @param value The value to set
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withCircuitBreakerRequestVolumeThreshold(int)
     */
    public void setCircuitBreakerRequestVolumeThreshold(final Integer value);


    /**
     * Sets the command's circuit Breaker Sleep Window In Milliseconds setting
     * @param value The value to set
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withCircuitBreakerSleepWindowInMilliseconds(int)
     */
    public void setCircuitBreakerSleepWindowInMilliseconds(final Integer value);


    /**
     * Sets the command's execution Isolation Semaphore Max Concurrent Requests setting
     * @param value The value to set
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withExecutionIsolationSemaphoreMaxConcurrentRequests(int)
     */
    public void setExecutionIsolationSemaphoreMaxConcurrentRequests(final Integer value);


    /**
     * Sets the command's execution Isolation Strategy setting
     * @param value The value to set
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withExecutionIsolationStrategy(com.netflix.hystrix.HystrixCommandProperties$ExecutionIsolationStrategy)
     */
    public void setExecutionIsolationStrategy(final String value);


    /**
     * Sets the command's execution Isolation Thread Interrupt On Timeout setting
     * @param value The value to set
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withExecutionIsolationThreadInterruptOnTimeout(boolean)
     */
    public void setExecutionIsolationThreadInterruptOnTimeout(final Boolean value);


    /**
     * Sets the command's execution Isolation Thread Timeout In Milliseconds setting
     * @param value The value to set
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withExecutionIsolationThreadTimeoutInMilliseconds(int)
     */
    public void setExecutionIsolationThreadTimeoutInMilliseconds(final Integer value);


    /**
     * Sets the command's execution Timeout In Milliseconds setting
     * @param value The value to set
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withExecutionTimeoutInMilliseconds(int)
     */
    public void setExecutionTimeoutInMilliseconds(final Integer value);


    /**
     * Sets the command's execution Timeout Enabled setting
     * @param value The value to set
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withExecutionTimeoutEnabled(boolean)
     */
    public void setExecutionTimeoutEnabled(final Boolean value);


    /**
     * Sets the command's fallback Isolation Semaphore Max Concurrent Requests setting
     * @param value The value to set
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withFallbackIsolationSemaphoreMaxConcurrentRequests(int)
     */
    public void setFallbackIsolationSemaphoreMaxConcurrentRequests(final Integer value);


    /**
     * Sets the command's fallback Enabled setting
     * @param value The value to set
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withFallbackEnabled(boolean)
     */
    public void setFallbackEnabled(final Boolean value);


    /**
     * Sets the command's metrics Health Snapshot Interval In Milliseconds setting
     * @param value The value to set
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withMetricsHealthSnapshotIntervalInMilliseconds(int)
     */
    public void setMetricsHealthSnapshotIntervalInMilliseconds(final Integer value);


    /**
     * Sets the command's metrics Rolling Percentile Bucket Size setting
     * @param value The value to set
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withMetricsRollingPercentileBucketSize(int)
     */
    public void setMetricsRollingPercentileBucketSize(final Integer value);


    /**
     * Sets the command's metrics Rolling Percentile Enabled setting
     * @param value The value to set
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withMetricsRollingPercentileEnabled(boolean)
     */
    public void setMetricsRollingPercentileEnabled(final Boolean value);


    /**
     * Sets the command's metrics Rolling Percentile Window In Milliseconds setting
     * @param value The value to set
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withMetricsRollingPercentileWindowInMilliseconds(int)
     */
    public void setMetricsRollingPercentileWindowInMilliseconds(final Integer value);


    /**
     * Sets the command's metrics Rolling Percentile Window Buckets setting
     * @param value The value to set
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withMetricsRollingPercentileWindowBuckets(int)
     */
    public void setMetricsRollingPercentileWindowBuckets(final Integer value);


    /**
     * Sets the command's metrics Rolling Statistical Window In Milliseconds setting
     * @param value The value to set
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withMetricsRollingStatisticalWindowInMilliseconds(int)
     */
    public void setMetricsRollingStatisticalWindowInMilliseconds(final Integer value);


    /**
     * Sets the command's metrics Rolling Statistical Window Buckets setting
     * @param value The value to set
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withMetricsRollingStatisticalWindowBuckets(int)
     */
    public void setMetricsRollingStatisticalWindowBuckets(final Integer value);


    /**
     * Sets the command's request Cache Enabled setting
     * @param value The value to set
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withRequestCacheEnabled(boolean)
     */
    public void setRequestCacheEnabled(final Boolean value);


    /**
     * Sets the command's request Log Enabled setting
     * @param value The value to set
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withRequestLogEnabled(boolean)
     */
    public void setRequestLogEnabled(final Boolean value);
    

    /**
     * Returns the command's circuit Breaker Enabled setting
     * @return the command's circuit Breaker Enabled setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getCircuitBreakerEnabled(class Boolean)
     */
    public Boolean getCircuitBreakerEnabled();


    /**
     * Returns the command's circuit Breaker Error Threshold Percentage setting
     * @return the command's circuit Breaker Error Threshold Percentage setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getCircuitBreakerErrorThresholdPercentage(class Integer)
     */
    public Integer getCircuitBreakerErrorThresholdPercentage();


    /**
     * Returns the command's circuit Breaker Force Closed setting
     * @return the command's circuit Breaker Force Closed setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getCircuitBreakerForceClosed(class Boolean)
     */
    public Boolean getCircuitBreakerForceClosed();


    /**
     * Returns the command's circuit Breaker Force Open setting
     * @return the command's circuit Breaker Force Open setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getCircuitBreakerForceOpen(class Boolean)
     */
    public Boolean getCircuitBreakerForceOpen();


    /**
     * Returns the command's circuit Breaker Request Volume Threshold setting
     * @return the command's circuit Breaker Request Volume Threshold setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getCircuitBreakerRequestVolumeThreshold(class Integer)
     */
    public Integer getCircuitBreakerRequestVolumeThreshold();


    /**
     * Returns the command's circuit Breaker Sleep Window In Milliseconds setting
     * @return the command's circuit Breaker Sleep Window In Milliseconds setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getCircuitBreakerSleepWindowInMilliseconds(class Integer)
     */
    public Integer getCircuitBreakerSleepWindowInMilliseconds();


    /**
     * Returns the command's execution Isolation Semaphore Max Concurrent Requests setting
     * @return the command's execution Isolation Semaphore Max Concurrent Requests setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getExecutionIsolationSemaphoreMaxConcurrentRequests(class Integer)
     */
    public Integer getExecutionIsolationSemaphoreMaxConcurrentRequests();


    /**
     * Returns the command's execution Isolation Strategy setting
     * @return the command's execution Isolation Strategy setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getExecutionIsolationStrategy(class com.netflix.hystrix.HystrixCommandProperties$ExecutionIsolationStrategy)
     */
    public String getExecutionIsolationStrategy();


    /**
     * Returns the command's execution Isolation Thread Interrupt On Timeout setting
     * @return the command's execution Isolation Thread Interrupt On Timeout setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getExecutionIsolationThreadInterruptOnTimeout(class Boolean)
     */
    public Boolean getExecutionIsolationThreadInterruptOnTimeout();


    /**
     * Returns the command's execution Isolation Thread Timeout In Milliseconds setting
     * @return the command's execution Isolation Thread Timeout In Milliseconds setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getExecutionIsolationThreadTimeoutInMilliseconds(class Integer)
     */
    public Integer getExecutionIsolationThreadTimeoutInMilliseconds();


    /**
     * Returns the command's execution Timeout In Milliseconds setting
     * @return the command's execution Timeout In Milliseconds setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getExecutionTimeoutInMilliseconds(class Integer)
     */
    public Integer getExecutionTimeoutInMilliseconds();


    /**
     * Returns the command's execution Timeout Enabled setting
     * @return the command's execution Timeout Enabled setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getExecutionTimeoutEnabled(class Boolean)
     */
    public Boolean getExecutionTimeoutEnabled();


    /**
     * Returns the command's fallback Isolation Semaphore Max Concurrent Requests setting
     * @return the command's fallback Isolation Semaphore Max Concurrent Requests setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getFallbackIsolationSemaphoreMaxConcurrentRequests(class Integer)
     */
    public Integer getFallbackIsolationSemaphoreMaxConcurrentRequests();


    /**
     * Returns the command's fallback Enabled setting
     * @return the command's fallback Enabled setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getFallbackEnabled(class Boolean)
     */
    public Boolean getFallbackEnabled();


    /**
     * Returns the command's metrics Health Snapshot Interval In Milliseconds setting
     * @return the command's metrics Health Snapshot Interval In Milliseconds setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getMetricsHealthSnapshotIntervalInMilliseconds(class Integer)
     */
    public Integer getMetricsHealthSnapshotIntervalInMilliseconds();


    /**
     * Returns the command's metrics Rolling Percentile Bucket Size setting
     * @return the command's metrics Rolling Percentile Bucket Size setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getMetricsRollingPercentileBucketSize(class Integer)
     */
    public Integer getMetricsRollingPercentileBucketSize();


    /**
     * Returns the command's metrics Rolling Percentile Enabled setting
     * @return the command's metrics Rolling Percentile Enabled setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getMetricsRollingPercentileEnabled(class Boolean)
     */
    public Boolean getMetricsRollingPercentileEnabled();


    /**
     * Returns the command's metrics Rolling Percentile Window In Milliseconds setting
     * @return the command's metrics Rolling Percentile Window In Milliseconds setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getMetricsRollingPercentileWindowInMilliseconds(class Integer)
     */
    public Integer getMetricsRollingPercentileWindowInMilliseconds();


    /**
     * Returns the command's metrics Rolling Percentile Window Buckets setting
     * @return the command's metrics Rolling Percentile Window Buckets setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getMetricsRollingPercentileWindowBuckets(class Integer)
     */
    public Integer getMetricsRollingPercentileWindowBuckets();


    /**
     * Returns the command's metrics Rolling Statistical Window In Milliseconds setting
     * @return the command's metrics Rolling Statistical Window In Milliseconds setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getMetricsRollingStatisticalWindowInMilliseconds(class Integer)
     */
    public Integer getMetricsRollingStatisticalWindowInMilliseconds();


    /**
     * Returns the command's metrics Rolling Statistical Window Buckets setting
     * @return the command's metrics Rolling Statistical Window Buckets setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getMetricsRollingStatisticalWindowBuckets(class Integer)
     */
    public Integer getMetricsRollingStatisticalWindowBuckets();


    /**
     * Returns the command's request Cache Enabled setting
     * @return the command's request Cache Enabled setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getRequestCacheEnabled(class Boolean)
     */
    public Boolean getRequestCacheEnabled();


    /**
     * Returns the command's request Log Enabled setting
     * @return the command's request Log Enabled setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getRequestLogEnabled(class Boolean)
     */
    public Boolean getRequestLogEnabled();

    

    
}
