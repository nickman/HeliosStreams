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

import javax.management.ObjectName;

import com.heliosapm.utils.jmx.JMXHelper;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommand.Setter;
import com.netflix.hystrix.HystrixCommandProperties;
import com.netflix.hystrix.HystrixCommandProperties.ExecutionIsolationStrategy;
import com.netflix.hystrix.HystrixThreadPoolProperties;

/**
 * <p>Title: ManagedHystrixCommandFactory</p>
 * <p>Description: Wrapper for {@link HystrixCommandFactory} instances to expose the settings via JMX</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.hystrix.ManagedHystrixCommandFactory</code></p>
 */

public class ManagedHystrixCommandFactory implements ManagedHystrixCommandFactoryMBean {
	protected final HystrixCommand.Setter setter; 
	protected final String key;
	protected final HystrixCommandProperties.Setter commandPropertySetter;
	protected final HystrixThreadPoolProperties.Setter threadPoolPropertySetter;
	protected final ObjectName objectName;
	
	/**
	 * Creates a new ManagedHystrixCommandFactory
	 * @param setter the command setter
	 * @param key The command key
	 * @param commandPropertySetter the command property setter
	 * @param threadPoolPropertySetter the thread pool property setter
	 */
	public ManagedHystrixCommandFactory(final Setter setter, final String key, final HystrixCommandProperties.Setter commandPropertySetter, final HystrixThreadPoolProperties.Setter threadPoolPropertySetter) {		
		this.setter = setter;
		this.key = key;
		this.commandPropertySetter = commandPropertySetter;
		this.threadPoolPropertySetter = threadPoolPropertySetter;
		final HystrixCommand<Object> sampleCommand = new HystrixCommand<Object>(setter) {
			@Override
			protected Object run() throws Exception {
				return null;
			}				
		};		
		ObjectName tmp = null;
		try {
			tmp = JMXHelper.objectName(String.format(OBJECT_NAME_TEMPLATE, sampleCommand.getCommandGroup().name(), sampleCommand.getCommandKey().name(), sampleCommand.getThreadPoolKey().name()));
		} catch (Exception ex) {
			tmp = JMXHelper.objectName(String.format(OBJECT_NAME_TEMPLATE, 
					ObjectName.quote(sampleCommand.getCommandGroup().name()), 
					ObjectName.quote(sampleCommand.getCommandKey().name()), 
					ObjectName.quote(sampleCommand.getThreadPoolKey().name())
			)); 
		}
		objectName = tmp;
	}
	
	/**
	 * Returns the JMX ObjectName for this managed factory
	 * @return the JMX ObjectName for this managed factory
	 */
	public ObjectName getObjectName() {
		return objectName;
	}
	

    /**
     * Returns the command's circuit Breaker Enabled setting
     * @return the command's circuit Breaker Enabled setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getCircuitBreakerEnabled(Boolean)
     */
    public Boolean getCircuitBreakerEnabled() {
         return commandPropertySetter.getCircuitBreakerEnabled();
    }


    /**
     * Returns the command's circuit Breaker Error Threshold Percentage setting
     * @return the command's circuit Breaker Error Threshold Percentage setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getCircuitBreakerErrorThresholdPercentage(Integer)
     */
    public Integer getCircuitBreakerErrorThresholdPercentage() {
         return commandPropertySetter.getCircuitBreakerErrorThresholdPercentage();
    }


    /**
     * Returns the command's circuit Breaker Force Closed setting
     * @return the command's circuit Breaker Force Closed setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getCircuitBreakerForceClosed(Boolean)
     */
    public Boolean getCircuitBreakerForceClosed() {
         return commandPropertySetter.getCircuitBreakerForceClosed();
    }


    /**
     * Returns the command's circuit Breaker Force Open setting
     * @return the command's circuit Breaker Force Open setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getCircuitBreakerForceOpen(Boolean)
     */
    public Boolean getCircuitBreakerForceOpen() {
         return commandPropertySetter.getCircuitBreakerForceOpen();
    }


    /**
     * Returns the command's circuit Breaker Request Volume Threshold setting
     * @return the command's circuit Breaker Request Volume Threshold setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getCircuitBreakerRequestVolumeThreshold(Integer)
     */
    public Integer getCircuitBreakerRequestVolumeThreshold() {
         return commandPropertySetter.getCircuitBreakerRequestVolumeThreshold();
    }


    /**
     * Returns the command's circuit Breaker Sleep Window In Milliseconds setting
     * @return the command's circuit Breaker Sleep Window In Milliseconds setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getCircuitBreakerSleepWindowInMilliseconds(Integer)
     */
    public Integer getCircuitBreakerSleepWindowInMilliseconds() {
         return commandPropertySetter.getCircuitBreakerSleepWindowInMilliseconds();
    }


    /**
     * Returns the command's execution Isolation Semaphore Max Concurrent Requests setting
     * @return the command's execution Isolation Semaphore Max Concurrent Requests setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getExecutionIsolationSemaphoreMaxConcurrentRequests(Integer)
     */
    public Integer getExecutionIsolationSemaphoreMaxConcurrentRequests() {
         return commandPropertySetter.getExecutionIsolationSemaphoreMaxConcurrentRequests();
    }


    /**
     * Returns the command's execution Isolation Strategy setting
     * @return the command's execution Isolation Strategy setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getExecutionIsolationStrategy(hystrix)
     */
    public String getExecutionIsolationStrategy() {
         return commandPropertySetter.getExecutionIsolationStrategy().name();
    }


    /**
     * Returns the command's execution Isolation Thread Interrupt On Timeout setting
     * @return the command's execution Isolation Thread Interrupt On Timeout setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getExecutionIsolationThreadInterruptOnTimeout(Boolean)
     */
    public Boolean getExecutionIsolationThreadInterruptOnTimeout() {
         return commandPropertySetter.getExecutionIsolationThreadInterruptOnTimeout();
    }

    /**
     * Returns the command's execution Timeout In Milliseconds setting
     * @return the command's execution Timeout In Milliseconds setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getExecutionTimeoutInMilliseconds(Integer)
     */
    public Integer getExecutionTimeoutInMilliseconds() {
         return commandPropertySetter.getExecutionTimeoutInMilliseconds();
    }


    /**
     * Returns the command's execution Timeout Enabled setting
     * @return the command's execution Timeout Enabled setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getExecutionTimeoutEnabled(Boolean)
     */
    public Boolean getExecutionTimeoutEnabled() {
         return commandPropertySetter.getExecutionTimeoutEnabled();
    }


    /**
     * Returns the command's fallback Isolation Semaphore Max Concurrent Requests setting
     * @return the command's fallback Isolation Semaphore Max Concurrent Requests setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getFallbackIsolationSemaphoreMaxConcurrentRequests(Integer)
     */
    public Integer getFallbackIsolationSemaphoreMaxConcurrentRequests() {
         return commandPropertySetter.getFallbackIsolationSemaphoreMaxConcurrentRequests();
    }


    /**
     * Returns the command's fallback Enabled setting
     * @return the command's fallback Enabled setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getFallbackEnabled(Boolean)
     */
    public Boolean getFallbackEnabled() {
         return commandPropertySetter.getFallbackEnabled();
    }


    /**
     * Returns the command's metrics Health Snapshot Interval In Milliseconds setting
     * @return the command's metrics Health Snapshot Interval In Milliseconds setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getMetricsHealthSnapshotIntervalInMilliseconds(Integer)
     */
    public Integer getMetricsHealthSnapshotIntervalInMilliseconds() {
         return commandPropertySetter.getMetricsHealthSnapshotIntervalInMilliseconds();
    }


    /**
     * Returns the command's metrics Rolling Percentile Bucket Size setting
     * @return the command's metrics Rolling Percentile Bucket Size setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getMetricsRollingPercentileBucketSize(Integer)
     */
    public Integer getMetricsRollingPercentileBucketSize() {
         return commandPropertySetter.getMetricsRollingPercentileBucketSize();
    }


    /**
     * Returns the command's metrics Rolling Percentile Enabled setting
     * @return the command's metrics Rolling Percentile Enabled setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getMetricsRollingPercentileEnabled(Boolean)
     */
    public Boolean getMetricsRollingPercentileEnabled() {
         return commandPropertySetter.getMetricsRollingPercentileEnabled();
    }


    /**
     * Returns the command's metrics Rolling Percentile Window In Milliseconds setting
     * @return the command's metrics Rolling Percentile Window In Milliseconds setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getMetricsRollingPercentileWindowInMilliseconds(Integer)
     */
    public Integer getMetricsRollingPercentileWindowInMilliseconds() {
         return commandPropertySetter.getMetricsRollingPercentileWindowInMilliseconds();
    }


    /**
     * Returns the command's metrics Rolling Percentile Window Buckets setting
     * @return the command's metrics Rolling Percentile Window Buckets setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getMetricsRollingPercentileWindowBuckets(Integer)
     */
    public Integer getMetricsRollingPercentileWindowBuckets() {
         return commandPropertySetter.getMetricsRollingPercentileWindowBuckets();
    }


    /**
     * Returns the command's metrics Rolling Statistical Window In Milliseconds setting
     * @return the command's metrics Rolling Statistical Window In Milliseconds setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getMetricsRollingStatisticalWindowInMilliseconds(Integer)
     */
    public Integer getMetricsRollingStatisticalWindowInMilliseconds() {
         return commandPropertySetter.getMetricsRollingStatisticalWindowInMilliseconds();
    }


    /**
     * Returns the command's metrics Rolling Statistical Window Buckets setting
     * @return the command's metrics Rolling Statistical Window Buckets setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getMetricsRollingStatisticalWindowBuckets(Integer)
     */
    public Integer getMetricsRollingStatisticalWindowBuckets() {
         return commandPropertySetter.getMetricsRollingStatisticalWindowBuckets();
    }


    /**
     * Returns the command's request Cache Enabled setting
     * @return the command's request Cache Enabled setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getRequestCacheEnabled(Boolean)
     */
    public Boolean getRequestCacheEnabled() {
         return commandPropertySetter.getRequestCacheEnabled();
    }


    /**
     * Returns the command's request Log Enabled setting
     * @return the command's request Log Enabled setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#getRequestLogEnabled(Boolean)
     */
    public Boolean getRequestLogEnabled() {
         return commandPropertySetter.getRequestLogEnabled();
    }


    /**
     * Sets the command's circuit Breaker Enabled setting
     * @param the command's circuit Breaker Enabled setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withCircuitBreakerEnabled(boolean)
     */
    public void setCircuitBreakerEnabled(final Boolean value) {
        if(value==null) throw new IllegalArgumentException("The passed circuit Breaker Enabled was null");
        commandPropertySetter.withCircuitBreakerEnabled(value);
    }


    /**
     * Sets the command's circuit Breaker Error Threshold Percentage setting
     * @param the command's circuit Breaker Error Threshold Percentage setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withCircuitBreakerErrorThresholdPercentage(int)
     */
    public void setCircuitBreakerErrorThresholdPercentage(final Integer value) {
        if(value==null) throw new IllegalArgumentException("The passed circuit Breaker Error Threshold Percentage was null");
        commandPropertySetter.withCircuitBreakerErrorThresholdPercentage(value);
    }


    /**
     * Sets the command's circuit Breaker Force Closed setting
     * @param the command's circuit Breaker Force Closed setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withCircuitBreakerForceClosed(boolean)
     */
    public void setCircuitBreakerForceClosed(final Boolean value) {
        if(value==null) throw new IllegalArgumentException("The passed circuit Breaker Force Closed was null");
        commandPropertySetter.withCircuitBreakerForceClosed(value);
    }


    /**
     * Sets the command's circuit Breaker Force Open setting
     * @param the command's circuit Breaker Force Open setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withCircuitBreakerForceOpen(boolean)
     */
    public void setCircuitBreakerForceOpen(final Boolean value) {
        if(value==null) throw new IllegalArgumentException("The passed circuit Breaker Force Open was null");
        commandPropertySetter.withCircuitBreakerForceOpen(value);
    }


    /**
     * Sets the command's circuit Breaker Request Volume Threshold setting
     * @param the command's circuit Breaker Request Volume Threshold setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withCircuitBreakerRequestVolumeThreshold(int)
     */
    public void setCircuitBreakerRequestVolumeThreshold(final Integer value) {
        if(value==null) throw new IllegalArgumentException("The passed circuit Breaker Request Volume Threshold was null");
        commandPropertySetter.withCircuitBreakerRequestVolumeThreshold(value);
    }


    /**
     * Sets the command's circuit Breaker Sleep Window In Milliseconds setting
     * @param the command's circuit Breaker Sleep Window In Milliseconds setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withCircuitBreakerSleepWindowInMilliseconds(int)
     */
    public void setCircuitBreakerSleepWindowInMilliseconds(final Integer value) {
        if(value==null) throw new IllegalArgumentException("The passed circuit Breaker Sleep Window In Milliseconds was null");
        commandPropertySetter.withCircuitBreakerSleepWindowInMilliseconds(value);
    }


    /**
     * Sets the command's execution Isolation Semaphore Max Concurrent Requests setting
     * @param the command's execution Isolation Semaphore Max Concurrent Requests setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withExecutionIsolationSemaphoreMaxConcurrentRequests(int)
     */
    public void setExecutionIsolationSemaphoreMaxConcurrentRequests(final Integer value) {
        if(value==null) throw new IllegalArgumentException("The passed execution Isolation Semaphore Max Concurrent Requests was null");
        commandPropertySetter.withExecutionIsolationSemaphoreMaxConcurrentRequests(value);
    }


    /**
     * Sets the command's execution Isolation Strategy setting
     * @param the command's execution Isolation Strategy setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withExecutionIsolationStrategy(com.netflix.hystrix.HystrixCommandProperties$ExecutionIsolationStrategy)
     */
    public void setExecutionIsolationStrategy(final String value) {
        if(value==null) throw new IllegalArgumentException("The passed execution Isolation Strategy was null");
        commandPropertySetter.withExecutionIsolationStrategy(ExecutionIsolationStrategy.valueOf(value));
    }


    /**
     * Sets the command's execution Isolation Thread Interrupt On Timeout setting
     * @param the command's execution Isolation Thread Interrupt On Timeout setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withExecutionIsolationThreadInterruptOnTimeout(boolean)
     */
    public void setExecutionIsolationThreadInterruptOnTimeout(final Boolean value) {
        if(value==null) throw new IllegalArgumentException("The passed execution Isolation Thread Interrupt On Timeout was null");
        commandPropertySetter.withExecutionIsolationThreadInterruptOnTimeout(value);
    }

    /**
     * Sets the command's execution Timeout In Milliseconds setting
     * @param the command's execution Timeout In Milliseconds setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withExecutionTimeoutInMilliseconds(int)
     */
    public void setExecutionTimeoutInMilliseconds(final Integer value) {
        if(value==null) throw new IllegalArgumentException("The passed execution Timeout In Milliseconds was null");
        commandPropertySetter.withExecutionTimeoutInMilliseconds(value);
    }


    /**
     * Sets the command's execution Timeout Enabled setting
     * @param the command's execution Timeout Enabled setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withExecutionTimeoutEnabled(boolean)
     */
    public void setExecutionTimeoutEnabled(final Boolean value) {
        if(value==null) throw new IllegalArgumentException("The passed execution Timeout Enabled was null");
        commandPropertySetter.withExecutionTimeoutEnabled(value);
    }


    /**
     * Sets the command's fallback Isolation Semaphore Max Concurrent Requests setting
     * @param the command's fallback Isolation Semaphore Max Concurrent Requests setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withFallbackIsolationSemaphoreMaxConcurrentRequests(int)
     */
    public void setFallbackIsolationSemaphoreMaxConcurrentRequests(final Integer value) {
        if(value==null) throw new IllegalArgumentException("The passed fallback Isolation Semaphore Max Concurrent Requests was null");
        commandPropertySetter.withFallbackIsolationSemaphoreMaxConcurrentRequests(value);
    }


    /**
     * Sets the command's fallback Enabled setting
     * @param the command's fallback Enabled setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withFallbackEnabled(boolean)
     */
    public void setFallbackEnabled(final Boolean value) {
        if(value==null) throw new IllegalArgumentException("The passed fallback Enabled was null");
        commandPropertySetter.withFallbackEnabled(value);
    }


    /**
     * Sets the command's metrics Health Snapshot Interval In Milliseconds setting
     * @param the command's metrics Health Snapshot Interval In Milliseconds setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withMetricsHealthSnapshotIntervalInMilliseconds(int)
     */
    public void setMetricsHealthSnapshotIntervalInMilliseconds(final Integer value) {
        if(value==null) throw new IllegalArgumentException("The passed metrics Health Snapshot Interval In Milliseconds was null");
        commandPropertySetter.withMetricsHealthSnapshotIntervalInMilliseconds(value);
    }


    /**
     * Sets the command's metrics Rolling Percentile Bucket Size setting
     * @param the command's metrics Rolling Percentile Bucket Size setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withMetricsRollingPercentileBucketSize(int)
     */
    public void setMetricsRollingPercentileBucketSize(final Integer value) {
        if(value==null) throw new IllegalArgumentException("The passed metrics Rolling Percentile Bucket Size was null");
        commandPropertySetter.withMetricsRollingPercentileBucketSize(value);
    }


    /**
     * Sets the command's metrics Rolling Percentile Enabled setting
     * @param the command's metrics Rolling Percentile Enabled setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withMetricsRollingPercentileEnabled(boolean)
     */
    public void setMetricsRollingPercentileEnabled(final Boolean value) {
        if(value==null) throw new IllegalArgumentException("The passed metrics Rolling Percentile Enabled was null");
        commandPropertySetter.withMetricsRollingPercentileEnabled(value);
    }


    /**
     * Sets the command's metrics Rolling Percentile Window In Milliseconds setting
     * @param the command's metrics Rolling Percentile Window In Milliseconds setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withMetricsRollingPercentileWindowInMilliseconds(int)
     */
    public void setMetricsRollingPercentileWindowInMilliseconds(final Integer value) {
        if(value==null) throw new IllegalArgumentException("The passed metrics Rolling Percentile Window In Milliseconds was null");
        commandPropertySetter.withMetricsRollingPercentileWindowInMilliseconds(value);
    }


    /**
     * Sets the command's metrics Rolling Percentile Window Buckets setting
     * @param the command's metrics Rolling Percentile Window Buckets setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withMetricsRollingPercentileWindowBuckets(int)
     */
    public void setMetricsRollingPercentileWindowBuckets(final Integer value) {
        if(value==null) throw new IllegalArgumentException("The passed metrics Rolling Percentile Window Buckets was null");
        commandPropertySetter.withMetricsRollingPercentileWindowBuckets(value);
    }


    /**
     * Sets the command's metrics Rolling Statistical Window In Milliseconds setting
     * @param the command's metrics Rolling Statistical Window In Milliseconds setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withMetricsRollingStatisticalWindowInMilliseconds(int)
     */
    public void setMetricsRollingStatisticalWindowInMilliseconds(final Integer value) {
        if(value==null) throw new IllegalArgumentException("The passed metrics Rolling Statistical Window In Milliseconds was null");
        commandPropertySetter.withMetricsRollingStatisticalWindowInMilliseconds(value);
    }


    /**
     * Sets the command's metrics Rolling Statistical Window Buckets setting
     * @param the command's metrics Rolling Statistical Window Buckets setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withMetricsRollingStatisticalWindowBuckets(int)
     */
    public void setMetricsRollingStatisticalWindowBuckets(final Integer value) {
        if(value==null) throw new IllegalArgumentException("The passed metrics Rolling Statistical Window Buckets was null");
        commandPropertySetter.withMetricsRollingStatisticalWindowBuckets(value);
    }


    /**
     * Sets the command's request Cache Enabled setting
     * @param the command's request Cache Enabled setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withRequestCacheEnabled(boolean)
     */
    public void setRequestCacheEnabled(final Boolean value) {
        if(value==null) throw new IllegalArgumentException("The passed request Cache Enabled was null");
        commandPropertySetter.withRequestCacheEnabled(value);
    }


    /**
     * Sets the command's request Log Enabled setting
     * @param the command's request Log Enabled setting
     * @see com.netflix.hystrix.HystrixCommandProperties.Setter#withRequestLogEnabled(boolean)
     */
    public void setRequestLogEnabled(final Boolean value) {
        if(value==null) throw new IllegalArgumentException("The passed request Log Enabled was null");
        commandPropertySetter.withRequestLogEnabled(value);
    }

	@Override
	public void setThreadRollingStatisticalWindowInMilliseconds(Integer value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setThreadRollingStatisticalWindowBuckets(Integer value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setCoreSize(Integer value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setKeepAliveTimeMinutes(Integer value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setMaxQueueSize(Integer value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setQueueSizeRejectionThreshold(Integer value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setExecutionIsolationThreadTimeoutInMilliseconds(Integer value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Integer getExecutionIsolationThreadTimeoutInMilliseconds() {
		// TODO Auto-generated method stub
		return null;
	}

	
	


}
