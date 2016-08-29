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
package com.heliosapm.streams.metrics.router.nodes;

import javax.management.ObjectName;

import com.heliosapm.utils.jmx.JMXHelper;

/**
 * <p>Title: AggregatorWindowKey</p>
 * <p>Description: A compound key identifying a unique WindowAggregation instance</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.router.nodes.AggregatorWindowKey</code></p>
 * @param <V> The aggregator type
 * @param <T> The type aggregated by the aggregator
 */

public class AggregatorWindowKey<V extends Aggregator<T>, T> {
	private final long windowDuration;
	private final long retention;
	private final Class<V> aggregatorType;
	private final boolean resetting;
	
	/** The format template for JMX ObjectNames */
	public static final String OBJECT_NAME_TEMPLATE = "com.heliosapm.streams.metrics.router:service=AggWindow,duration=%s,retention=%s,resetting=%s,aggregator=%s";
	
	/**
	 * Creates a new AggregatorWindowKey
	 * @param windowDuration The window duration in seconds
	 * @param resetting true if the window will reset the accumulator after each period
	 * @param retention The idle key retention time
	 * @param aggregatorType The aggregator type
	 */
	AggregatorWindowKey(final long windowDuration, final long retention, final boolean resetting, final Class<V> aggregatorType) {		
		this.windowDuration = windowDuration;
		this.retention = retention;
		this.aggregatorType = aggregatorType;
		this.resetting = resetting;
	}
	
	/**
	 * Creates a JMX ObjectName for this key
	 * @return a JMX ObjectName for this key
	 */
	public ObjectName toObjectName() {
		return JMXHelper.objectName(String.format(OBJECT_NAME_TEMPLATE, windowDuration, retention, resetting, aggregatorType.getSimpleName()));
	}


	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return new StringBuilder("AggregatorWindowKey [d:").append(windowDuration).append(", res:").append(resetting).append(", r:").append(retention).append(", a:" + aggregatorType.getSimpleName() + "]").toString();
	}


	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((aggregatorType == null) ? 0 : aggregatorType.hashCode());
		result = prime * result + (resetting ? 1231 : 1237);
		result = prime * result + (int) (retention ^ (retention >>> 32));
		result = prime * result + (int) (windowDuration ^ (windowDuration >>> 32));
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
		AggregatorWindowKey other = (AggregatorWindowKey) obj;
		if (aggregatorType == null) {
			if (other.aggregatorType != null)
				return false;
		} else if (!aggregatorType.equals(other.aggregatorType))
			return false;
		if (resetting != other.resetting)
			return false;
		if (retention != other.retention)
			return false;
		if (windowDuration != other.windowDuration)
			return false;
		return true;
	}
	
	
	
	
}
