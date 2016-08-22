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
package com.heliosapm.streams.metrics.processors.impl;

import org.apache.kafka.streams.processor.Processor;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.beans.factory.config.SingletonBeanRegistry;

import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.processors.AbstractStreamedMetricProcessorSupplier;

/**
 * <p>Title: StreamedMetricMeterSupplier</p>
 * <p>Description: Streamed metric processor for metered metrics</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.processors.impl.StreamedMetricMeterSupplier</code></p>
 */

public class StreamedMetricMeterSupplier extends AbstractStreamedMetricProcessorSupplier<String, StreamedMetric, String, StreamedMetric> {
	/** The aggregation period to supply to created StreamedMetricMeter instances  */
	protected int aggregationPeriod = -1;
	/** The timeout period in seconds after which an idle TimestampedMetricKey will be removed from the store */
	protected int idleTimeout = -1;
	
	
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.processors.AbstractStreamedMetricProcessorSupplier#getProcessor()
	 */
	@Override
	protected StreamedMetricMeter getProcessor(final String topicSink, final String[] sources) {		
		return new StreamedMetricMeter(aggregationPeriod, period, maxForwardsWithoutCommit, topicSink, sources, idleTimeout);
	}
	
	/**
	 * Returns the the aggregation period to supply to created StreamedMetricMeter instances
	 * @return the aggregationPeriod
	 */
	public int getAggregationPeriod() {
		return aggregationPeriod;
	}

	/**
	 * Sets the aggregation period to supply to created StreamedMetricMeter instances
	 * @param aggregationPeriod the aggregationPeriod to set
	 */
	@Required
	public void setAggregationPeriod(final int aggregationPeriod) {
		if(aggregationPeriod<1) throw new IllegalArgumentException("Invalid aggregation period: " + aggregationPeriod);
		this.aggregationPeriod = aggregationPeriod;
	}

	/**
	 * Returns the timeout period in seconds after which an idle TimestampedMetricKey will be removed from the store
	 * @return the idle timeout
	 */
	public int getIdleTimeout() {
		return idleTimeout;
	}

	/**
	 * Sets the timeout period in seconds after which an idle TimestampedMetricKey will be removed from the store
	 * @param idleTimeout the idle timeout in seconds
	 */
	public void setIdleTimeout(final int idleTimeout) {
		if(idleTimeout<1) throw new IllegalArgumentException("Invalid idle timeout: " + idleTimeout);
		this.idleTimeout = idleTimeout;
	}

}
