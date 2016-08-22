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

import org.springframework.beans.factory.annotation.Required;

import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.processors.AbstractStreamedMetricProcessorSupplier;



/**
 * <p>Title: StreamedMetricAccumulatorSupplier</p>
 * <p>Description: Streamed metric processor for metered metrics</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.processors.impl.StreamedMetricAccumulatorSupplier</code></p>
 */

public class StreamedMetricAccumulatorSupplier extends AbstractStreamedMetricProcessorSupplier<String, StreamedMetric, String, StreamedMetric> {
	/** The flush period in seconds */
	protected long flushPeriod = 5L;
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.processors.AbstractStreamedMetricProcessorSupplier#getProcessor(java.lang.String, java.lang.String[])
	 */
	@Override
	public StreamedMetricAccumulator getProcessor(final String topicSink, final String[] sources) {
		return new StreamedMetricAccumulator(flushPeriod, maxForwardsWithoutCommit, getStateStoreNames()[0], topicSink, sources);
	}
	
	/**
	 * Returns the frequency of aggregation flushes in seconds
	 * @return the frequency of aggregation flushes
	 */
	public long getFlushPeriod() {
		return flushPeriod;
	}

	/**
	 * Sets the frequency of aggregation flushes in seconds
	 * @param flushPeriod the flush period to set
	 */
	@Required
	public void setFlushPeriod(final long flushPeriod) {
		if(flushPeriod<1) throw new IllegalArgumentException("Invalid flushPeriod period: " + flushPeriod);
		this.flushPeriod = flushPeriod;
	}


}
