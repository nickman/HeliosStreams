/*
 * Copyr	ight 2015 the original author or authors.
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

import java.util.concurrent.TimeUnit;

import com.heliosapm.streams.metrics.StreamedMetric;

/**
 * <p>Title: StreamedMetricMeterAggregator</p>
 * <p>Description: A metering aggregator to count instances of {@link StreamedMetric}s by key </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.router.nodes.StreamedMetricMeterAggregator</code></p>
 */

public class StreamedMetricMeterAggregator implements Aggregator<StreamedMetric> {
	/** A sharable StreamedMetricMeterAggregator instance */
	public static final StreamedMetricMeterAggregator AGGREGATOR = new StreamedMetricMeterAggregator();
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.router.nodes.Aggregator#timestamp(java.util.concurrent.TimeUnit, java.lang.Object)
	 */
	@Override
	public long timestamp(final TimeUnit unit, final StreamedMetric t) {
		return unit.convert(t.getTimestamp(), TimeUnit.MILLISECONDS);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.router.nodes.Aggregator#aggregateInto(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void aggregateInto(final StreamedMetric to, final StreamedMetric from) {
		try {
			to.forceToLong().increment(from.forValue(1L).getValueNumber().longValue());			
		} catch (Exception x) {
			x.printStackTrace(System.err);
		}
	}


}
