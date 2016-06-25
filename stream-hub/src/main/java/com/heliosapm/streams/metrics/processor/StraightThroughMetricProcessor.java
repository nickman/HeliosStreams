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
package com.heliosapm.streams.metrics.processor;

import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.ValueType;

/**
 * <p>Title: StraightThroughMetricProcessor</p>
 * <p>Description: Passes metrics stright through to OpenTSDB</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.processor.StraightThroughMetricProcessor</code></p>
 */

public class StraightThroughMetricProcessor extends AbstractStreamedMetricProcessor {

	/**
	 * Creates a new StraightThroughMetricProcessor
	 * @param period The frequency of write commits
	 * @param sink The topic to write to
	 */
	public StraightThroughMetricProcessor(final long period, final String sink) {
		super(ValueType.S, period, sink);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.processor.AbstractStreamedMetricProcessor#doProcess(java.lang.String, com.heliosapm.streams.metrics.StreamedMetric)
	 */
	@Override
	protected void doProcess(final String key, final StreamedMetric value) {
		context.forward(key, value);		
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.processor.AbstractStreamedMetricProcessor#punctuate(long)
	 */
	@Override
	public void punctuate(final long timestamp) {
		context.commit();
		super.punctuate(timestamp);
	}

}
