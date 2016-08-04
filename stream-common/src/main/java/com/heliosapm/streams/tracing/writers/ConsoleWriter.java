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
package com.heliosapm.streams.tracing.writers;

import java.util.Collection;

import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.tracing.AbstractMetricWriter;

/**
 * <p>Title: ConsoleWriter</p>
 * <p>Description: Writes metrics to standard out</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tracing.writers.ConsoleWriter</code></p>
 */

public class ConsoleWriter extends AbstractMetricWriter {

	/**
	 * Creates a new ConsoleWriter
	 */
	public ConsoleWriter() {
		super(false, true);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.AbstractMetricWriter#doMetrics(java.util.Collection)
	 */
	@Override
	protected void doMetrics(Collection<StreamedMetric> metrics) {
		for(StreamedMetric sm: metrics) {
			System.out.println(sm.toOpenTSDBString());
		}
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.AbstractMetricWriter#doMetrics(com.heliosapm.streams.metrics.StreamedMetric[])
	 */
	@Override
	protected void doMetrics(StreamedMetric... metrics) {
		for(StreamedMetric sm: metrics) {
			System.out.println(sm.toOpenTSDBString());
		}
	}

	/**
	 * {@inheritDoc}
	 * @see com.google.common.util.concurrent.AbstractIdleService#startUp()
	 */
	@Override
	protected void startUp() throws Exception {
		/* No Op */
	}

	/**
	 * {@inheritDoc}
	 * @see com.google.common.util.concurrent.AbstractIdleService#shutDown()
	 */
	@Override
	protected void shutDown() throws Exception {
		/* No Op */
	}

}
