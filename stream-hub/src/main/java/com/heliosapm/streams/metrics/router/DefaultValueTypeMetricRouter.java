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
package com.heliosapm.streams.metrics.router;

import java.util.EnumMap;
import java.util.Map;

import org.apache.kafka.streams.processor.Processor;

import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.StreamedMetricValue;
import com.heliosapm.streams.metrics.ValueType;
import com.heliosapm.streams.metrics.processor.StreamedMetricAccumulator;

/**
 * <p>Title: DefaultValueTypeMetricRouter</p>
 * <p>Description: The default {@link ValueTypeMetricRouter} implementation</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.router.DefaultValueTypeMetricRouter</code></p>
 */

public class DefaultValueTypeMetricRouter implements ValueTypeMetricRouter {
	/** A map of routes keyed by the value type */
	protected final Map<ValueType, Processor<String, StreamedMetric>> routes = new EnumMap<ValueType, Processor<String, StreamedMetric>>(ValueType.class);
	
	/**
	 * Creates a new DefaultValueTypeMetricRouter
	 */
	public DefaultValueTypeMetricRouter() {
		routes.put(ValueType.A, new StreamedMetricAccumulator(5000, ""));
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.router.ValueTypeMetricRouter#route(com.heliosapm.streams.metrics.StreamedMetricValue)
	 */
	@Override
	public Processor<String, StreamedMetric> route(final StreamedMetric metric) {		
		return routes.get(metric.getValueType());
	}


	
}
