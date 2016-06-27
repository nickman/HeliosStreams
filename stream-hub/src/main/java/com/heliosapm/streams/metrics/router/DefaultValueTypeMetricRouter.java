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

import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

import com.heliosapm.streams.metrics.ValueType;
import com.heliosapm.streams.metrics.processor.StraightThroughMetricProcessor;
import com.heliosapm.streams.metrics.processor.StreamedMetricAccumulator;
import com.heliosapm.streams.metrics.processor.StreamedMetricProcessor;

/**
 * <p>Title: DefaultValueTypeMetricRouter</p>
 * <p>Description: The default {@link ValueTypeMetricRouter} implementation</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.router.DefaultValueTypeMetricRouter</code></p>
 */

public class DefaultValueTypeMetricRouter implements ValueTypeMetricRouter {
	/** A map of routes keyed by the value type */
	protected final Map<ValueType, StreamedMetricProcessor> routes = new EnumMap<ValueType, StreamedMetricProcessor>(ValueType.class);
	
	/**
	 * Creates a new DefaultValueTypeMetricRouter
	 */
	public DefaultValueTypeMetricRouter() {
		routes.put(ValueType.A, new StreamedMetricAccumulator(5000, "tsdb.metrics.binary"));
		routes.put(ValueType.S, new StraightThroughMetricProcessor(1000, "tsdb.metrics.binary"));
	}
	
	

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.router.ValueTypeMetricRouter#route(com.heliosapm.streams.metrics.ValueType)
	 */
	@Override
	public StreamedMetricProcessor route(final ValueType valueType, final TopologyBuilder t) {
		StreamedMetricProcessor processor = null;
		switch(valueType) {
			case A:
				processor = new StreamedMetricAccumulator(5000, "tsdb.metrics.binary");
			case D:
				break;
			case M:
				break;
			case P:
				break;
			case S:
				break;
			case X:
				break;
			default:
				break;
			
		}
		if(processor!=null) {
			for(StateStoreSupplier ss: processor.getStateStores()) {
				t.addStateStore(ss, processor.getDataStoreNames());
			}
		}
		return processor;
	}
	
	
}
