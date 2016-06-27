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

import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.ValueType;

/**
 * <p>Title: ValueTypeMetricRouter</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.router.ValueTypeMetricRouter</code></p>
 */

public interface ValueTypeMetricRouter {
//	/**
//	 * Routes the passed message according to the value type
//	 * @param valueType The value type of the message
//	 * @param message The message to route
//	 * @return the name of topic to route to
//	 */
//	public String route(ValueType valueType, String message);
	
	/**
	 * Determines the name of the topic to route the passed metric to
	 * @param valueType The value type to route
	 * @return the name of the topic to route to
	 */
	public ProcessorSupplier<String, StreamedMetric> route(ValueType valueType, TopologyBuilder t);
	
	
}
