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

import org.apache.kafka.streams.kstream.KStream;

import com.heliosapm.streams.metrics.StreamedMetricValue;
import com.heliosapm.streams.metrics.ValueType;

/**
 * <p>Title: DefaultValueTypeMetricRouter</p>
 * <p>Description: The default {@link ValueTypeMetricRouter} implementation</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.router.DefaultValueTypeMetricRouter</code></p>
 */

public class DefaultValueTypeMetricRouter implements ValueTypeMetricRouter {
	/** A map of routes keyed by the value type */
	protected final Map<ValueType, String> routes = new EnumMap<ValueType, String>(ValueType.class);
	
	/**
	 * Creates a new DefaultValueTypeMetricRouter
	 * @param routingMap The value type routing map
	 */
	public DefaultValueTypeMetricRouter(final Map<String, String> routingMap) {
		if(routingMap==null || routingMap.isEmpty()) throw new IllegalArgumentException("The passed routing map was null or empty");
		for(Map.Entry<String, String> entry: routingMap.entrySet()) {
			final String key = entry.getKey().trim().toUpperCase();
			try {
				final ValueType v = ValueType.valueOf(key);
				final String dup = routes.put(v, entry.getValue().trim());
				if(dup!=null) throw new IllegalArgumentException("The routing map had a duplicate route for [" + key + "]. First [" + dup + "], then [" + entry.getValue() + "]");
			} catch (Exception ex) {
				throw new IllegalArgumentException("The routing map had an invalid value type key [" + key + "]");
			}
		}
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.router.ValueTypeMetricRouter#route(com.heliosapm.streams.metrics.ValueType, java.lang.String)
	 */
	@Override
	public String route(final ValueType valueType, final String message) {
		return routes.get(valueType);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.router.ValueTypeMetricRouter#route(com.heliosapm.streams.metrics.StreamedMetricValue)
	 */
	@Override
	public String route(StreamedMetricValue metric) {
		// TODO Auto-generated method stub
		return null;
	}

	
}
