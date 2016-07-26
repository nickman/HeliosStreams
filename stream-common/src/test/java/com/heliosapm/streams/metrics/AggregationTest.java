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
package com.heliosapm.streams.metrics;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.heliosapm.streams.metrics.aggregation.StreamedMetricAggregation;
import com.heliosapm.streams.metrics.store.TestKeyValueStore;
import com.heliosapm.utils.collections.FluentMap;
import com.heliosapm.utils.collections.FluentMap.MapType;

/**
 * <p>Title: AggregationTest</p>
 * <p>Description: Aggregating metrics test</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.AggregationTest</code></p>
 */

public class AggregationTest extends BaseTest {
	protected final TestKeyValueStore<String, StreamedMetricAggregation> store = new TestKeyValueStore<String, StreamedMetricAggregation>(); 
	
	/**
	 * Clears the store
	 */
	@Before
	public void resetStore() {
		store.clear();
	}
	
	
	@Test
	public void simpleTest() {
		final String metricName = "sys.cpu.total";
		final Map<String, String> tags = Collections.unmodifiableSortedMap(FluentMap.newMap(MapType.TREE, String.class, String.class).fput("foo", "bar").fput("sna",  "foo").asMap(TreeMap.class));
		final double initialValue = nextPosInt(100) + nextPosDouble();
		log("Initial Value: [%s]", initialValue);
		final StreamedMetricValue smv = new StreamedMetricValue(initialValue, metricName, tags).setValueType(ValueType.PERIODAGG);
		final StreamedMetricAggregation sma = StreamedMetricAggregation.get(smv, false, 500, TimeUnit.MILLISECONDS, store);
		log(sma);

	}
}
