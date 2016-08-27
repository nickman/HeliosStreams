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
package com.heliosapm.streams.metrics.router.nodes;

import java.util.Set;
import java.util.stream.Stream;

import org.apache.kafka.streams.KeyValue;

/**
 * <p>Title: WindowAggregationAction</p>
 * <p>Description: Defines the action executed in a WindowAggregation</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.router.nodes.WindowAggregationAction</code></p>
 * @param <K> The aggregation type's key
 * @param <V> The aggregation type's value
 */

public interface WindowAggregationAction<K, V> {
	/**
	 * Callback from a WindowAggregation when a time window expires
	 * @param aggregatedStream A stream of all the aggregated key/value pairs
	 * @param expiredKeys A [possibly empty] set of expired keys which will [definitely] be empty if retention is not enabled.
	 */
	public void onExpire(final Stream<KeyValue<K,V>> aggregatedStream, final Set<K> expiredKeys);
}
