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

import java.util.concurrent.TimeUnit;

/**
 * <p>Title: Aggregatable</p>
 * <p>Description: Defines an aggregatable type that supplies an effective timestamp and implements some form of aggregation 
 * with another instance of the same type</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.router.nodes.Aggregatable</code></p>
 */

public interface Aggregatable {
	/**
	 * Supplies the effective timestamp for this object
	 * @param unit The unit the timestamp should be expressed in
	 * @return the timestamp in the specified unit
	 */
	public long timestamp(final TimeUnit unit);
	
	/**
	 * Aggregates the {@code #from} instance into this instance
	 * @param from The item to aggregate
	 */
	public void aggregateInto(final Aggregatable from);
}
