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
package com.heliosapm.streams.metrics.router.nodes;

import java.util.concurrent.TimeUnit;

/**
 * <p>Title: Aggregator</p>
 * <p>Description: Defines a class that can aggregate instances of <b><code>T</code></b></p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.router.nodes.Aggregator</code></p>
 * @param <T> The type being aggregated
 */

public interface Aggregator<T> {
	/**
	 * Supplies the effective timestamp for this object
	 * @param unit The unit the timestamp should be expressed in
	 * @param t The instance to get the timestamp from
	 * @return the timestamp in the specified unit
	 */
	public long timestamp(final TimeUnit unit, final T t);
	
	/**
	 * Aggregates the {@code #from} instance into the {@code #to} instance
	 * @param from The aggregate source
	 * @param to The aggregate target
	 */
	public void aggregateInto(final T to, final T from);
	
	

}
