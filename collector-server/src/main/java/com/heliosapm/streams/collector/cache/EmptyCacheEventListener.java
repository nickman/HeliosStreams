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
package com.heliosapm.streams.collector.cache;

/**
 * <p>Title: EmptyCacheEventListener</p>
 * <p>Description: An empty {@link CacheEventListener} useful for extending</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.cache.EmptyCacheEventListener</code></p>
 */

public class EmptyCacheEventListener implements CacheEventListener {

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.cache.CacheEventListener#onValueAdded(java.lang.String, java.lang.Object)
	 */
	@Override
	public void onValueAdded(final String key, final Object value) {
		/* No Op */
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.cache.CacheEventListener#onValueRemoved(java.lang.String, java.lang.Object)
	 */
	@Override
	public void onValueRemoved(final String key, final Object removedValue) {
		/* No Op */
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.cache.CacheEventListener#onValueReplaced(java.lang.String, java.lang.Object, java.lang.Object)
	 */
	@Override
	public void onValueReplaced(final String key, final Object oldValue, final Object newValue) {
		/* No Op */
	}

}
