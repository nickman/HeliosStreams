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
 * <p>Title: CacheEventListener</p>
 * <p>Description:  Defines a listener that is notified of cache events</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.cache.CacheEventListener</code></p>
 */

public interface CacheEventListener {
	/**
	 * Called when a value is added to the cache with a new key
	 * @param key The cache key
	 * @param value The cache value
	 */
	public void onValueAdded(final String key, final Object value);
	/**
	 * Called when a value is removed from the cache
	 * @param key The cache key
	 * @param removedValue The value that was removed
	 */
	public void onValueRemoved(final String key, final Object removedValue);
	/**
	 * Called when a value is added to the cache replacing an existing value
	 * @param key The cache key
	 * @param oldValue The value that was removed
	 * @param newValue The value that was added
	 */
	public void onValueReplaced(final String key, final Object oldValue, final Object newValue);

}
