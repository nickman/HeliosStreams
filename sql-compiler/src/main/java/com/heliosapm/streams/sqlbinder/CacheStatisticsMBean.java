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
package com.heliosapm.streams.sqlbinder;

/**
 * <p>Title: CacheStatisticsMBean</p>
 * <p>Description: JMX MXBean interface for {@link CacheStatistics}</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.sqlbinder.CacheStatisticsMBean</code></p>
 */
public interface CacheStatisticsMBean {
	
	/**
	 * Discards all entries in the cache.
	 */
	public void invalidateAll();
	
	/**
	 * Performs any pending maintenance operations needed by the cache. 
	 */
	public void cleanup();	
	
	/**
	 * Returns the approximate number of entries in this cache.
	 * @return the approximate number of entries in this cache.
	 */
	public long getSize();
	
	/**
	 * Returns the number of times Cache lookup methods have returned either a cached or uncached value.
	 * @return the cache request count
	 */
	public long getRequestCount();

	/**
	 * Returns the number of times Cache lookup methods have returned a cached value. 
	 * @return the cache hit count
	 */
	public long getHitCount();

	/**
	 * Returns the ratio of cache requests which were hits. This is defined as hitCount / requestCount, or 1.0 when requestCount == 0. Note that hitRate + missRate =~ 1.0. 
	 * @return the cache hit rate
	 */
	public double getHitRate();

	/**
	 * Returns the number of times Cache lookup methods have returned an uncached (newly loaded) value, or null. Multiple concurrent calls to Cache lookup methods on an absent value can result in multiple misses, all returning the results of a single cache load operation. 
	 * @return the cache miss count
	 */
	public long getMissCount();

	/**
	 * Returns the ratio of cache requests which were misses. 
	 * This is defined as missCount / requestCount, or 0.0 when requestCount == 0. 
	 * Note that hitRate + missRate =~ 1.0. 
	 * Cache misses include all requests which weren't cache hits, including requests which 
	 * resulted in either successful or failed loading attempts, 
	 * and requests which waited for other threads to finish loading. 
	 * It is thus the case that missCount &gt;= loadSuccessCount + loadExceptionCount. 
	 * Multiple concurrent misses for the same key will result in a single load operation. 
	 * @return the cache miss rate
	 */
	public double getMissRate();

	/**
	 * Returns the total number of times that Cache lookup methods attempted to load new values.
	 * @return the cache load count
	 */
	public long getLoadCount();

	/**
	 * Returns the number of times Cache lookup methods have successfully loaded a new value.
	 * @return the cache successful load count
	 */
	public long getLoadSuccessCount();

	/**
	 * Returns the number of times Cache lookup methods threw an exception while loading a new value.
	 * @return the cache load exception count
	 */
	public long getLoadExceptionCount();

	/**
	 * Returns the ratio of cache loading attempts which threw exceptions.
	 * @return the cache load exception rate
	 */
	public double getLoadExceptionRate();

	/**
	 * Returns the total number of nanoseconds the cache has spent loading new values.
	 * @return the cache load time
	 */
	public long getTotalLoadTime();

	/**
	 * Returns the average time spent loading new values.
	 * @return the cache average load time
	 */
	public double getAverageLoadPenalty();

	/**
	 * Returns the number of times an entry has been evicted.
	 * @return the cache eviction count
	 */
	public long getEvictionCount();


}
