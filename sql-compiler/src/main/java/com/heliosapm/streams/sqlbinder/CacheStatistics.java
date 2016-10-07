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

import javax.management.ObjectName;
import com.google.common.cache.Cache;



/**
 * <p>Title: CacheStatistics</p>
 * <p>Description: JMX stats for Google Guava Caches</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.sqlbinder.CacheStatistics</code></p>
 */
public class CacheStatistics implements CacheStatisticsMBean {
	/** The wrapped cache instance */
	protected final Cache<?, ?> cache;	
	/** The cache stats JMX object name */
	protected final ObjectName objectName;


	/**
	 * Creates a new CacheStatistics
	 * @param cache The guava cache instance to wrap
	 * @param objectName The assigned JMX ObjectName for this cache
	 */
	public CacheStatistics(final Cache<?, ?> cache, final ObjectName objectName) {
		this.cache = cache;
		this.objectName = objectName;		
	}

	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.CacheStatisticsMBean.CacheStatisticsMXBean#invalidateAll()
	 */
	@Override
	public void invalidateAll() {
		cache.invalidateAll();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.CacheStatisticsMBean.CacheStatisticsMXBean#cleanup()
	 */
	@Override
	public void cleanup() {
		cache.cleanUp();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.CacheStatisticsMBean.CacheStatisticsMXBean#getSize()
	 */
	@Override
	public long getSize() {
		return cache.size();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.CacheStatisticsMBean.CacheStatisticsMXBean#getRequestCount()
	 */
	@Override
	public long getRequestCount() {
		return cache.stats().requestCount();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.CacheStatisticsMBean.CacheStatisticsMXBean#getHitCount()
	 */
	@Override
	public long getHitCount() {
		return cache.stats().hitCount();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.CacheStatisticsMBean.CacheStatisticsMXBean#getHitRate()
	 */
	@Override
	public double getHitRate() {
		return cache.stats().hitRate();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.CacheStatisticsMBean.CacheStatisticsMXBean#getMissCount()
	 */
	@Override
	public long getMissCount() {
		return cache.stats().missCount();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.CacheStatisticsMBean.CacheStatisticsMXBean#getMissRate()
	 */
	@Override
	public double getMissRate() {
		return cache.stats().missRate();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.CacheStatisticsMBean.CacheStatisticsMXBean#getLoadCount()
	 */
	@Override
	public long getLoadCount() {
		return cache.stats().loadCount();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.CacheStatisticsMBean.CacheStatisticsMXBean#getLoadSuccessCount()
	 */
	@Override
	public long getLoadSuccessCount() {
		return cache.stats().loadSuccessCount();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.CacheStatisticsMBean.CacheStatisticsMXBean#getLoadExceptionCount()
	 */
	@Override
	public long getLoadExceptionCount() {
		return cache.stats().loadExceptionCount();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.CacheStatisticsMBean.CacheStatisticsMXBean#getLoadExceptionRate()
	 */
	@Override
	public double getLoadExceptionRate() {
		return cache.stats().loadExceptionRate();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.CacheStatisticsMBean.CacheStatisticsMXBean#getTotalLoadTime()
	 */
	@Override
	public long getTotalLoadTime() {
		return cache.stats().totalLoadTime();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.CacheStatisticsMBean.CacheStatisticsMXBean#getAverageLoadPenalty()
	 */
	@Override
	public double getAverageLoadPenalty() {
		return cache.stats().averageLoadPenalty();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.CacheStatisticsMBean.CacheStatisticsMXBean#getEvictionCount()
	 */
	@Override
	public long getEvictionCount() {
		return cache.stats().evictionCount();
	}

}
