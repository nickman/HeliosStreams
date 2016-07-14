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
package com.heliosapm.streams.opentsdb.mocks;

/**
 * <p>Title: UniqueIdRegistryMXBean</p>
 * <p>Description: JMX MXBean interface for the {@link UniqueIdRegistry}</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.opentsdb.mocks.UniqueIdRegistryMXBean</code></p>
 */
public interface UniqueIdRegistryMXBean {

	/**
	 * Returns the Metrics UniqueId cache hits
	 * @return  the Metrics UniqueId cache hits
	 */
	public int getMetricCacheHits();
	
	/**
	 * Returns the Metrics UniqueId cache misses
	 * @return  the Metrics UniqueId cache misses
	 */
	public int getMetricCacheMisses();
	
	/**
	 * Returns the Metrics UniqueId cache size
	 * @return  the Metrics UniqueId cache size
	 */
	public int getMetricCacheSize();
	
	/**
	 * Empties the metric cache
	 */
	public void purgeMetricCache();
	
	/**
	 * Returns the TAGK UniqueId cache hits
	 * @return  the TAGK UniqueId cache hits
	 */
	public int getTagKCacheHits();
	
	/**
	 * Returns the TAGK UniqueId cache misses
	 * @return  the TAGK UniqueId cache misses
	 */
	public int getTagKCacheMisses();
	
	/**
	 * Returns the TAGK UniqueId cache size
	 * @return  the TAGK UniqueId cache size
	 */
	public int getTagKCacheSize();
	
	/**
	 * Empties the tagk cache
	 */
	public void purgeTagKCache();
	
	
	/**
	 * Returns the TAGV UniqueId cache hits
	 * @return  the TAGV UniqueId cache hits
	 */
	public int getTagVCacheHits();
	
	/**
	 * Returns the TAGV UniqueId cache misses
	 * @return  the TAGV UniqueId cache misses
	 */
	public int getTagVCacheMisses();
	
	/**
	 * Returns the TAGV UniqueId cache size
	 * @return  the TAGV UniqueId cache size
	 */
	public int getTagVCacheSize();
	
	/**
	 * Empties the tagv cache
	 */
	public void purgeTagVCache();
	
	/**
	 * Empties all the caches
	 */
	public void purgeAllCaches();
	


}
