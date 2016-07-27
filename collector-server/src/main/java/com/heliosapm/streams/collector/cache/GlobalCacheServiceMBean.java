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

import java.util.HashMap;
import javax.management.ObjectName;
import com.heliosapm.utils.jmx.JMXHelper;

/**
 * <p>Title: GlobalCacheServiceMBean</p>
 * <p>Description: JMX MBean interface for {@link GlobalCacheService}</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.cache.GlobalCacheServiceMBean</code></p>
 */

public interface GlobalCacheServiceMBean {
	/** The cache event scheduler's JMX ObjectName */
	public static final ObjectName schedulerObjectName = JMXHelper.objectName("com.heliosapm.streams.collector.cache:service=Scheduler,type=CacheEventScheduler");
	/** The global cache service's JMX ObjectName */
	public static final ObjectName objectName = JMXHelper.objectName("com.heliosapm.streams.collector.cache:service=GlobalCacheService");
	
	/** The JMX notification type for a cache add event */
	public static final String NOTIF_ADD_EVENT = "com.heliosapm.collector.cache.add";
	/** The JMX notification type for a cache remove event */
	public static final String NOTIF_REMOVE_EVENT = "com.heliosapm.collector.cache.remove";
	/** The JMX notification type for a cache replace event */
	public static final String NOTIF_REPLACE_EVENT = "com.heliosapm.collector.cache.replace";
	/** The JMX notification type for a cache expiry event */
	public static final String NOTIF_EXPIRE_EVENT = "com.heliosapm.collector.expire";
	
	/**
	 * Returns a map where they key is the cache binding key and the value is the class name of the bound value
	 * @return a map of the bindings
	 */
	public HashMap<String, String> printCacheValues();
	
	/**
	 * Returns the number of items in cache
	 * @return the number of items in cache
	 */
	public int getSize();
	
	/**
	 * Returns the number of expiries
	 * @return the number of expiries
	 */
	public long getExpiryCount();
	
	/**
	 * Returns the number of distinct registered cache event listeners
	 * @return the number of distinct registered cache event listeners
	 */
	public int getListenerCount();
	
	
}
