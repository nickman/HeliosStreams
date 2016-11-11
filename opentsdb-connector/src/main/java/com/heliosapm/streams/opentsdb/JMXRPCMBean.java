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
package com.heliosapm.streams.opentsdb;

import java.util.List;
import java.util.Map;

import javax.management.ObjectName;

import com.heliosapm.utils.jmx.JMXHelper;

import net.opentsdb.uid.NoSuchUniqueId;

/**
 * <p>Title: JMXRPCMBean</p>
 * <p>Description: JMX MBean interface for {@link JMXRPC}</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.opentsdb.JMXRPCMBean</code></p>
 */

public interface JMXRPCMBean {
	/** The service JMX ObjectName */
	public static final ObjectName OBJECT_NAME = JMXHelper.objectName("net.opentsdb:service=TSDB");
	/** The config key prefix for the JMX service URLs of the servers to start */
	public static final String CONFIG_JMX_URLS = "jmxrpc.jmxurls.";
	/** The default JMX service URLs of the servers to start */
	public static final String DEFAULT_JMX_URLS  = "service:jmx:jmxmp://0.0.0.0:4245";
	/** The config key for the default timeout in ms. for all async calls */
	public static final String CONFIG_ASYNC_TIMEOUT = "jmxrpc.async.timeout";
	/** The default JMX service URLs of the servers to start */
	public static final long DEFAULT_ASYNC_TIMEOUT  = 5000;
	
	/**
	 * Returns the default timeout in ms.
	 * @return the default timeout
	 */
	public long getDefaultTimeout();

	/**
	 * Sets the default timeout in ms.
	 * @param defaultTimeout the default timeout to set
	 */
	public void setDefaultTimeout(long defaultTimeout);
	
	/**
	 * Attempts to find the name for a unique identifier given a type
	 * @param type The type name of UID
	 * @param uid The UID to search for
	 * @param timeout The timeout in ms.
	 * @return The name of the UID object if found
	 * @throws IllegalArgumentException if the type, uid or timeout is not valid
	 * @throws NoSuchUniqueId if the UID was not found
	 * @see net.opentsdb.core.TSDB#getUidName(net.opentsdb.uid.UniqueId.UniqueIdType, byte[])
	 */
	public String getUidName(final String type, final byte[] uid, final long timeout);

	/**
	 * Attempts to find the name for a unique identifier given a type using the default timeout
	 * @param type The type name of UID
	 * @param uid The UID to search for
	 * @return The name of the UID object if found
	 * @throws IllegalArgumentException if the type, uid or timeout is not valid
	 * @throws NoSuchUniqueId if the UID was not found
	 * @see net.opentsdb.core.TSDB#getUidName(net.opentsdb.uid.UniqueId.UniqueIdType, byte[])
	 */
	public String getUidName(final String type, final byte[] uid);
	
	
	/**
	 * Updates a UIDMeta's display name with the default timeout
	 * @param type The type name of the UID
	 * @param uid The UIDMeta's UID
	 * @param displayName The new display name
	 */
	public void updateUIDDisplayName(final String type, final String uid, final String displayName);
	
	/**
	 * Updates a UIDMeta's display name
	 * @param type The type name of the UID
	 * @param uid The UIDMeta's UID
	 * @param displayName The new display name
	 * @param timeout The timeout in ms.
	 */
	public void updateUIDDisplayName(final String type, final String uid, final String displayName, final long timeout);
	
	/**
	 * Flushes the internal TSDB caches
	 */
	public void dropCaches();
	
	/**
	 * Returns the cummulative number of UID cache hits
	 * @return the cummulative number of UID cache hits
	 */
	public int getUidCacheHits();

	/**
	 * Returns the cummulative number of UID cache misses
	 * @return the cummulative number of UID cache misses
	 */
	public int getUidCacheMisses();

	/**
	 * Returns the UID cache size
	 * @return the UID cache size
	 */
	public int getUidCacheSize();
	
	public void flush()  throws Exception;
	
	public Map<String, String> getConfig();
	
	public int get99thPutLatency();
	
	public int get95thPutLatency();
	
	public int get75thPutLatency();
	
	public int get50thPutLatency();
	
	public int get99thScanLatency();
	
	public int get95thScanLatency();
	
	public int get75thScanLatency();
	
	public int get50thScanLatency();
	
	public void shutdownTSD() throws Exception;
	
	public List<String> suggestMetrics(String search);
	
	public List<String> suggestMetrics(String search, int max_results);
	
	public List<String> suggestTagNames(String search);
	
	public List<String> suggestTagNames(String search, int max_results);
	
	public List<String> suggestTagValues(String search);
	
	public List<String> suggestTagValues(String search, int max_results);
	
	
	
	
	
	
}
