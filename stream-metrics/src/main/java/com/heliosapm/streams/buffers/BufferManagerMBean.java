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
package com.heliosapm.streams.buffers;


/**
 * <p>Title: BufferManagerMBean</p>
 * <p>Description: JMX MBean interface for {@link BufferManager}</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.buffers.BufferManagerMBean</code></p>
 */

public interface BufferManagerMBean {
	/** The JMX ObjectName string for the BufferManager management interface */
	public static final String OBJECT_NAME = "net.opentsdb:service=BufferManager";
	
	/**
	 * Indicates if the server allocator is using pooled buffers
	 * @return true if the server allocator is using pooled buffers, false otherwise
	 */
	public boolean isPooledBuffers();

	/**
	 * Indicates if the server allocator is using direct buffers
	 * @return true if the server allocator is using direct buffers, false otherwise
	 */
	public boolean isDirectBuffers();

	/**
	 * Returns the number of heap arenas
	 * @return the number of heap arenas
	 */
	public int getHeapArenas();

	/**
	 * Returns the number of direct arenas
	 * @return the number of direct arenas
	 */
	public int getDirectArenas();

	/**
	 * Returns the memory page size for allocated pool buffer pages
	 * @return the memory page size for allocated pool buffer pages
	 */
	public int getPageSize();

	/**
	 * Returns the max order (I don't know what this is) 
	 * @return the max order
	 */
	public int getMaxOrder();

	/**
	 * Returns the size of the cache for tiny buffers
	 * @return the size of the cache for tiny buffers
	 */
	public int getTinyCacheSize();

	/**
	 * Returns the size of the cache for small buffers
	 * @return the size of the cache for small buffers
	 */
	public int getSmallCacheSize();

	/**
	 * Returns the size of the cache for normal buffers
	 * @return the size of the cache for normal buffers
	 */
	public int getNormalCacheSize();

	/**
	 * Returns a formatted report of the buffer stats
	 * @return a formatted report of the buffer stats
	 */
	public String printStats();
	
}
