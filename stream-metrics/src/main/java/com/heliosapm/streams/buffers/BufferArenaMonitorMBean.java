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
 * <p>Title: BufferArenaMonitorMBean</p>
 * <p>Description: JMX MBean interface for {@link BufferArenaMonitor} instances</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.buffers.BufferArenaMonitorMBean</code></p>
 */

public interface BufferArenaMonitorMBean {
	/**
	 * Returns the number of allocations allocations
	 * @return the number of allocations allocations
	 */
	public long getAllocations();

	/**
	 * Returns the number of tiny allocations
	 * @return the number of tiny allocations
	 */
	public long getTinyAllocations();

	/**
	 * Returns the number of small allocations
	 * @return the number of small allocations
	 */
	public long getSmallAllocations();

	/**
	 * Returns the number of normal allocations
	 * @return the number of normal allocations
	 */
	public long getNormalAllocations();

	/**
	 * Returns the number of huge allocations
	 * @return the number of huge allocations
	 */
	public long getHugeAllocations();

	/**
	 * Returns the number of deallocations allocations
	 * @return the number of deallocations allocations
	 */
	public long getDeallocations();

	/**
	 * Returns the number of tiny allocations
	 * @return the number of tiny allocations
	 */
	public long getTinyDeallocations();

	/**
	 * Returns the number of small allocations
	 * @return the number of small allocations
	 */
	public long getSmallDeallocations();

	/**
	 * Returns the number of normal allocations
	 * @return the number of normal allocations
	 */
	public long getNormalDeallocations();

	/**
	 * Returns the number of huge allocations
	 * @return the number of huge allocations
	 */
	public long getHugeDeallocations();

	/**
	 * Returns the number of active allocations
	 * @return the number of active allocations
	 */
	public long getActiveAllocations();

	/**
	 * Returns the number of activeTiny allocations
	 * @return the number of activeTiny allocations
	 */
	public long getActiveTinyAllocations();

	/**
	 * Returns the number of activeSmall allocations
	 * @return the number of activeSmall allocations
	 */
	public long getActiveSmallAllocations();

	/**
	 * Returns the number of activeNormal allocations
	 * @return the number of activeNormal allocations
	 */
	public long getActiveNormalAllocations();

	/**
	 * Returns the number of activeHuge allocations
	 * @return the number of activeHuge allocations
	 */
	public long getActiveHugeAllocations();
	
	/**
	 * Returns the type of arena
	 * @return the type of arena
	 */
	public String getType();
	
	/**
	 * Indicates if the arena is direct
	 * @return true if the arena is direct, false if it is heap
	 */
	public boolean isDirect();
	
	/**
	 * Returns the number of chunk lists
	 * @return the number of chunk lists
	 */
	public int getChunkLists();

	/**
	 * Returns the number of small subpages
	 * @return the small subpages
	 */
	public int getSmallSubPages();

	/**
	 * Returns the number of tiny subpages
	 * @return the tiny subpages
	 */
	public int getTinySubPages();
	
	/**
	 * Returns the total chunk allocated bytes for this pooled allocator
	 * @return the total chunk allocated bytes for this pooled allocator
	 */
	public long getTotalChunkSize();
	
	/**
	 * Returns the free chunk allocated bytes for this pooled allocator
	 * @return the free chunk allocated bytes for this pooled allocator
	 */
	public long getChunkFreeBytes();
	
	/**
	 * Returns the used chunk allocated bytes for this pooled allocator
	 * @return the used chunk allocated bytes for this pooled allocator
	 */
	public long getChunkUsedBytes();
	
	/**
	 * Return the percentage of the current usage of the total chunk space
	 * @return the percentage of the current usage of the total chunk space
	 */
	public int getChunkUsage();
	
	/**
	 * Returns the elapsed time of the last stats aggregation in ms.
	 * @return the elapsed time of the last stats aggregation in ms.
	 */
	public long getLastElapsed();
	

}
