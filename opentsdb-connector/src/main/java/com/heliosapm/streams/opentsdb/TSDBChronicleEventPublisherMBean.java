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

/**
 * <p>Title: TSDBChronicleEventPublisherMBean</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.opentsdb.TSDBChronicleEventPublisherMBean</code></p>
 */

public interface TSDBChronicleEventPublisherMBean {
	
	
	public String getOutQueueDir();

	public int getOutQueueBlockSize();

	public String getOutQueueRollCycle();

	public int getPendingDeleteCount();

	public long getRolledFiles();

	public long getDeletedRolledFiles();

	public long getPendingRolledFiles();

	public String getTsuidCacheDbFile();
	
	public long getTsuidCacheDbFileSize();
	

	public int getAvgKeySize();

	public int getLookupCacheSize();
	
	public int getLookupCacheSegments();
	

	public int getCacheRbThreads();

	public int getDispatchRbThreads();

	public int getCacheRbSize();

	public int getDispatchRbSize();

	public String getCacheRbWaitStrat();

	public String getDispatchRbWaitStrat();

	public long getCacheRbCapacity();

	public long getDispatchRbCapacity();

	public long getDispatchHandleCount();
	
	public double getDispatchHandle1mRate();
	
	public double getDispatchHandle99PctElapsed();
	
	public long getCacheLookupHandleCount();
	
	public double getCacheLookupHandle1mRate();
	
	public double getCacheLookupHandle99PctElapsed();
	
	public long getResolveUidHandleCount();
	
	public double getResolveUidHandle1mRate();
	
	public double getResolveUidHandle99PctElapsed();
	
	public void clearLookupCache();
	
	public long getDispatchExceptionCount();
	
	public long getCacheLookupExceptionCount();
	
	public int getOutQueueFileCount();
	
	public long getOutQueueFileSize();
	
	

}
