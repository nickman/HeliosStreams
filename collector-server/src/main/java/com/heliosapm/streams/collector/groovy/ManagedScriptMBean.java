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
package com.heliosapm.streams.collector.groovy;

import java.io.IOException;
import java.util.Date;

import javax.management.MBeanRegistration;

/**
 * <p>Title: ManagedScriptMBean</p>
 * <p>Description: JMX MBean interface for {@link ManagedScript} instances</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.groovy.ManagedScriptMBean</code></p>
 */

public interface ManagedScriptMBean extends MBeanRegistration {
	/**
	 * Closes the groovy class loader and unregisters the MBean
	 */
	public void close() throws IOException;
	
	/**
	 * @return
	 * @see com.codahale.metrics.Snapshot#getMedian()
	 */
	public double getMedianCollectTime();

	/**
	 * @return
	 * @see com.codahale.metrics.Snapshot#get75thPercentile()
	 */
	public double get75PctCollectTime();

	/**
	 * @return
	 * @see com.codahale.metrics.Snapshot#get95thPercentile()
	 */
	public double get95PctCollectTime();

	/**
	 * @return
	 * @see com.codahale.metrics.Snapshot#get98thPercentile()
	 */
	public double get98PctCollectTime();

	/**
	 * @return
	 * @see com.codahale.metrics.Snapshot#get99thPercentile()
	 */
	public double get99PctCollectTime();

	/**
	 * @return
	 * @see com.codahale.metrics.Snapshot#get999thPercentile()
	 */
	public double get999PctCollectTime();

	/**
	 * @return
	 * @see com.codahale.metrics.Snapshot#getMax()
	 */
	public long getMaxCollectTime();

	/**
	 * @return
	 * @see com.codahale.metrics.Snapshot#getMean()
	 */
	public double getMeanCollectTime();

	/**
	 * @return
	 * @see com.codahale.metrics.Snapshot#getMin()
	 */
	public long getMinCollectTime();
	
	/**
	 * Returns the total number of completed collections
	 * @return the total number of completed collections
	 */
	public long getCollectionCount();
	
	/**
	 * Returns the time of the last collection error, or null if one has never occurred
	 * @return the time of the last collection error
	 */
	public Date getLastCollectionErrorTime();
	
	/**
	 * Returns the number of consecutive errors since the last successful attempt (or start)
	 * @return the number of consecutive errors since the last successful attempt
	 */
	public long getConsecutiveCollectionErrors();

	/**
	 * Returns the total number of collection errors
	 * @return the total number of collection errors
	 */
	public long getTotalCollectionErrors();	
}
