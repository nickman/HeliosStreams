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

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import javax.management.ObjectName;

//import org.springframework.beans.factory.NamedBean;

/**
 * <p>Title: ManagedScriptMBean</p>
 * <p>Description: JMX MBean interface for {@link ManagedScript} instances</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.groovy.ManagedScriptMBean</code></p>
 */

public interface ManagedScriptMBean { //extends NamedBean {
	
	/**
	 * Indicates if the hystrix circuit-breaker is enabled for this script
	 * @return true if the hystrix circuit-breaker is enabled, false otherwise
	 */	
	public boolean isHystrixEnabled();
	
	/**
	 * Sets the enabled state of the hystrix circuit-breaker for this script
	 * @param enabled true to enable, false to disable
	 */
	public void setHystrixEnabled(final boolean enabled);


	/**
	 * Closes the groovy class loader and unregisters the MBean
	 * @throws IOException will not be thrown.
	 */
	public void close() throws IOException;
	
	/**
	 * @return the median collect time
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
	public Date getLastCollectionErrorDate();
	
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
	
	/**
	 * Returns this script's current bindings
	 * @return this script's current bindings
	 */
	public Map<String, String> getBindings();
	
	/**
	 * Returns the deployment sequence id
	 * @return the deployment sequence id
	 */
	public int getDeploymentId();
	
	/**
	 * Returns the date of the last completed collection, or null if one has not been collected yet
	 * @return the date of the last completed collection
	 */
	public Date getLastCollectionDate();
	
	/**
	 * Returns the elapsed time in ms. of the last completed collection, or null if one has not been collected yet
	 * @return Returns the elapsed time in ms. of the last completed collection
	 */
	public Long getLastCollectionElapsed();
	
	/**
	 * Returns the pending dependencies that must be satisfied before the script runs
	 * @return the pending dependencies
	 */
	public Set<String> getPendingDependencies();
	
	/**
	 * Returns the current state of this script
	 * @return the current state of this script
	 */
	public String getState();
	
	/**
	 * Returns a map of the values of each field in the script instance
	 * @return a map of the values of each field in the script instance
	 */
	public Map<String, String> printFieldValues();
	
	/**
	 * Returns the schedule period
	 * @return the schedule period
	 */
	public Long getScheduledPeriod();

	/**
	 * Returns the schedule period unit
	 * @return the scheduled period unit
	 */
	public String getScheduledPeriodUnit();

	/**
	 * Returns the original source file
	 * @return the original source file
	 */
	public File getSourceFile();

	/**
	 * Returns the linked file if this is a symbolic link, otherwise returns null
	 * @return the linked file if this is a symbolic link, otherwise returns null
	 */
	public File getLinkedSourceFile();
	
	/**
	 * Returns the original source
	 * @return the original source
	 */
	public String printOriginalSource();
	
	/**
	 * Returns the prejected source
	 * @return the prejected source
	 */
	public String printPrejectedSource();
	
	/**
	 * Indicates if the source was prejected
	 * @return true if the source was prejected, false otherwise
	 */
	public boolean isPrejected();
	
	/**
	 * Returns the compile time for this script in ms.
	 * @return the compile time for this script in ms.
	 */
	public long getCompileTime();
	
	/**
	 * Indicates if this script is scheduled for execution
	 * @return true if this script is scheduled for execution, false otherwise
	 */
	public boolean isScheduleActive();
	
	/**
	 * Returns the elapsed time in seconds until the next time this script is executed,
	 * or null if it is not scheduled for execution. 
	 * @return the elased time in seconds until the next time this script is executed
	 */
	public Long getTimeUntilNextCollect();
	
	/**
	 * Returns the script's JMX ObjectName
	 * @return the script's JMX ObjectName
	 */
	public ObjectName getObjectName();
	
	/**
	 * Pauses a scheduled script
	 */
	public void pause();
	
	/**
	 * Resumes a paused script
	 */
	public void resume();
	
	
	
}
