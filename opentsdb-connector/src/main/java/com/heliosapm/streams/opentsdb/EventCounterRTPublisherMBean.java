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
 * <p>Title: EventCounterRTPublisherMBean</p>
 * <p>Description: JMX MBean interface for EventCounterRTPublisher instance</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.opentsdb.EventCounterRTPublisherMBean</code></p>
 */

public interface EventCounterRTPublisherMBean {

	/**
	 * Returns the rate of events in the last minute
	 * @return the rate of events in the last minute
	 */
	public double getTotalOneMinuteRate();
	
	/**
	 * Returns the rate of datapoints in the last minute
	 * @return the rate of datapoints in the last minute
	 */
	public double getDataPointsOneMinuteRate();
	
	/**
	 * Returns the rate of long datapoints in the last minute
	 * @return the rate of long datapoints in the last minute
	 */
	public double getLongDataPointsOneMinuteRate();
	
	/**
	 * Returns the rate of double datapoints in the last minute
	 * @return the rate of double datapoints in the last minute
	 */
	public double getDoubleDataPointsOneMinuteRate();
	
	/**
	 * Returns the rate of annotations in the last minute
	 * @return the rate of annotations in the last minute
	 */
	public double getAnnotationsOneMinuteRate();
}
