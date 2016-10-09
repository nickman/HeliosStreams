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
package com.heliosapm.streams.tsdb.listener.triggers;

import javax.management.ObjectName;

/**
 * <p>Title: AbstractTriggerMBean</p>
 * <p>Description: JMX MBean interface for {@link AbstractTrigger} </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tsdb.listener.triggers.AbstractTriggerMBean</code></p>
 */

public interface AbstractTriggerMBean {
	/** The JMX notification type for a new host event */
	public static final String NEW_HOST = "h2.event.host";
	/** The JMX notification type for a new agent event */
	public static final String NEW_AGENT = "h2.event.agent";
	/** The JMX notification type for a new metric event */
	public static final String NEW_METRIC = "h2.event.metric.";	
	/**
	 * Returns the number of calls to this trigger
	 * @return the number of calls to this trigger
	 */
	public long getCallCount();
	
	/**
	 * Returns this trigger's JMX {@link ObjectName}
	 * @return this trigger's JMX {@link ObjectName}
	 */
	public ObjectName getOn();

	/**
	 * Returns the schema that this trigger is installed in
	 * @return the schema that this trigger is installed in
	 */
	public String getSchemaName();

	/**
	 * Returns the name of the trigger
	 * @return the name of the trigger
	 */
	public String getTriggerName();

	/**
	 * Returns the table that this trigger is attached to
	 * @return the table that this trigger is attached to
	 */
	public String getTableName();

	/**
	 * Indicates if this trigger is fired before the operation, or after
	 * @return true if this trigger is fired before the operation, false if after
	 */
	public boolean isBefore();

	/**
	 * Returns the bitmask of the operations that this trigger fires on
	 * @return the bitmask of the operations that this trigger fires on
	 */
	public int getType();
	
	/**
	 * Returns the names of the operations that this trigger fires on
	 * @return the names of the operations that this trigger fires on
	 */
	public String getTypeNames();	
	
	/**
	 * Returns the rolling average elapsed time in ns. of trigger ops
	 * @return the rolling average elapsed time in ns. of trigger ops
	 */
	public long getAverageElapsedTimeNs();
	
	/**
	 * Returns the last elapsed time in ns. of trigger ops
	 * @return the last elapsed time in ns. of trigger ops
	 */
	public long getLastElapsedTimeNs();	
	/**
	 * Returns the rolling average elapsed time in ms. of trigger ops
	 * @return the rolling average elapsed time in ms. of trigger ops
	 */
	public long getAverageElapsedTimeMs();
	
	/**
	 * Returns the last elapsed time in ms. of trigger ops
	 * @return the last elapsed time in ms. of trigger ops
	 */
	public long getLastElapsedTimeMs();	
	
	/**
	 * Returns the rolling average elapsed time in us. of trigger ops
	 * @return the rolling average elapsed time in us. of trigger ops
	 */
	public long getAverageElapsedTimeUs();
	
	/**
	 * Returns the last elapsed time in us. of trigger ops
	 * @return the last elapsed time in us. of trigger ops
	 */
	public long getLastElapsedTimeUs();	
	
	/**
	 * Returns the rolling 90th percentile elapsed time in ns.
	 * @return the rolling 90th percentile elapsed time in ns.
	 */
	public long get90PercentileElapsedTimeNs();	
}
