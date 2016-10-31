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
package com.heliosapm.streams.metrichub.tsdbclient;

import javax.management.ObjectName;

import com.heliosapm.utils.jmx.JMXHelper;

/**
 * <p>Title: ConnectionManagerMBean</p>
 * <p>Description: JMX MBean interface for the {@link ConnectionManager}</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrichub.tsdbclient.ConnectionManagerMBean</code></p>
 */

public interface ConnectionManagerMBean {
	/** The MBean's ObjectName */
	public static final ObjectName OBJECT_NAME = JMXHelper.objectName("com.heliosapm.streams.metrichub:service=ConnectionManager");
	
	/**
	 * Returns the number of currently established connections
	 * @return the number of currently established connections
	 */
	public long getConnectionsEstablished();

	/**

	/**
	 * Returns the cummulative number of connections established 
	 * @return the cummulative number of connections established
	 */
	public long getConnections();

	/**
	 * Returns the cummulative number of unknow caused exceptions
	 * @return the cummulative number of unknow caused exceptions
	 */
	public long getExceptionsUnknown();

	/**
	 * Returns the cummulative number of closed caused exceptions
	 * @return the cummulative number of closed caused exceptions
	 */
	public long getExceptionsClosed();

	/**
	 * Returns the cummulative number of connection reset caused exceptions
	 * @return the cummulative number of connection reset caused exceptions
	 */
	public long getExceptionsReset();

	/**
	 * Returns the cummulative number of connection timeout caused exceptions
	 * @return the cummulative number of connection timeout caused exceptions
	 */
	public long getExceptionsTimeout();

	/**
	 * Returns the cummulative number of idle connections that were reaped 
	 * @return the cummulative number of idle connections that were reaped
	 */
	public long getIdleTimeout();

	/**
	 * Returns the number of registered channels
	 * @return the number of registered channels
	 */
	public int getChannels();
	
	/**
	 * Terminates all established connections
	 */
	public void terminateAllConnections();	
	

}
