/**
 * Helios, OpenSource Monitoring
 * Brought to you by the Helios Development Group
 *
 * Copyright 2016, Helios Development Group and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org. 
 *
 */
package com.heliosapm.streams.onramp;

import javax.management.ObjectName;

import com.heliosapm.utils.jmx.JMXHelper;

/**
 * <p>Title: ConnectionManagerMBean</p>
 * <p>Description: JMX MBean interface for {@link ConnectionManager}</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.onramp.ConnectionManagerMBean</code></p>
 */

public interface ConnectionManagerMBean {
	/** The MBean's ObjectName */
	public static final ObjectName OBJECT_NAME = JMXHelper.objectName("com.heliosapm.streams.onramp:service=ConnectionManager");
	
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
