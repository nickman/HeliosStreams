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
package com.heliosapm.streams.metrics;

import javax.management.ObjectName;

import com.heliosapm.utils.jmx.JMXHelper;

/**
 * <p>Title: BlacklistMBean</p>
 * <p>Description: JMX MBean interface for the {@link Blacklist} instance</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.BlacklistMBean</code></p>
 */

public interface BlacklistMBean {
	
	/** The JMX notification type for a new blacklisted metric */
	public static final String NOTIF_NEW_BLACKLIST = "com.heliosapm.streams.metrics.blacklisted";
	
	/** The JMX ObjectName for this MBean */
	public static final ObjectName OBJECT_NAME = JMXHelper.objectName("com.heliosapm.streams:service=Blacklist,type=Metrics");
	
	/**
	 * Adds the passed metric key to the blacklist
	 * @param key The metric key to blacklist
	 */
	public void blackList(final String key);
	
	/**
	 * Determines if the passed metric key has been blacklisted 
	 * and increments the incident count if it has
	 * @param key The metric key to test
	 * @return true if the metric key has been blacklisted, false otherwise
	 */
	public boolean isBlackListed(final String key);
	
	/**
	 * Returns the number of blacklisted metric keys
	 * @return the number of blacklisted metric keys
	 */
	public int getBlacklistedCount();
	
	/**
	 * Returns the total number of observed blacklisted metric instances
	 * @return the total number of observed blacklisted metric instances
	 */
	public long getBlacklistInstances();
	
	/**
	 * Resets the blacklist state and counters
	 */
	public void reset();

}
