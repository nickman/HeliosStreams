/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.heliosapm.streams.admin.zookeep;

import javax.management.ObjectName;

import com.heliosapm.utils.jmx.JMXHelper;

/**
 * <p>Title: ZooKeepPublisherMBean</p>
 * <p>Description: The JMX MBean interface for the {@link ZooKeepPublisher} instance</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.admin.zookeep.ZooKeepPublisherMBean</code></p>
 */

public interface ZooKeepPublisherMBean {
	/** The ZooKeepPublisher JMX ObjectName */
	public static final ObjectName OBJECT_NAME = JMXHelper.objectName("com.heliosapm.streams.admin:service=ZooKeepPublisher");
	
	/** Notification type emitted when the ZooKeepPublisher connects to ZooKeeper */
	public static final String NOTIF_CONNECTED = "admin.zookeep.connected";
	/** Notification type emitted when the ZooKeepPublisher cleanly disconnects from ZooKeeper */
	public static final String NOTIF_DISCONNECTED = "admin.zookeep.disconnected";
	/** Notification type emitted when the ZooKeepPublisher connection to ZooKeeper expires */
	public static final String NOTIF_EXPIRED = "admin.zookeep.expired";
	
	/**
	 * Returns the advertised URL for admin services
	 * @return the advertised URL for admin services
	 */
	public String getAdminUrl();
	
	/**
	 * Returns the zookeep connect string
	 * @return the zookeep connect string
	 */
	public String getZooConnect();
	
	/**
	 * Indicates if the zookeep connection is active
	 * @return true if the zookeep connection is active, false otherwise
	 */
	public boolean isConnected();


	/**
	 * Returns the zookeep session timeout in ms.
	 * @return the timeout
	 */
	public int getZooTimeout();
	
}
