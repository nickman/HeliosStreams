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
package com.heliosapm.streams.discovery;

import javax.management.ObjectName;

import com.heliosapm.utils.jmx.JMXHelper;

/**
 * <p>Title: EndpointListenerMBean</p>
 * <p>Description: JMX MBean interface for {@link EndpointListener} instances</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.discovery.EndpointListenerMBean</code></p>
 */

public interface EndpointListenerMBean {
	
	/** The JMX ObjectName for this service */
	public static final ObjectName OBJECT_NAME = JMXHelper.objectName("com.heliosapm.streams.discovery:service=EndpointListener");
	
	/** The prefix for all discovery notification types */
	public static final String NOTIF_PREFIX = "discovery.";
	/** The prefix for all endpoint discovery notification types */
	public static final String NOTIF_ENDPOINT_PREFIX = NOTIF_PREFIX + "endpoint.";
	/** The prefix for all discovery service notification types */
	public static final String NOTIF_SERVICE_PREFIX = NOTIF_PREFIX + "service.";
	
	/** Notification type emitted when the discovery service initially connects to zookeeper */
	public static final String NOTIF_SERVICE_CONNECT = NOTIF_SERVICE_PREFIX + "connected";
	/** Notification type emitted when the discovery service disconnects from zookeeper */
	public static final String NOTIF_SERVICE_DISCONNECT = NOTIF_SERVICE_PREFIX + "disconnected";
	/** Notification type emitted when the discovery service reconnects to zookeeper */
	public static final String NOTIF_SERVICE_RECONNECT = NOTIF_SERVICE_PREFIX + "reconnected";
	
	/** Notification type emitted when the discovery service discovers a new endpoint */
	public static final String NOTIF_ENDPOINT_UP = NOTIF_ENDPOINT_PREFIX + "up";
	/** Notification type emitted when a registered endpoint goes down */
	public static final String NOTIF_ENDPOINT_DOWN = NOTIF_ENDPOINT_PREFIX + "down";
	
	/**
	 * Returns the number of registered local (not JMX) listeners
	 * @return the number of registered local (not JMX) listeners
	 */
	public int getListenerCount();
	
	/**
	 * Returns the zookeeper connect tring
	 * @return the zookeeper connect tring
	 */
	public String getZkConnection();

	/**
	 * Returns the service type 
	 * @return the serviceType
	 */
	public String getServiceType();
	
	/**
	 * Returns the zookeeper connection timeout in ms. 
	 * @return the zookeeper connection timeout in ms.
	 */
	public int getConnectionTimeout();


	/**
	 * Returns the zookeeper session timeout in ms.
	 * @return the zookeeper session timeout in ms.
	 */
	public int getSessionTimeout();


	/**
	 * Indicates if this service is connected to zookeeper
	 * @return true if this service is connected to zookeeper, false otherwise
	 */
	public boolean isConnected();
	
	/**
	 * Returns the total number of endpoint up events
	 * @return the total number of endpoint up events
	 */
	public long getUpEvents();


	/**
	 * Returns the total number of endpoint down events
	 * @return the total number of endpoint down events
	 */
	public long getDownEvents();
	
	
	/**
	 * Returns the number of registered and online endpoints
	 * @return the number of registered and online endpoints
	 */
	public int getRegisteredEndpoints();
	
	
}
