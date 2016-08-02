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
package com.heliosapm.streams.common.naming;


import javax.management.MBeanNotificationInfo;
import javax.management.Notification;
import javax.management.ObjectName;

import com.heliosapm.utils.jmx.JMXHelper;

/**
 * <p>Title: AgentNameMBean</p>
 * <p>Description: JMX MBean for {@link AgentName}</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.common.naming.AgentNameMBean</code></p>
 */

public interface AgentNameMBean {
	
	/** The StoreService JMX ObjectName */
	public static final ObjectName OBJECT_NAME = JMXHelper.objectName("com.heliosapm.streams:service=AgentName");
	
	
	/** The system property config name for the metric submitting host name  */
	public static final String PROP_HOST_NAME = "helios.id.host";
	/** The system property config name for the metric submitting app name  */
	public static final String PROP_APP_NAME = "helios.id.app";
	/** The system property config name for a system property or env var that specifies the app name.
	 * If the prop value starts with <b><code>e:</code></b>, will inspect the environment, otherwise,
	 * looks for a system prop. 
	 */ 
	public static final String SYSPROP_APP_NAME = "helios.id.app.prop";
	/** The system property config name for reading a remote app name  */
	public static final String REMOTE_PROP_APP_NAME = "remote.helios.id.app";
	/** The system property config name for reading a remote host name  */
	public static final String REMOTE_PROP_HOST_NAME = "remote.helios.id.host";
	
	/** The system property config name for a prop where the value is a JS script that will compute the app name */ 
	public static final String JS_APP_NAME = "helios.id.app.js";
	/** The system property config name for a prop where the value is a JS script that will compute remote app name */ 
	public static final String REMOTE_JS_APP_NAME = "remote.helios.id.app.js";
	/** The system property config name for a prop where the value is a JS script that will compute remote host name */ 
	public static final String REMOTE_JS_HOST_NAME = "remote.helios.id.host.js";
	
	/** The configuration property name for forcing all trace content to lower case  */
	public static final String PROP_FORCE_LOWER_CASE = "helios.trace.lc";
	/** The default force all lower case tracing */
	public static final boolean DEFAULT_FORCE_LOWER_CASE = true;
	/** The configuration property name for only tracing the short host name (rather than the FQN) */
	public static final String PROP_USE_SHORT_HOSTNAMES = "helios.hostname.short";
	/** The default short host name */
	public static final boolean DEFAULT_USE_SHORT_HOSTNAMES = true;
	
	
	/** The name of the file used to lock the metric persistence directory */
	public static final String LOCK_FILE_NAME = ".helios.lock";
	
	/** The system property config name for additional JVM wide standard tags for all submitted metrics
	 * in addition to <b><code>host</code></b> and <b><code>app</code></b>. Format should be comma separated values:
	 * <b><code>key1=val1,key2=val2,keyn=valn</code></b>. 
	 */
	public static final String PROP_EXTRA_TAGS = "helios.tags.extra";
	
    /** The global tag name for the host */
    public static final String HOST_TAG = "host";
    /** The global tag name for the app */
    public static final String APP_TAG = "app";
	
	
	/** Notification type for initial name and host assignment */
	public static final String NOTIF_ASSIGNED = "helios.agentname.assigned";
	/** Notification type for an AgentName app name change */
	public static final String NOTIF_APP_NAME_CHANGE = "helios.agentname.change.name";
	/** Notification type for an AgentName host change */
	public static final String NOTIF_HOST_NAME_CHANGE = "helios.agentname.change.host";
	/** Notification type for an AgentName app and host change */
	public static final String NOTIF_BOTH_NAME_CHANGE = "helios.agentname.change.both";
	
	final MBeanNotificationInfo[] NOTIF_INFOS = new MBeanNotificationInfo[]{
			new MBeanNotificationInfo(new String[]{NOTIF_ASSIGNED}, Notification.class.getName(), "Broadcast when the AgentName is initially assigned"),
			new MBeanNotificationInfo(new String[]{NOTIF_APP_NAME_CHANGE}, Notification.class.getName(), "Broadcast when the AgentName processes an app name change"),
			new MBeanNotificationInfo(new String[]{NOTIF_HOST_NAME_CHANGE}, Notification.class.getName(), "Broadcast when the AgentName processes a host change"),
			new MBeanNotificationInfo(new String[]{NOTIF_BOTH_NAME_CHANGE}, Notification.class.getName(), "Broadcast when the AgentName processes an app and host change")
	};

	
	/**
	 * Returns the current app name
	 * @return the current app name
	 */
	public String getAppName();
	
	/**
	 * Returns the source of the current app name
	 * @return the source of the current app name
	 */
	public String getAppNameSource();
	

	/**
	 * Returns the current host name
	 * @return the current host name
	 */
	public String getHostName();
	
	/**
	 * Returns the source of the current host name
	 * @return the source of the current host name
	 */
	public String getHostNameSource();
	
	
	/**
	 * Updates the AgentName's app name.
	 * If a new name is set, the system property {@link Constants#PROP_APP_NAME}
	 * will be updated and listeners will be notified
	 * @param newAppName The new app name. Ignored if null or empty.
	 */	
	public void resetAppName(String newAppName);
	
	/**
	 * Updates the AgentName's host name.
	 * If a new name is set, the system property {@link Constants#PROP_HOST_NAME}
	 * will be updated and listeners will be notified
	 * @param newHostName The new host name. Ignored if null or empty.
	 */	
	public void resetHostName(String newHostName);
	
	/**
	 * Resets the cached app and host names. If a new name is set, the corresponding
	 * system property {@link Constants#PROP_HOST_NAME} and/or {@link Constants#PROP_APP_NAME}
	 * will be updated. 
	 * @param newAppName The new app name to set. Ignored if null or empty.
	 * @param newHostName The new host name to set. Ignored if null or empty.
	 */
	public void resetNames(String newAppName, String newHostName);

}
