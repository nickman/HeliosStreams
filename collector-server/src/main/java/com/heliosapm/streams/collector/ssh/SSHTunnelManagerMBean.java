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
package com.heliosapm.streams.collector.ssh;

import java.net.URL;

import javax.management.ObjectName;

import com.heliosapm.utils.jmx.JMXHelper;

/**
 * <p>Title: SSHTunnelManagerMBean</p>
 * <p>Description: The JMX MBean interface for the {@link SSHTunnelManager} instance</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.ssh.SSHTunnelManagerMBean</code></p>
 */

public interface SSHTunnelManagerMBean {
	
	/** The JMX ObjectName this MBean is registered with */
	public static final ObjectName OBJECT_NAME = JMXHelper.objectName("com.heliosapm.ssh:service=SSHTunnelManager");
	
	
}
