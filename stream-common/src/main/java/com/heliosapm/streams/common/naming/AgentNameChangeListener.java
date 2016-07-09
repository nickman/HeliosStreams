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

/**
 * <p>Title: AgentNameChangeListener</p>
 * <p>Description: Defines a listener that is notified when there are changes to the agent name</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.common.naming.AgentNameChangeListener</code></p>
 */

public interface AgentNameChangeListener {
	/**
	 * Callback when the host or app changes name
	 * @param app The new app name, or null if only the host changed
	 * @param host The new host name, or null if only the app changed
	 */
	public void onAgentNameChange(String app, String host);

}
