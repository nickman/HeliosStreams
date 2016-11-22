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
package com.heliosapm.streams.agent.publisher;


import com.heliosapm.streams.agent.cl.CommandLineParser;
import com.heliosapm.streams.agent.services.EndpointPublisher;

/**
 * <p>Title: AgentCommandProcessor</p>
 * <p>Description: Defines a class that processes an instance of an agent command</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.agent.publisher.AgentCommandProcessor</code></p>
 */

public interface AgentCommandProcessor {
	/** The system and agent property key that keys a comma separated list of installed connector JMX URLs */
	public static final String JMXMP_CONNECTORS_PROP = "jmxmp.connectors";
	/** The system and agent property key that keys a JSON string of jmx-connector URLs to advertised endpoints */
	public static final String JMXMP_ENDPOINTS_PROP = "jmxmp.endpoints";
	/** The system and agent property key that keys the set Zookeeper connect string */
	public static final String ZK_CONNECT_PROP = EndpointPublisher.ZK_CONNECT_CONF;
	
	/**
	 * Executes the passed command line sourced directive
	 * @param cmdLine The command line directive
	 * @return the response message
	 */
	public String processCommand(CommandLineParser cmdLine);
}
