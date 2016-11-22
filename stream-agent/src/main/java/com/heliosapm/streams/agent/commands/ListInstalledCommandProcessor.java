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
package com.heliosapm.streams.agent.commands;

import com.heliosapm.streams.agent.cl.CommandLineParser;
import com.heliosapm.streams.agent.publisher.AgentCommandProcessor;

/**
 * <p>Title: ListInstalledCommandProcessor</p>
 * <p>Description: Lists JVMs with installed agents and the associated attributes</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.agent.commands.ListInstalledCommandProcessor</code></p>
 */

public class ListInstalledCommandProcessor implements AgentCommandProcessor {

	/**
	 * Creates a new ListInstalledCommandProcessor
	 */
	public ListInstalledCommandProcessor() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.endpoint.publisher.agent.AgentCommandProcessor#processCommand(com.heliosapm.CommandLineParser.publisher.cl.CommandLine)
	 */
	@Override
	public String processCommand(final CommandLineParser cmdLine) {
		// TODO Auto-generated method stub
		return null;
	}

}
