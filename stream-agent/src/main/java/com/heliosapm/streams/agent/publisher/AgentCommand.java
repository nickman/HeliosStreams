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
import com.heliosapm.streams.agent.commands.InstallCommandProcessor;
import com.heliosapm.streams.agent.commands.ListCommandProcessor;
import com.heliosapm.streams.agent.commands.ListInstalledCommandProcessor;

/**
 * <p>Title: AgentCommand</p>
 * <p>Description: Enumerates the agent commands that can be issued on the command-line</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.agent.publisher.AgentCommand</code></p>
 */

public enum AgentCommand implements AgentCommandProcessor {
	/** List JVMs */
	LIST(new ListCommandProcessor()),
	/** List JVMs and configurations of JVMs with the JMXMP agent installed */
	ILIST(new ListInstalledCommandProcessor()),	
	/** Install the agent into a JVM */
	INSTALL(new InstallCommandProcessor());
	
	private AgentCommand(final AgentCommandProcessor processor) {
		this.processor = processor;
	}
	
	private final AgentCommandProcessor processor;

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.endpoint.publisher.agent.AgentCommandProcessor#processCommand(com.heliosapm.CommandLineParser.publisher.cl.CommandLine)
	 */
	@Override
	public String processCommand(final CommandLineParser cmdLine) {
		return processor.processCommand(cmdLine);
	}
}


