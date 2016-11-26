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
package com.heliosapm.streams.agent.cl;

import java.io.StringWriter;
import java.util.Arrays;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.heliosapm.streams.agent.publisher.AgentCommand;
import com.heliosapm.streams.agent.util.SimpleLogger;



/**
 * <p>Title: CommandLine</p>
 * <p>Description: Command line processor for publisher agent</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org) 
 * <p><code>com.heliosapm.streams.agent.cl.CommandLine</code></p>
 */

public class CommandLineParser extends AgentCommandParser {
	/** The agent command to execute */
	protected AgentCommand command = null;
	/** The PID of the JVM process to install into */
	protected Long pid = null;
	/** The overridden host name that should be published for the target JVM */
	protected String host = null;
	/** The overridden app name that should be published for the target JVM */
	protected String app = null;
	
	
	// ==================================================================================
	//			List command arguments
	// ==================================================================================
	protected boolean showAgentProps = false;
	protected boolean showSystemProps = false;
	protected boolean showJMXMP = false;
	protected boolean showAgentName = false;
	

	
	/**
	 * Creates a new CommandLine
	 * @param args The command line arguments
	 */
	public CommandLineParser(final String... args) {		
		super(args);
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.agent.cl.AgentCommandParser#parse()
	 */
	@Override
	public CommandLineParser parse() {
		return (CommandLineParser)super.parse();
	}
	
//	/**
//	 * Creates a CommandLine from the passed arguments
//	 * @param args The arguments to parse
//	 * @return the parsed CommandLine instance
//	 */
//	public static CommandLineParser parse(final String[] args) {
//		SimpleLogger.log("Agent Arguments: %s", Arrays.toString(args));
//		CommandLineParser cl = new CommandLineParser();
//		CmdLineParser parser = new CmdLineParser(cl);
//	   	try {
//    		parser.parseArgument(args);
//    		return cl;
//        } catch (CmdLineException e) {
//            final StringBuilder b = new StringBuilder(e.getMessage()).append("\n");
//            final StringWriter sw = new StringWriter();            
//            parser.printUsage(sw, null);
//            sw.flush();
//            b.append(sw.toString());
//            throw new RuntimeException(b.toString());
//        }		 		
//	}
	
	
	public static void main(String[] args) {
		SimpleLogger.log("Testing CmdLine");
		final String agentArg = "install " +
				"--specs " +  
				"foo,bar,jvm,kafka:8192:0.0.0.0:DefaultDomain;jdatasources,tomcat:8193:0.0.0.0:jboss " + 
				"--host " +
				"appServer5 " + 
				"--app " + 
				"stream-boy     " + 
				"--zk " + 
				"localhost:2181,localhost:2182";

		final String[] xargs = {
			"install",
			"--specs",
			"foo,bar,jvm,kafka:8192:0.0.0.0:DefaultDomain;jdatasources,tomcat:8193:0.0.0.0:jboss",
			"--host",
			"appServer5",
			"--app",
			"stream-boy",
			"--zk",
			"localhost:2181,localhost:2182"
		};
		
		try {
//			CommandLineParser cl = CommandLineParser.parse(agentArg);
//			SimpleLogger.log(cl.toString());
		} catch (Exception ex) {
			SimpleLogger.elog(ex.getMessage());
		}
	}
	

	
	
	/**
	 * Sets the agent command
	 * @param command the command to set
	 */
	@Argument(index=0, required=true, metaVar="COMMAND", usage="The agent command")
	protected void setCommand(final AgentCommand command) {
		this.command = command;
	}
	
	/**
	 * Sets the pid of the JVM process to install into
	 * @param pid the pid to set
	 */
	@Option(name="--pid", usage="The PID of the target JVM process", metaVar="JVM PID")
	protected void setPid(long pid) {
		this.pid = pid;
	}

	/**
	 * Returns the configured agent command
	 * @return the configured agent command
	 */
	public AgentCommand getCommand() {
		return command;
	}

	/**
	 * Returns the pid of the JVM process to install into
	 * @return the pid of the JVM process to install into
	 */
	public Long getPid() {
		return pid;
	}



}
