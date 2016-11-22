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

import com.heliosapm.streams.agent.util.SimpleLogger;



/**
 * <p>Title: CommandLine</p>
 * <p>Description: Command line processor for publisher agent</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org) 
 * <p><code>com.heliosapm.endpoint.publisher.CommandLine</code></p>
 */

public class CommandLine {
	// ==================================================================================
	//			Install command arguments
	// ==================================================================================
	/** The parsed out JMXSpecs */
	protected JMXMPSpec[] specs = {};
	/** The Zookeeper connection string */
	protected String zookeep = null;
	/** The agent command to execute */
//	protected AgentCommand command = null;
	/** The PID of the JVM process to install into */
	protected long pid = -1;
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
	 */
	public CommandLine() {		
	}
	
	/**
	 * Creates a CommandLine from the passed arguments
	 * @param args The arguments to parse
	 * @return the parsed CommandLine instance
	 */
	public static CommandLine parse(final String[] args) {
		SimpleLogger.log("Agent Arguments: %s", Arrays.toString(args));
		CommandLine cl = new CommandLine();
		CmdLineParser parser = new CmdLineParser(cl);
	   	try {
    		parser.parseArgument(args);
    		return cl;
        } catch (CmdLineException e) {
            final StringBuilder b = new StringBuilder(e.getMessage()).append("\n");
            final StringWriter sw = new StringWriter();            
            parser.printUsage(sw, null);
            sw.flush();
            b.append(sw.toString());
            throw new RuntimeException(b.toString());
        }		 		
	}
	
	/**
	 * Creates a CommandLine from the passed string
	 * @param agentArg The string to parse
	 * @return the parsed CommandLine instance
	 */
	public static CommandLine parse(final String agentArg) {
		return parse(agentArg.split("\\s+"));
	}
	
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
			CommandLine cl = CommandLine.parse(agentArg);
			SimpleLogger.log(cl.toString());
		} catch (Exception ex) {
			SimpleLogger.elog(ex.getMessage());
		}
	}
	

	/**
	 * Sets the JMXMPSpecs
	 * @param specs the JMXMPSpecs
	 */
	@Option(name="--specs", required=false, usage="Sets one or more JMXMPSpecs in the form of <comma separated endpoints>[:<JMXMP listenng port>][:<JMXMP bind interface>][:<JMX Domain Name>]", handler=JMXMPSpecOptionHandler.class)
	protected void setJMXMPSpecs(final JMXMPSpec[] specs) {
		this.specs = specs;
	}
	
	/**
	 * Sets the zookeeper connect string
	 * @param connectString the zookeeper connect string
	 */
	@Option(name="--zk", usage="Sets the Zookeeper connect string in the form of <host1>:<port1>[,<hostn>:<portn>]")
	protected void setZKConnectString(final String connectString) {
		if(connectString==null || connectString.trim().isEmpty()) throw new IllegalArgumentException("The passed ZK connect string was nul or empty");
		this.zookeep = connectString.trim();
	}
	
	/**
	 * Sets the overriden host name to be published for the target JVM
	 * @param host the overriden host name
	 */
	@Option(name="--host", usage="Overrides the published host name", metaVar="HOST")
	protected void setHost(final String host) {
		this.host = host;
	}

	/**
	 * Sets the overriden app name to be published for the target JVM
	 * @param app the overriden app name
	 */
	@Option(name="--app", usage="Overrides the published app name", metaVar="APP")
	protected void setApp(final String app) {
		this.app = app;
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
	@Option(name="--pid", usage="The PID of the target JVM process")
	protected void setPid(long pid) {
		this.pid = pid;
	}
	


	/**
	 * Returns the JMXMP specs
	 * @return the specs
	 */
	public JMXMPSpec[] getJMXMPSpecs() {
		return specs.clone();
	}

	/**
	 * Returns the zookeeper connection string
	 * @return the zookeeper connection string
	 */
	public String getZookeep() {
		return zookeep;
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		final StringBuilder b = new StringBuilder("Publisher Configuration:");
		b.append("\n\tAgent Command: ").append(command);
		b.append("\n\tTarget PID: ").append(pid);
		b.append("\n\tPublished host override: ").append(host);
		b.append("\n\tPublished app override: ").append(app);
		b.append("\n\tJMXMP and Endpoint Specs:");
		for(JMXMPSpec spec: specs) {
			b.append("\n\t\t").append(spec);
		}
		b.append("\n\tZookeeper Connect String:").append(zookeep);
		b.append("\n");
		return b.toString();
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
	public long getPid() {
		return pid;
	}

	/**
	 * Returns the overriden host name to be published for the target JVM
	 * @return the overriden host name or null if not overriden
	 */
	public String getHost() {
		return host;
	}

	/**
	 * Returns the overriden app name to be published for the target JVM
	 * @return the overriden app name or null if not overriden
	 */
	public String getApp() {
		return app;
	}


}
