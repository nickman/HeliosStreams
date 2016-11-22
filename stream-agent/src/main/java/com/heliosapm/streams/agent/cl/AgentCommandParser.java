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
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.heliosapm.streams.agent.endpoint.Endpoint;
import com.heliosapm.streams.agent.util.SimpleLogger;

/**
 * <p>Title: AgentCommandParser</p>
 * <p>Description: Parses and provides values for an agent command</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.agent.cl.AgentCommandParser</code></p>
 */

public class AgentCommandParser {
	/** The parsed out JMXSpecs */
	protected JMXMPSpec[] specs = {};
	/** The parsed out endpoints */
	protected final Set<Endpoint> endpoints = new LinkedHashSet<Endpoint>();
	/** The Zookeeper connection string */
	protected String zookeep = null;
	/** The overridden host name that should be published for the target JVM */
	protected String host = null;
	/** The overridden app name that should be published for the target JVM */
	protected String app = null;
	
	/** The original provided arguments */
	protected final String[] args;
	
	/** A whitespace splitter */
	public static final Pattern WHITESPACE_SPLITTER = Pattern.compile("\\s+");

	/**
	 * Creates a new AgentCommandParser
	 * @param args The agent commands
	 */
	public AgentCommandParser(final String[] args) {
		SimpleLogger.log("Commands: %s", Arrays.toString(args));
		this.args = args;
	}
	
	/**
	 * Creates a new AgentCommandParser
	 * @param arg The agent command
	 */
	public AgentCommandParser(final String arg) {
		this(WHITESPACE_SPLITTER.split(arg));
	}
	
	public AgentCommandParser parse() {
		CmdLineParser parser = new CmdLineParser(this);
	   	try {
    		parser.parseArgument(args);
        } catch (CmdLineException e) {
            final StringBuilder b = new StringBuilder(e.getMessage()).append("\n");
            final StringWriter sw = new StringWriter();            
            parser.printUsage(sw, null);
            sw.flush();
            b.append(sw.toString());
            throw new RuntimeException(b.toString(), e);
        }
	   	return this;
	}
	
	/**
	 * Sets the JMXMPSpecs
	 * @param specs the JMXMPSpecs
	 */
	@Option(name="--specs", required=false, usage="Sets one or more JMXMPSpecs in the form of <comma separated specs>[:<JMXMP listening port>][:<JMXMP bind interface>][:<JMX Domain Name>]", handler=JMXMPSpecOptionHandler.class)
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
	 * Adds an endpoint
	 * @param endpoint The endpoint to add
	 */
	@Option(name="--ep", usage="Adds an endpoint in the form of <name>[-<period><unit>][/<processor name>] e.g. stdjvm-15s, or dropwizard-1m/standardtagged", handler=EndpointOptionHandler.class, aliases={"--endpoint"}, depends="--zk")
	protected void setEndpoint(final Endpoint endpoint) {
		endpoints.add(endpoint);
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

	/**
	 * Returns the JMXMP specs
	 * @return the JMXMP specs
	 */
	public JMXMPSpec[] getJMXMPSpecs() {
		return specs;
	}
	
	/**
	 * Returns the ZooKeeper connect string
	 * @return the ZooKeeper connect string
	 */
	public String getZKConnectString() {
		return zookeep;
	}
	
	/**
	 * Returns the endpoints to advertise
	 * @return the endpoints to advertise
	 */
	public Endpoint[] getEndpoints() {
		return endpoints.toArray(new Endpoint[endpoints.size()]);
	}
	

}
