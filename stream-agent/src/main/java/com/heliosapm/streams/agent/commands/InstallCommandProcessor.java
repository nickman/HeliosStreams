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

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.jmxmp.JMXMPConnector;
import javax.management.remote.jmxmp.JMXMPConnectorServer;

import org.cliffc.high_scale_lib.NonBlockingHashSet;

import com.heliosapm.shorthand.attach.vm.VirtualMachine;
import com.heliosapm.streams.agent.cl.CommandLineParser;
import com.heliosapm.streams.agent.cl.JMXMPSpec;
import com.heliosapm.streams.agent.endpoint.Endpoint;
import com.heliosapm.streams.agent.naming.AgentName;
import com.heliosapm.streams.agent.util.SimpleLogger;
import com.heliosapm.utils.jmx.JMXHelper;

/**
 * <p>Title: InstallCommandProcessor</p>
 * <p>Description: Command processor to install the endpoint publisher and JMXMP Connector Server</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.agent.commands.InstallCommandProcessor</code></p>
 */

public class InstallCommandProcessor extends AbstractAgentCommandProcessor {
	
	/** Format for a JMXMP JMXServiceURL */
	public static final String JMXMP_URL_FORMAT = "service:jmx:jmxmp://%s:%s";
	
	/** A set of installed JMXMP connector servers */
	private static final Set<JMXMPConnectorServer> servers = new NonBlockingHashSet<JMXMPConnectorServer>();
	
	static {
		AccessController.doPrivileged(new PrivilegedAction<Void>() {
			@Override
			public Void run() {
				Runtime.getRuntime().addShutdownHook(new Thread(){
					public void run() {
						if(!servers.isEmpty()) {
							SimpleLogger.log("Shutting down JMXMP Connector Servers");
							for(final JMXMPConnectorServer server: servers) {
								AccessController.doPrivileged(new PrivilegedAction<Void>() {
									@Override
									public Void run() {
										try { server.stop(); } catch (Exception x) {/* No Op */}
										return null;
									}
								});
								
							}
							servers.clear();
						}
					}
				});
				return null;
			}
		});
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.endpoint.publisher.agent.AgentCommandProcessor#processCommand(com.heliosapm.CommandLineParser.publisher.cl.CommandLine)
	 */
	@Override
	public String processCommand(final CommandLineParser cmdLine) {
		// register zookeep sysprop
		// register app/host
		AgentName.getInstance().resetNames(cmdLine.getApp(), cmdLine.getHost());
		final String zookeepConnect = cmdLine.getZKConnectString();
		final boolean zkProvided = zookeepConnect!=null && !zookeepConnect.trim().isEmpty();
		if(zkProvided) {
			setProperty(ZK_CONNECT_PROP, zookeepConnect.trim());
		}
		final StringBuilder b = new StringBuilder("[InstallCommandProcessor]:");
		VirtualMachine vm = null;
		try {
			vm = VirtualMachine.attach("" + cmdLine.getPid());
			for(final JMXMPSpec spec: cmdLine.getJMXMPSpecs()) {
				try {
					final Endpoint[] endpoints = null;  /// FIXME !!!
					final String bind = spec.getBindInterface();
					final int port = spec.getPort();
					final String svc = String.format(JMXMP_URL_FORMAT, bind, port);
					final JMXServiceURL serviceUrl = JMXHelper.serviceUrl(svc);
					final MBeanServer[] server = new MBeanServer[1];
					final JMXMPConnectorServer[] connectorServer = new JMXMPConnectorServer[1];
					final int[] actualPort = new int[1];
					AccessController.doPrivileged(new PrivilegedAction<Void>() {
						@Override
						public Void run() {
							try {
								server[0] = JMXMPConnector.getMBeanServer(spec.getJmxDomain());
								connectorServer[0] = new JMXMPConnectorServer(serviceUrl, null, server[0]);
								connectorServer[0].start();
								servers.add(connectorServer[0]);
								actualPort[0] = connectorServer[0].getAddress().getPort();
								if(endpoints!=null && endpoints.length > 0) {
									addEndpoints(svc, endpoints);
								}
								return null;
							} catch (Exception ex) {
								throw new RuntimeException(ex);
							}
						}
					});
					
				} catch (Exception ex) {
					throw new RuntimeException(ex);
				}
			}
		} catch (Exception ex) {
			b.insert(0, "ERROR ");
			b.append("Install failed:").append(ex);
		} finally {
			if(vm!=null) try { vm.detach(); } catch (Exception x) {/* No Op */}
		}
		return b.toString();
	}

}
