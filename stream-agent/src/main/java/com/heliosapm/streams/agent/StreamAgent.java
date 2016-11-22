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
package com.heliosapm.streams.agent;

import java.io.File;
import java.lang.instrument.Instrumentation;
import java.lang.management.ManagementFactory;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.management.remote.jmxmp.JMXMPConnector;

import com.heliosapm.streams.agent.util.SimpleLogger;


/**
 * <p>Title: Agent</p>
 * <p>Description: The JMXMP and Endpoint publisher agent</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.agent.Agent</code></p>
 */

public class StreamAgent {
	/** The agent provided instrumentation */
	public static Instrumentation INSTRUMENTATION = null;
	/** Static class logger */
	protected static final Logger log = Logger.getLogger(StreamAgent.class.getName());
	/** This JVM's PID */
	public static final long PID;
	/** The log file for agent operations */
	public static final File agentLogFile;
	
	static {
		final long[] pid = new long[1];
		final File[] logFile = new File[1]; 
		AccessController.doPrivileged(new PrivilegedAction<Void>() {
			@Override
			public Void run() {
				pid[0] = Long.parseLong(ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
				logFile[0] = new File(new File(System.getProperty("java.io.tmpdir")), "agentlogfile-" + PID + ".log");
				logFile[0].delete();
				//logFile[0].deleteOnExit();
				return null;
			}
		});
		PID = pid[0];
		agentLogFile = logFile[0];
		SimpleLogger.setAgentLogFile(agentLogFile);
	}
	
	
	/** Keep a reference to created connectors keyed by the listening port */
	private static final Map<Integer, JMXMPConnector> connectors = new ConcurrentHashMap<Integer, JMXMPConnector>();
	/** A scheduler in case we need to wait for an MBeanServer to show up */
	private static volatile ScheduledExecutorService scheduler = null;
	
	/**
	 * The agent premain
	 * @param agentArgs The agent arguments
	 * @param inst The agent instrumentation
	 */
	public static void premain(final String agentArgs, final Instrumentation inst) {
		if(INSTRUMENTATION==null) INSTRUMENTATION = inst;
//		try {			
//			final CommandLine cl = CommandLine.parse(agentArgs);
//			cl.getCommand().processCommand(cl);
//		} catch (Throwable ex) {
//			ex.printStackTrace(System.err);
//		}
	}

	/**
	 * The agent premain with no instrumentation
	 * @param agentArgs The agent arguments
	 */
	public static void premain(final String agentArgs) {
		premain(agentArgs, null);
	}
	
	/**
	 * The agent main 
	 * @param agentArgs The agent arguments
	 * @param inst The agent instrumentation
	 */
	public static void agentmain(final String agentArgs, final Instrumentation inst) {
		premain(agentArgs, inst);
	}

	/**
	 * The agent main with no instrumentation
	 * @param agentArgs The agent arguments
	 */
	public static void agentmain(final String agentArgs) {
		premain(agentArgs, null);
	}
	
//	/**
//	 * The requested MBeanServer was not found, so we'll retry  for a while and see if it shows up
//	 * @param spec The spec with the missing MBeanServer 
//	 */
//	private static void schedule(final Map<SpecField, String> spec) {
//		final String domain = spec.get(SpecField.DOMAIN);
//		final int port = Integer.parseInt(spec.get(SpecField.PORT));
//		final ScheduledFuture<?>[] sf = new ScheduledFuture[1];  
//		sf[0] = getScheduler().scheduleWithFixedDelay(new Runnable(){
//			@Override
//			public void run() {
//				if(JMXMPConnector.getMBeanServer(domain)!=null) {
//					sf[0].cancel(false);
//					try {
//						JMXMPConnector connector = new JMXMPConnector(spec);
//						connectors.put(port, connector);
//						connector.start();
//					} catch (Exception ex) {
//						log.log(Level.SEVERE, "Failed to start JMXMP server for spec [" + spec + "]. Retries are cancelled");
//					}
//				}
//			}
//		}, 5, 5, TimeUnit.SECONDS);
//		
//	}
	
	private static ScheduledExecutorService getScheduler() {
		if(scheduler==null) {
			synchronized(StreamAgent.class) {
				if(scheduler==null) {
					scheduler = Executors.newScheduledThreadPool(1, new ThreadFactory(){
						final AtomicInteger serial = new AtomicInteger();
						@Override
						public Thread newThread(final Runnable r) {
							final Thread t = new Thread("MBeanServerPollingThread#" + serial.incrementAndGet());
							t.setDaemon(true);
							return t;
						}
					});
				}
			}
		}
		return scheduler;
	}
	
	private StreamAgent() {}

}
