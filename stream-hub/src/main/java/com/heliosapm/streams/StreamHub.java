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
package com.heliosapm.streams;

import java.net.URL;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationFailedEvent;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.event.ContextClosedEvent;

import com.heliosapm.streams.admin.AdminFinder;
import com.heliosapm.utils.collections.Props;
import com.heliosapm.utils.concurrency.ExtendedThreadManager;
import com.heliosapm.utils.url.URLHelper;

/**
 * <p>Title: StreamHub</p>
 * <p>Description: Boostrap class for StreamHub instances</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.StreamHub</code></p>
 */

@SpringBootApplication
@ImportResource("classpath:streamhub.xml")
@EnableDiscoveryClient
//@EnableAdminServer
public class StreamHub implements Watcher {
	private static ConfigurableApplicationContext appCtx = null;
	private static SpringApplication springApp = null;
	/** Static class logger */
//	public static final Logger LOG = LogManager.getLogger(StreamHub.class);
	
	private static final String[] stateStoreInMems = {
		"streamhub.statestore.metrictimestamp.inmemory",
		"streamhub.statestore.accumulator.inmemory",
	};

	/** The zookeep parent node name to retrieve the streamhub admin url */
	public static final String ZOOKEEP_URL_ROOT = "/streamhub/admin";
	/** The zookeep node name to retrieve the streamhub admin url */
	public static final String ZOOKEEP_URL = ZOOKEEP_URL_ROOT + "/url";
	
	/** The command line arg prefix for the zookeep connect */
	public static final String ZOOKEEP_CONNECT_ARG = "--zookeep=";
	/** The default zookeep connect */
	public static final String DEFAULT_ZOOKEEP_CONNECT = "localhost:2181";
	/** The command line arg prefix for the zookeep connect timeout in ms. */
	public static final String ZOOKEEP_TIMEOUT_ARG = "--ztimeout=";
	/** The default zookeep connect timeout in ms. */
	public static final int DEFAULT_ZOOKEEP_TIMEOUT = 15000;
	
	
	/** The original system props so we can reset */
	private static final Map<String, String> VIRGIN_SYS_PROPS = Collections.unmodifiableMap(new HashMap<String, String>(Props.asMap(System.getProperties())));
	
	/** The UTF character set */
	public static final Charset UTF8 = Charset.forName("UTF8");
	
	/*
	 * Admin props:  spring.boot.admin.url=http://localhost:8080
	 * App Version:  info.version=@project.version@
	 * spring.boot.admin.api-path:"api/applications"
	 */
	
	/**
	 * Creates a new StreamHub
	 * @param configXML The configuration XML
	 */
	public StreamHub(final URL configXML) {
//		appCtx = new GenericXmlApplicationContext();
//		appCtx.load(new UrlResource(configXML));
	}
	
	public StreamHub() {
		
//		JMXHelper.fireUpJMXMPServer(1828);
//		URL configURL = defaultURL;
//		final int maxIndex = args.length-1;
//		for(int i = 0; i < args.length; i++) {
//			if(args[i].indexOf("--")==0) {
//				final String command = args[i].substring(2).toLowerCase();
//				if("config".equals(command)) {
//					if(i == maxIndex) {
//						usage();
//						System.exit(-1);
//					}
//					i++;
//					configURL = URLHelper.toURL(args[1]);
//				}				
//			}
//		}
//		final StreamHub streamHub = new StreamHub(configURL);
		
//		final Thread t = new Thread("StreamHubRunner") {
//			public void run() {
//				try {
//					appCtx = app.run();
//				} catch (Exception ex) {
//					try { appCtx.close(); } catch (Exception x) {/* No Op */}
//					main.interrupt();
//					Runtime.getRuntime().halt(-1);
//				}
//			}
//		};
//		t.setDaemon(true);
//		t.start();
//		StdInCommandHandler.getInstance().registerCommand("shutdown", new Runnable(){
//			public void run() {
//				try {
//					appCtx.close();
//				} catch (Exception ex) {
//					System.out.println("WARN: Failure on app context close:" + ex);
//				} finally {					
//					main.interrupt();
//				}
//			}
//		}).runAsync(true)
//		.join();
		
//		try {
//			Thread.currentThread().join();
//		} catch (InterruptedException ex) {
//			System.exit(0);
//		}
		
	}
	
	/** The discovery zookeep client */
	protected ZooKeeper zk;
	
	public StreamHub(final String[] args) {
		final AdminFinder af = AdminFinder.getInstance(args);
		try {
			final String adminURL = af.getAdminURL(true);
			//System.setProperty("spring.boot.admin.url", adminURL);
			System.setProperty("spring.boot.admin.url", "http://localhost:7560/streamhubadmin");
			final Properties p = URLHelper.readProperties(URLHelper.toURL(adminURL + "/streamhubadmin/nodeconfig/helioleopard/streamhub"));
			System.getProperties().putAll(p);
			springApp = new SpringApplication(StreamHub.class);			
			appCtx = springApp.run(args);			
			
		} catch (Exception ex) {
			throw new RuntimeException("Failed to start StreamHub Instance", ex);
		}
		
	}
	
	/**
	 * Starts the stream hub
	 */
	public void start() {
		appCtx.refresh();
	}

	/**
	 * Main entry point for StreamHub
	 * @param args Supported arguments are all <b><code>--</code></b> prefixed, followed by the value if not a boolean flag: <ul>
	 * 	<li><b>--config &lt;URL or file for the spring xml configuration file&gt;</b> Overrides the built in spring config.</li>
	 * </ul>
	 * TODO: Add JMXMP Port and iface
	 * TODO: Add help
	 */
	public static void main(final String[] args) {
		System.setProperty("spring.boot.admin.client.enabled", "true");
		System.setProperty("info.version", "1.0.1");
		System.setProperty("spring.boot.admin.client.name", "StreamHubNode");
		final StreamHub hub = new StreamHub(args);
	}
	
//	JMXHelper.getAgentProperties().setProperty("sun.java.command", "StreamHubOK");
//	JMXHelper.getAgentProperties().setProperty("sun.rt.javaCommand" , "StreamHubOK");
//	System.setProperty("sun.java.command", "StreamHubOK");
//	System.setProperty("sun.rt.javaCommand" , "StreamHubOK");						
	
	
	private static void loadProps(final String[] args) {
		if(args!=null) {
			for(String s: args) {
				try {
					final Properties p = URLHelper.readProperties(URLHelper.toURL(s));
					if(!p.isEmpty()) {
						for(String key: p.stringPropertyNames()) {
							System.setProperty(key, p.getProperty(key, "").trim());
						}
					}
				} catch (Exception x) {/* No Op */}
			}
		}
	}
	
	/**
	 * Finds a command line arg value
	 * @param prefix The prefix
	 * @param defaultValue The default value if not found
	 * @param args The command line args to search
	 * @return the value
	 */
	private static int findArg(final String prefix, final int defaultValue, final String[] args) {
		final String s = findArg(prefix, (String)null, args);
		if(s==null) return defaultValue;
		try {
			return Integer.parseInt(s);
		} catch (Exception ex) {
			return defaultValue;
		}
	}
	
	
	/**
	 * Finds a command line arg value
	 * @param prefix The prefix
	 * @param defaultValue The default value if not found
	 * @param args The command line args to search
	 * @return the value
	 */
	private static String findArg(final String prefix, final String defaultValue, final String[] args) {
		for(String s: args) {
			if(s.startsWith(prefix)) {
				s = s.replace(prefix, "").trim();
				return s;
			}
		}
		return defaultValue;
	}
	
	
	/**
	 * Err prints the usage
	 */
	public static void usage() {
		System.err.println("Usage:\n\t//TODO: implements this");
	}

	@Override
	public void process(WatchedEvent arg0) {
		// TODO Auto-generated method stub
		
	}

}
