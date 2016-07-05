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

import java.lang.management.ManagementFactory;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.management.remote.jmxmp.JMXMPConnectorServer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.event.ApplicationFailedEvent;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.ContextClosedEvent;

import com.heliosapm.streams.admin.AdminFinder;
import com.heliosapm.utils.collections.Props;
import com.heliosapm.utils.concurrency.ExtendedThreadManager;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.url.URLHelper;

/**
 * <p>Title: StreamHubBoot</p>
 * <p>Description: The boot class for StreamHub instances</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.StreamHubBoot</code></p>
 */

public class StreamHubBoot {

	/** The default host name */
	public static final String HOST = ManagementFactory.getRuntimeMXBean().getName().split("@")[1];
	/** The current process PID */
	public static final String PID = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
	
	/** The command line arg prefix for the zookeep connect */
	public static final String ZOOKEEP_CONNECT_ARG = "--zookeep=";
	/** The default zookeep connect */
	public static final String DEFAULT_ZOOKEEP_CONNECT = "localhost:2181";
	/** The command line arg prefix for the zookeep connect timeout in ms. */
	public static final String ZOOKEEP_TIMEOUT_ARG = "--ztimeout=";
	/** The default zookeep connect timeout in ms. */
	public static final int DEFAULT_ZOOKEEP_TIMEOUT = 15000;

	private static final String[] stateStoreInMems = {
			"streamhub.statestore.metrictimestamp.inmemory",
			"streamhub.statestore.accumulator.inmemory",
		};		
	/** The original system props so we can reset */
	private static final Map<String, String> VIRGIN_SYS_PROPS = Collections.unmodifiableMap(new HashMap<String, String>(Props.asMap(System.getProperties())));	
	/** The UTF character set */
	public static final Charset UTF8 = Charset.forName("UTF8");
	
	/** The boot time instance */
	private static StreamHubBoot boot = null;
	
	/*
	 * Admin props:  spring.boot.admin.url=http://localhost:8080
	 * App Version:  info.version=@project.version@
	 * spring.boot.admin.api-path:"api/applications"
	 */
	
	/** The current spring boot instantiated application context */
	protected ConfigurableApplicationContext appCtx = null;
	/** The current spring boot application instance */
	protected SpringApplication springApp = null;
	/** The AdminFinder instance */
	protected final AdminFinder adminFinder;
	/** The discovered admin server url */
	protected String adminServerUrl = null;
	
	protected StreamHub streamHub = null;
	
	/**
	 * Creates a new StreamHubBoot
	 * @param args The command line args
	 */
	public StreamHubBoot(final String[] args) {
		if(System.getProperty("os.name", "").toLowerCase().contains("windows")) {
			for(String key: stateStoreInMems) {
				System.setProperty(key, "true");
			}
		}
		System.setProperty("java.net.preferIPv4Stack" , "true");
		System.setProperty("spring.output.ansi.enabled", "DETECT");
		System.setProperty("org.apache.logging.log4j.simplelog.StatusLogger.level", "OFF");
		ExtendedThreadManager.install();
		log(">>>>> Discovering admin server url....");
		adminFinder = AdminFinder.getInstance(args);
		adminServerUrl = adminFinder.getAdminURL(true);
		log("<<<<< Discovered admin server url: [%s]", adminServerUrl);
				
		final String nodeConfigUrl = adminServerUrl + "/nodeconfig/" + HOST.toLowerCase() + "/streamhub";
		log(">>>>> Fetching marching orders from [%s]", nodeConfigUrl);
		final Properties p = URLHelper.readProperties(URLHelper.toURL(nodeConfigUrl));
		p.setProperty("spring.boot.admin.url", adminServerUrl);
		p.setProperty("spring.config.location", nodeConfigUrl);
//		System.getProperties().putAll(p);
//		p.setProperty("spring.config.location", nodeConfigUrl);
		log("<<<<< Configured marching orders. App Properties: [%s]", p.size());
		for(String key: p.stringPropertyNames()) {
			log("\t%s : %s", key, p.getProperty(key));
		}
				
		final JMXMPConnectorServer jmxmp = JMXHelper.fireUpJMXMPServer(p.getProperty("jmx.jmxmp.uri", "jmxmp://0.0.0.0:0"));
		if(jmxmp!=null) {
			System.out.println("JMXMP Server enabled on [" + jmxmp.getAddress() + "]");
		}
		
		
		streamHub = new StreamHub(args, p);
//		SpringApplication.run(StreamHub.class, args);
	}
	
//	app.addListeners(new ApplicationListener<ApplicationFailedEvent>() {
//		@Override
//		public void onApplicationEvent(final ApplicationFailedEvent appFailedEvent) {
//			final Throwable t = appFailedEvent.getException();
//			System.err.println("AppCtx failed on startup");
//			t.printStackTrace(System.err);
//			try { appFailedEvent.getApplicationContext().close(); } catch (Exception x) {/* No Op */}
//			main.interrupt();
//		}	
//	});
//	app.addListeners(new ApplicationListener<ApplicationReadyEvent>() {
//		@Override
//		public void onApplicationEvent(final ApplicationReadyEvent readyEvent) {
//			System.out.println("\n\t*************************************\n\tStreamHub Started\n\t*************************************\n");
//		}
//	});
//	app.addListeners(new ApplicationListener<ContextClosedEvent>() {
//		@Override
//		public void onApplicationEvent(final ContextClosedEvent appStoppedEvent) {
//			System.out.println("AppCtx Stopped");
//			main.interrupt();
//		}	
//	});
	
	
	
	/**
	 * The main entry point for booting StreamHub nodes
	 * @param args All optional. As follows: <ul>
	 * 
	 * </ul>
	 */
	public static void main(final String[] args) {
		boot = new StreamHubBoot(args);
	}
	
	
	/**
	 * Formatted out printer
	 * @param fmt The format specifier
	 * @param args The arguments
	 */
	public static void log(final Object fmt, final Object...args) {
		System.out.println(String.format(fmt.toString(), args));
	}
	
	/**
	 * Formatted err printer
	 * @param fmt The format specifier
	 * @param args The arguments
	 */
	public static void loge(final Object fmt, final Object...args) {
		System.err.println(String.format(fmt.toString(), args));
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

}
