/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.heliosapm.streams.admin;

import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.management.remote.jmxmp.JMXMPConnectorServer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.config.server.EnableConfigServer;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;

import com.heliosapm.streams.admin.zookeep.ZooKeepPublisher;
import com.heliosapm.utils.collections.Props;
import com.heliosapm.utils.concurrency.ExtendedThreadManager;
import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.url.URLHelper;

import de.codecentric.boot.admin.config.EnableAdminServer;

/**
 * <p>Title: StreamHubAdminServer</p>
 * <p>Description: Bootstrap class for the StreamHub admin server</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.admin.StreamHubAdminServer</code></p>
 */
@SpringBootApplication
@EnableAdminServer
@EnableAutoConfiguration
//@EnableConfigServer
public class StreamHubAdminServer {

	/** The command line arg prefix for the properties config for this admin instance */
	public static final String ADMIN_CONFIG_PREFIX = "--config=";

	/** The default admin server properties */
	private static final Properties DEFAULT_ADMIN_PROPS = URLHelper.readProperties(StreamHubAdminServer.class.getClassLoader().getResource("defaultConfig.properties"));
	
	/** The spring boot initialized app context */
	private static ConfigurableApplicationContext appCtx = null;
	
	/** The original system props so we can reset */
	private static final Map<String, String> VIRGIN_SYS_PROPS = Collections.unmodifiableMap(new HashMap<String, String>(Props.asMap(System.getProperties())));
	
	/** The publisher that registers the advertised URL in ZooKeeper */
	private static ZooKeepPublisher zooKeepPublisher = null;
	
	private static SpringApplication springApp = null;

	
	
	/**
	 * Creates a new StreamHubAdminServer
	 */
	public StreamHubAdminServer() {
	}
	
	/**
	 * Starts the StreamHub admin server
	 * @param args As follows:
	 */
	public static void main(final String[] args) {
		System.setProperty("spring.application.name", "StreamHubAdmin");
		Props.setFromUnless(DEFAULT_ADMIN_PROPS, System.getProperties(), false);		
		installProps(args);
		final JMXMPConnectorServer jmxmp = JMXHelper.fireUpJMXMPServer(System.getProperty("jmx.jmxmp.uri"));
		if(jmxmp!=null) {
			System.out.println("JMXMP Server enabled on [" + jmxmp.getAddress() + "]");
		}
		zooKeepPublisher = new ZooKeepPublisher();
		ExtendedThreadManager.install();
		springApp = new SpringApplication(StreamHubAdminServer.class);							
		springApp.addListeners(new ApplicationListener<ContextRefreshedEvent>(){
			@Override
			public void onApplicationEvent(final ContextRefreshedEvent event) {
				try {
					zooKeepPublisher.start();
					System.out.println("\n\t==================================================\n\tStreamHubAdmin Server Started\n\t==================================================\n");
				} catch (Exception ex) {
					System.err.println("Failed to register with ZooKeeper. Shutting down. Stack trace follows.");
					ex.printStackTrace(System.err);
					System.exit(-1);
				}						
			}
		});
		springApp.addListeners(new ApplicationListener<ContextClosedEvent>(){
			@Override
			public void onApplicationEvent(final ContextClosedEvent event) {
				if(zooKeepPublisher!=null) {
					zooKeepPublisher.stop();
					if(jmxmp!=null) {
						try { jmxmp.stop(); System.out.println("Stopped JMXMP Connection Server"); } catch (Exception x) {/* No Op */}
					}
					System.out.println("\n\t==================================================\n\tStreamHubAdmin Server Stopped\n\t==================================================\n");						
				}
			}
		});			
		appCtx = springApp.run(args);
        StdInCommandHandler.getInstance().run();
    }	
	
	/**
	 * Examines the command line arguments to find the admin config arguments
	 * @param args The command line args
	 */
	private static void installProps(final String[] args) {
		for(String s: args) {
			if(s.startsWith(ADMIN_CONFIG_PREFIX)) {
				s = s.replace(ADMIN_CONFIG_PREFIX, "").trim();
			}
			try {
				final URL url = URLHelper.toURL(s);
				try {
					final String propsStr = URLHelper.getTextFromURL(url, 3, 1);
					if(propsStr!=null && !propsStr.trim().isEmpty()) {
						final Properties props = Props.strToProps(propsStr.trim());
						Props.setFromUnless(props, System.getProperties(), true);
					}
				} catch (Exception ex) {
					System.err.println("Failed to read properties from [" + s + "]:" + ex); 
				}
				// Props.setFromUnless(final Properties from, final Properties to, final boolean overrideTo, final Properties...unless)
			} catch (Exception x) {/* No Op */}
			break;
		}
	}

}
