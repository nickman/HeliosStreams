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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationFailedEvent;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextStoppedEvent;

import com.heliosapm.utils.concurrency.ExtendedThreadManager;
import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.jmx.JMXHelper;

/**
 * <p>Title: StreamHub</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.StreamHub</code></p>
 */
@SpringBootApplication
@Configuration
@ImportResource("classpath:streamhub.xml")
public class StreamHub {
	/** The default URL */
	private static final URL defaultURL = StreamHub.class.getClassLoader().getResource("streamhub.xml");
	private static SpringApplication app = null;
	private static ConfigurableApplicationContext appCtx = null;
	/** Static class logger */
	public static final Logger LOG = LogManager.getLogger(StreamHub.class);
	
	private static final String[] stateStoreInMems = {
		"streamhub.statestore.metrictimestamp.inmemory",
		"streamhub.statestore.accumulator.inmemory",
	};
	
	/**
	 * Creates a new StreamHub
	 * @param configXML The configuration XML
	 */
	public StreamHub(final URL configXML) {
//		appCtx = new GenericXmlApplicationContext();
//		appCtx.load(new UrlResource(configXML));
	}
	
	public StreamHub() {
		
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
	public static void main(String[] args) {
		final Thread main = Thread.currentThread();
		System.setProperty("java.net.preferIPv4Stack" , "true");
		System.setProperty("spring.output.ansi.enabled", "true");
		ExtendedThreadManager.install();
		final SpringApplication app = new SpringApplication(StreamHub.class);
		app.addListeners(new ApplicationListener<ApplicationFailedEvent>() {
			@Override
			public void onApplicationEvent(final ApplicationFailedEvent appFailedEvent) {
				final Throwable t = appFailedEvent.getException();
				LOG.error("AppCtx failed on startup", t);
				try { appFailedEvent.getApplicationContext().close(); } catch (Exception x) {/* No Op */}
				main.interrupt();
			}	
		});
		app.addListeners(new ApplicationListener<ApplicationReadyEvent>() {
			@Override
			public void onApplicationEvent(final ApplicationReadyEvent readyEvent) {
				LOG.info("\n\t*************************************\n\tStreamHub Started\n\t*************************************\n");
			}
		});
		app.addListeners(new ApplicationListener<ContextClosedEvent>() {
			@Override
			public void onApplicationEvent(final ContextClosedEvent appStoppedEvent) {
				LOG.error("AppCtx Stopped");
				main.interrupt();
			}	
		});
		
		JMXHelper.fireUpJMXMPServer(1829);
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
		
		final Thread t = new Thread("StreamHubRunner") {
			public void run() {
				try {
					appCtx = app.run(args);
				} catch (Exception ex) {
					try { appCtx.close(); } catch (Exception x) {/* No Op */}
					main.interrupt();
					Runtime.getRuntime().halt(-1);
				}
			}
		};
		t.setDaemon(true);
		t.start();
		StdInCommandHandler.getInstance().registerCommand("shutdown", new Runnable(){
			public void run() {
				try {
					appCtx.close();
				} catch (Exception x) {
				} finally {					
					main.interrupt();
				}
			}
		}).runAsync(true)
		.join();
		
//		try {
//			Thread.currentThread().join();
//		} catch (InterruptedException ex) {
//			System.exit(0);
//		}
	}
	
//	JMXHelper.getAgentProperties().setProperty("sun.java.command", "StreamHubOK");
//	JMXHelper.getAgentProperties().setProperty("sun.rt.javaCommand" , "StreamHubOK");
//	System.setProperty("sun.java.command", "StreamHubOK");
//	System.setProperty("sun.rt.javaCommand" , "StreamHubOK");						
	
	
	/**
	 * Err prints the usage
	 */
	public static void usage() {
		System.err.println("Usage:\n\t//TODO: implements this");
	}

}
