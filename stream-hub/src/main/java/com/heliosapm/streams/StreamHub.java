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

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.core.io.UrlResource;

import com.heliosapm.utils.concurrency.ExtendedThreadManager;

/**
 * <p>Title: StreamHub</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.StreamHub</code></p>
 */
@SpringBootApplication
public class StreamHub {
	/** The default URL */
	private static final URL defaultURL = StreamHub.class.getClassLoader().getResource("streamhub.xml");
	private static SpringApplication app = null;
	private static GenericXmlApplicationContext appCtx = null;
	
	/**
	 * Creates a new StreamHub
	 * @param configXML The configuration XML
	 */
	public StreamHub(final URL configXML) {
		appCtx = new GenericXmlApplicationContext();
		appCtx.load(new UrlResource(configXML));
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
		System.setProperty("java.net.preferIPv4Stack" , "true");
		ExtendedThreadManager.install();
		SpringApplication app = new SpringApplication(StreamHub.class);
//		JMXHelper.fireUpJMXMPServer(1829);
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
//		final Thread main = Thread.currentThread();
//		final Thread t = new Thread("StreamHubRunner") {
//			public void run() {
//				try {
//					streamHub.start();
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
//				} catch (Exception x) {/* No Op */}
//				System.exit(0);
//			}
//		}).run();
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
