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
package com.heliosapm.streams.collector;

import java.io.File;
import java.net.URL;

import org.apache.logging.log4j.LogManager;

import com.heliosapm.streams.collector.groovy.ManagedScriptFactory;
import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.url.URLHelper;

/**
 * <p>Title: CollectorServer</p>
 * <p>Description: The main entry point to boot a collector server</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.CollectorServer</code></p>
 * FIXME:  replace this ugly stuff with args4j
 */

public class CollectorServer {
	
	/** The command help text */
	public static final String COMMAND_HELP = "Helios CollectionServer: Command Line Options: " +
			"--root=<directory name> : Sets the root directory of the collector server. If not supplied, this will be the current directory. " +
			"--jmxmp=<jmxmp listening port> : Sets the port that the JMXMP listener will listen on. If not supplied, defaults to 3456 " +
			"--log4j2=<log4j2 xml config> : Sets the file location of a custom log4j2 XML configuration file. If not supplied, defaults to the internal default location. " +
			"--init : Initializes the root directory then exits" + 
			"--console : Enables console logging in the internal logging config" +
			"--help : Prints these options then exits ";


	/**
	 * The boot entry point for the CollectorServer
	 * @param args Command line options as follows: <ul>
	 * 	<li><b>--root=&lt;directory name&gt;</b> : Sets the root directory of the collector server.
	 * If not supplied, this will be the current directory.</li>
	 * 	<li><b>--jmxmp=&lt;jmxmp listening port&gt;</b> : Sets the port that the JMXMP listener will listen on.
	 * If not supplied, defaults to <b><code>3456</code></b></li>
	 * 	<li><b>--log4j2=&lt;log4j2 xml config&gt;</b> : Sets the file location of a custom log4j2 XML configuration file.
	 * If not supplied, defaults to the internal default location.</li>
	 *  <li><b>--console</b> : Enables console logging in the internal logging config</li>
	 * 	<li><b>--init</b> : Initializes the root directory</li>
	 * 	<li><b>--help</b> : Prints these options.</li>
	 * </ul>
	 */
	public static void main(String[] args) {
		System.out.println("Starting Helios CollectorServer....");
		System.setProperty("Log4jContextSelector", "org.apache.logging.log4j.core.selector.BasicContextSelector");
		if(args.length==1) {
			if("--help".equals(args[0])) {
				System.out.println(COMMAND_HELP);
				System.exit(0);
			}
		}
		 
		final String rootDir = findArg("--root=", new File(".").toPath().normalize().toFile().getAbsolutePath(), args);
		System.out.println("Helios CollectorServer Root Directory: [" + rootDir + "]");
		final File rootDirectory = new File(rootDir);
		if(!rootDirectory.isDirectory()) {
			if(rootDirectory.isFile()) {
				System.err.println("Specified root directory [" + rootDirectory + "] is a file");
				System.exit(-1);				
			} else if(!rootDirectory.exists()) {
				if(!rootDirectory.mkdirs()) {
					System.err.println("Failed to create root directory [" + rootDirectory + "]");
					System.exit(-1);
				}
			}
		}
		System.setProperty(ManagedScriptFactory.CONFIG_ROOT_DIR, rootDir);
		if(findArg("--init", null, args) != null) {
			initDir(rootDirectory);
		}
		
		final String jmxmpIface = findArg("--jmxmp=", "0.0.0.0:3456", args);
		final String log4jLoc = findArg("--log4j2=", null, args);
		final boolean enableConsole = findArg("--console", null, args) != null;
		if(log4jLoc!=null) {
			final File f = new File(log4jLoc);
			if(f.canRead()) {
				System.setProperty("log4j.configurationFile", f.toPath().normalize().toFile().getAbsolutePath());
			} else {
				System.err.println("Cannot read log4j2 config file [" + log4jLoc + "]. Falling back to default.");
			}
		} else {
			final File confDir = new File(rootDirectory, "conf");
			final File logDir = new File(rootDirectory, "log");
			
			final String configFile = enableConsole ? "console-log4j2.xml" : "quiet-log4j2.xml";
			final String resourceName = "deploy/logging/" + configFile;
			final File log4jXmlFile = new File(confDir, configFile);
			final URL internal = CollectorServer.class.getClassLoader().getResource(resourceName);			
			if(!log4jXmlFile.exists()) {
				URLHelper.writeToFile(internal, log4jXmlFile, false);
			}
			System.setProperty("log4j.configurationFile", log4jXmlFile.toPath().normalize().toFile().getAbsolutePath());
			if(!logDir.exists()) {
				logDir.mkdirs();
			}
			System.setProperty("helios.collectorserver.logdir", logDir.toPath().normalize().toFile().getAbsolutePath());
			System.out.println("Log config: [" + System.getProperty("log4j.configurationFile") + "]");
			System.out.println("Log directory: [" + System.getProperty("helios.collectorserver.logdir") + "]");
			
		}
		LogManager.getRootLogger();
		JMXHelper.fireUpJMXMPServer(jmxmpIface);
		ManagedScriptFactory.getInstance();
		StdInCommandHandler.getInstance().run();
	}
	
	private static void initDir(final File rootDirectory) {
		ManagedScriptFactory.initSubDirs(rootDirectory);
		System.out.println("Initialized directory [" + rootDirectory + "]");
		System.exit(0);
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
