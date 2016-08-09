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

import com.heliosapm.streams.collector.groovy.ManagedScriptFactory;
import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.jmx.JMXHelper;

/**
 * <p>Title: CollectorServer</p>
 * <p>Description: The main entry point to boot a collector server</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.CollectorServer</code></p>
 */

public class CollectorServer {
	
	/** The command help text */
	public static final String COMMAND_HELP = "Helios CollectionServer: Command Line Options: " +
			"--root=<directory name> : Sets the root directory of the collector server. If not supplied, this will be the current directory. " +
			"--jmxmp=<jmxmp listening port> : Sets the port that the JMXMP listener will listen on. If not supplied, defaults to 3456 " +
			"--log4j2=<log4j2 xml config> : Sets the file location of a custom log4j2 XML configuration file. If not supplied, defaults to the internal default location. " +
			"--init : Initializes the root directory" + 
			"--help : Prints these options. ";


	/**
	 * The boot entry point for the CollectorServer
	 * @param args Command line options as follows: <ul>
	 * 	<li><b>--root=&lt;directory name&gt;</b> : Sets the root directory of the collector server.
	 * If not supplied, this will be the current directory.</li>
	 * 	<li><b>--jmxmp=&lt;jmxmp listening port&gt;</b> : Sets the port that the JMXMP listener will listen on.
	 * If not supplied, defaults to <b><code>3456</code></b></li>
	 * 	<li><b>--log4j2=&lt;log4j2 xml config&gt;</b> : Sets the file location of a custom log4j2 XML configuration file.
	 * If not supplied, defaults to the internal default location.</li>
	 * 	<li><b>--init</b> : Initializes the root directory</li>
	 * 	<li><b>--help</b> : Prints these options.</li>
	 * </ul>
	 */
	public static void main(String[] args) {
		System.out.println("Starting Helios CollectorServer....");
		if(args.length==1) {
			if("--help".equals(args[0])) {
				System.out.println(COMMAND_HELP);
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
		
		final int jmxmpPort = findArg("--jmxmp=", 3456, args);
		final String log4jLoc = findArg("--log4j2=", null, args);
		if(log4jLoc!=null) {
			final File f = new File(log4jLoc);
			if(f.canRead()) {
				System.setProperty("log4j.configurationFile", f.toPath().normalize().toFile().getAbsolutePath());
			} else {
				System.err.println("Cannot read log4j2 config file [" + log4jLoc + "]. Falling back to default.");
			}
		}
		JMXHelper.fireUpJMXMPServer(jmxmpPort);
		ManagedScriptFactory.getInstance();
		StdInCommandHandler.getInstance().run();
	}
	
	private static void initDir(final File rootDirectory) {
		ManagedScriptFactory.initSubDirs(rootDirectory);
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
