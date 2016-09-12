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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.web.SpringBootServletInitializer;
import org.springframework.cloud.client.circuitbreaker.EnableCircuitBreaker;
import org.springframework.cloud.netflix.hystrix.EnableHystrix;
import org.springframework.cloud.netflix.hystrix.dashboard.EnableHystrixDashboard;
import org.springframework.cloud.netflix.turbine.EnableTurbine;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import com.heliosapm.streams.collector.cache.GlobalCacheService;
import com.heliosapm.streams.collector.groovy.ManagedScriptFactory;
import com.heliosapm.utils.collections.Props;
import com.heliosapm.utils.concurrency.ExtendedThreadManager;
import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.lang.StringHelper;
import com.heliosapm.utils.reflect.PrivateAccessor;
import com.heliosapm.utils.url.URLHelper;

/**
 * <p>Title: CollectorServer</p>
 * <p>Description: The main entry point to boot a collector server</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.CollectorServer</code></p>
 * FIXME:  replace this ugly stuff with args4j
 */
@Configuration
@EnableAutoConfiguration
@Controller
@SpringBootApplication
@EnableHystrix
@EnableCircuitBreaker
@EnableHystrixDashboard
@EnableTurbine
//@ComponentScan({"com.heliosapm.streams.collector.jmx.discovery.*"})
public class CollectorServer extends  SpringBootServletInitializer {
	
	/** The command help text */
	public static final String COMMAND_HELP = "Helios CollectionServer: Command Line Options: \n" +
			"--root=<directory name> : Sets the root directory of the collector server. If not supplied, this will be the current directory. \n" +
			"--jmxmp=<jmxmp listening port> : Sets the port that the JMXMP listener will listen on. If not supplied, defaults to 3456 \n" +
			"--log4j2=<log4j2 xml config> : Sets the file location of a custom log4j2 XML configuration file. If not supplied, defaults to the internal default location. \n" +
			"--init : Initializes the root directory then exits\n" + 
			"--console : Enables console logging in the internal logging config\n" +
			"--nospring : Runs the collector server as a plain java app, not a spring boot app\n" +
			"--help : Prints these options then exits \n";

	/** Non spring cmd line arg prefixes */
	public static final Set<String> NON_SPRING_CMDS = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(
			"root", "jmxmp", "log4j2", "init", "console", "nospring", "help"
	)));
	
	/** Pattern match for <b>--</b> prefixed cmd line args */
	public static final Pattern NON_SPRING_CMD_PATTERN = Pattern.compile("^\\-\\-(\\w+)?.*");
	
	public static final String BOOT_CLASS = "com.heliosapm.streams.collector.groovy.ManagedScriptFactory";
	
	/** The configuration key for the collector service root directory */
	public static final String CONFIG_ROOT_DIR = "collector.service.rootdir";

	/** The current booted app context */
	private static ConfigurableApplicationContext appCtx = null;
	/** The current booted spring app */
	private static SpringApplication springApp = null;
	/** The spring boot launch thread */
	private static Thread springBootLaunchThread = null;
	
	/** Indicates if we're running in spring mode */
	private static boolean noSpringMode = false;
	
	

//	public static void main(String[] args) {
//		final File f = new File("D:\\temp\\coll");
//		listInitResources(f.toPath());
//	}
	
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
		System.setProperty(CONFIG_ROOT_DIR, rootDir);
		final File confDir = new File(rootDir, "conf");
		final File sysprops = new File(confDir, "sys.properties");
		final File appprops = new File(confDir, "app.properties");
		if(sysprops.canRead()) {
			try {
				final Properties p = Props.strToProps(StringHelper.resolveTokens(URLHelper.getStrBuffFromURL(URLHelper.toURL(sysprops)))) ;
				System.getProperties().putAll(p);
				System.out.println("Applied [" + p.size() + "] system properties from [" + sysprops + "]");
			} catch (Exception ex) {
				System.err.println("Failed to read and apply System [" + sysprops + "]. Stack trace follows.");
				ex.printStackTrace(System.err);
				System.exit(-1);
			}
			if(appprops.canRead()) {
				try {
					final Properties p = Props.strToProps(StringHelper.resolveTokens(URLHelper.getStrBuffFromURL(URLHelper.toURL(appprops)))) ;
					ConfigurationHelper.setAppProperties(p);
					System.out.println("Applied [" + p.size() + "] app properties from [" + appprops + "]");
				} catch (Exception ex) {
					System.err.println("Failed to read and apply App properties [" + appprops + "]. Stack trace follows.");
					ex.printStackTrace(System.err);
					System.exit(-1);
				}
			}			
					//URLHelper.readProperties(URLHelper.toURL(sysprops));
		}
		if(findArg("--init", null, args) != null) {
			final int index = findArgIndex("--init", args);
			final int eindex = args[index].indexOf('=');
			String dirName = null;
			if(eindex!=-1) {
				dirName = args[index].substring(eindex+1);
				if(dirName.trim().isEmpty()) {
					dirName = null;
				} else {
					if(!URLHelper.isDirectory(dirName)) {
						System.err.println("Invalid --init directory [" + dirName + "]");
						System.exit(-1);
					}
				}
			}
			if(dirName==null) {
				try {
					dirName = args[index+1];
					if(!URLHelper.isDirectory(dirName)) {
						System.err.println("Invalid --init directory [" + dirName + "]");
						System.exit(-1);
					}
				} catch (Exception ex) {
					dirName = null;
				}
			}
			final File initDir = new File(dirName==null ? "." : dirName.trim());
			initDir(initDir);
			System.exit(0);
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
		ExtendedThreadManager.install();
		JMXHelper.fireUpJMXMPServer(jmxmpIface);
		noSpringMode = findArg("--nospring", null, args) == null;
		if(noSpringMode) {
			System.out.println("Booting in Spring Mode");
			final List<String> springArgs = new ArrayList<String>(args.length);
			for(String cmd: args) {
				final Matcher m = NON_SPRING_CMD_PATTERN.matcher(cmd);
				if(!m.matches() || !NON_SPRING_CMDS.contains(m.group(1).toLowerCase())) {
					springArgs.add(cmd);
				}
			}
			appCtx = SpringApplication.run(CollectorServer.class, springArgs.toArray(new String[0]));
			GlobalCacheService.getInstance().put("spring/ApplicationContext", appCtx);
			//appCtx.getAutowireCapableBeanFactory().configureBean(ManagedScriptFactory.getInstance(), "ManagedScriptFactory");
		} else {
			System.out.println("Booting in Standalone Mode");
			PrivateAccessor.invokeStatic(BOOT_CLASS, "getInstance");			
		}
		
		final Thread stopThread = Thread.currentThread();
		Runtime.getRuntime().addShutdownHook(new Thread("CollectorServerShutdownHook"){
			public void run() {
				stopThread.interrupt();
			}
		});
		try {
			Thread.currentThread().join();
		} catch (InterruptedException iex) {
			System.out.println("StopThread Interrupted. Shutting Down....");
		}
		
	}
	
	private static void initDir(final File rootDirectory) {
		ManagedScriptFactory.initSubDirs(rootDirectory);
		System.out.println("Initialized directory [" + rootDirectory + "]");
	}
	

	public static final boolean isSpringMode() {
		return noSpringMode;
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
	
	private static int findArgIndex(final String prefix, final String[] args) {
		for(int i = 0; i < args.length; i++) {
			final String s = args[i];
			if(s.startsWith(prefix)) {
				return i;
			}
		}
		return -1;
		
	}
	
//	private static void listInitResources(final Path target) {
//		FileSystem fileSystem = null;
//		try {
//			URI uri = CollectorServer.class.getResource("/init").toURI();
//		    Path myPath;
//		    String prefix;
//		    if (uri.getScheme().equals("jar")) {
//		        fileSystem = FileSystems.newFileSystem(uri, Collections.<String, Object>emptyMap());
//		        myPath = fileSystem.getPath("/init");
//		        prefix = "/init";
//		    } else {
//		        myPath = Paths.get(uri);
//		        prefix = new File(uri).getAbsolutePath();
//		    }
//		    Files.copy(myPath, target);
//		    System.out.println("Done");
////		    System.out.println("Base:" + prefix);
////		    Stream<Path> walk = Files.walk(myPath);
////		    for (Iterator<Path> it = walk.iterator(); it.hasNext();){
////		    	final File f = it.next().toFile();
////		    	if(f.isFile()) {
////		    		System.out.println("Init Resource:" + f.getName() + ", Parent:" + f.getParentFile());
////		    	}
////		    }
//		} catch (Exception ex) {
//			ex.printStackTrace(System.err);
//		} finally {
//			if(fileSystem!=null) try { fileSystem.close(); } catch (Exception x) {/* No Op */}
//			System.exit(0);
//		}
//	}
	
//	private static String[] findArg(final String prefix, final String defaultValue, final String[] args, final int includeOffset) {
//		try {
//			final String[] extracted = new String[includeOffsets.length];
//			int found = 0;
//			for(int i = 0; i < args.length; i++) {
//				final String s = args[i];
//				if(s.startsWith(prefix)) {
//					final int index = s.indexOf('=');
//					if(index!=-1) {
//						final String exSuffix = s.substring(index+1);
//						if(!exSuffix.trim().isEmpty()) {
//							extracted[found] = exSuffix;
//							found++;
//						}
//					}
//					for(int x = found; x < includeOffsets.length; x++) {
//						extracted[found] = args[includeOffsets[x]];
//						
//					}
//					//s = s.replace(prefix, "").trim();
//					//return s;
//				}
//			}
//			
//		} catch (Exception x) {
//			/* No Op */ 
//		}
//		return new String[] {defaultValue};
//	}
	
    @RequestMapping("/foo")
    public String home() {
        return "forward:/hystrix";
    }

//    @Override
//    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
//        return application.sources(HystrixDashboardApplication.class).web(true);
//    }	
	
}
