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
package com.heliosapm.streams.collector.groovy;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.jar.JarFile;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashSet;
import org.codehaus.groovy.control.CompilationFailedException;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.control.customizers.ImportCustomizer;
import org.codehaus.groovy.control.messages.WarningMessage;
import org.springframework.context.annotation.Import;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.heliosapm.shorthand.attach.vm.agent.LocalAgentInstaller;
import com.heliosapm.streams.collector.cache.GlobalCacheService;
import com.heliosapm.streams.collector.ds.JDBCDataSourceManager;
import com.heliosapm.streams.collector.execution.CollectorExecutionService;
import com.heliosapm.streams.collector.ssh.SSHConnection;
import com.heliosapm.streams.collector.ssh.SSHTunnelManager;
import com.heliosapm.streams.tracing.TracerFactory;
import com.heliosapm.utils.collections.Props;
import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.file.FileChangeEvent;
import com.heliosapm.utils.file.FileChangeEventListener;
import com.heliosapm.utils.file.FileChangeWatcher;
import com.heliosapm.utils.file.FileFinder;
import com.heliosapm.utils.file.FileHelper;
import com.heliosapm.utils.file.Filters.FileMod;
import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.jmx.SharedScheduler;
import com.heliosapm.utils.lang.StringHelper;
import com.heliosapm.utils.ref.ReferenceService;
import com.heliosapm.utils.reflect.PrivateAccessor;
import com.heliosapm.utils.url.URLHelper;

import groovy.lang.Binding;
import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyCodeSource;
import groovy.lang.GroovySystem;
import groovy.lang.MetaClassRegistryChangeEvent;
import groovy.lang.MetaClassRegistryChangeEventListener;
import jsr166e.LongAdder;
import jsr166y.ForkJoinTask;

/**
 * <p>Title: ManagedScriptFactory</p>
 * <p>Description: The factory for creating {@link ManagedScript} instances</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.groovy.ManagedScriptFactory</code></p>
 */

public class ManagedScriptFactory implements ManagedScriptFactoryMBean, FileChangeEventListener, MetaClassRegistryChangeEventListener {
	/** The singleton instance */
	private static volatile ManagedScriptFactory instance;
	/** The singleton instance ctor lock */
	private static final Object lock = new Object();
	/** Static class logger */
	private final Logger log = LogManager.getLogger(ManagedScriptFactory.class);
	
	/** The configuration key for the collector service root directory */
	public static final String CONFIG_ROOT_DIR = "collector.service.rootdir";
	/** The default collector service root directory */
	public static final String DEFAULT_ROOT_DIR = new File(new File(System.getProperty("user.home")), ".heliosapm-collector").getAbsolutePath();
	/** The configuration key for the groovy compiler auto imports */
	public static final String CONFIG_AUTO_IMPORTS = "collector.service.groovy.autoimports";
	/** The default groovy compiler auto imports */
	public static final String[] DEFAULT_AUTO_IMPORTS = {
			"import javax.management.*", 
			"import java.lang.management.*",
			"import com.heliosapm.streams.collector.groovy.*",
			"import groovy.transform.*"
	};
	
	
	/** The expected directory names under the collector-service root */
	public static final Set<String> DIR_NAMES = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(
			"templates", "tmp", "lib", "bin", "conf", "datasources", "web", "collectors", "cache", "db", "chronicle", "ssh", "fixtures"
	)));
	
	/** The collector service root directory */
	protected final File rootDirectory;
	/** The 3rd party lib directory */
	protected final File libDirectory;
	/** The 3rd party JDBC lib directory */
	protected final File jdbcLibDirectory;
	
	/** The collector service script directory */
	protected final File scriptDirectory;
	/** The collector service fixture directory */
	protected final File fixtureDirectory;
	/** The collector service conf directory */
	protected final File confDirectory;
	/** The collector service tmp directory where the compiler puts runtime artifacts */
	protected final File tmpDirectory;
	
	/** The collector service script path */
	protected final Path scriptPath;
	
	/** The configured plus the default auto imports */
	protected final Set<String> autoImports = new NonBlockingHashSet<String>();
	
	/** The configured tracing factory */
	protected final TracerFactory tracerFactory;
	
	/** The lib (jar) directory class loader */
	protected final URLClassLoader libDirClassLoader;
	/** The groovy compiler configuration */
	protected final CompilerConfiguration compilerConfig = new CompilerConfiguration(CompilerConfiguration.DEFAULT);
	/** The initial and default imports customizer for the compiler configuration */
	protected final ImportCustomizer importCustomizer = new ImportCustomizer();
	/** The groovy source file file finder */
	protected FileFinder sourceFinder = null; 
	/** The groovy source file watcher */
	protected FileChangeWatcher fileChangeWatcher = null;
	/** The JDBC data source manager to provide DB connections */
	protected JDBCDataSourceManager jdbcDataSourceManager = null;
	/** The collector execution thread pool */
	protected final CollectorExecutionService collectorExecutionService;
	/** The base bindings to supply to all compiled scripts */
	protected final Map<String, Object> globalBindings = new ConcurrentHashMap<String, Object>();
	
	/** A counter of successful compilations */
	protected final LongAdder successfulCompiles = new LongAdder();
	/** A counter of failed compilations */
	protected final LongAdder failedCompiles = new LongAdder();
	/** The source file names of successfully compiled scripts */
	protected final Set<String> compiledScripts = new ConcurrentSkipListSet<String>();
	/** The source file names of scripts that failed to compile */
	protected final Set<String> failedScripts = new ConcurrentSkipListSet<String>();
	/** Weak value cache to track created groovy class loaders */
	protected final Cache<Long, GroovyClassLoader> groovyClassLoaders = CacheBuilder.newBuilder()
		.concurrencyLevel(4)
		.initialCapacity(2048)
		.weakValues()
		.build();
	/** Id generator for groovy class loaders */
	protected final AtomicLong groovyClassLoaderSerial = new AtomicLong();
	/** Weak value cache to track created groovy scripts */
	protected final Cache<File, ManagedScript> managedScripts = CacheBuilder.newBuilder()
		.concurrencyLevel(4)
		.initialCapacity(2048)
		.weakValues()
		.build();
	
	
	/**
	 * Acquires and returns the ManagedScriptFactory singleton instance
	 * @return the ManagedScriptFactory singleton instance
	 */
	public static ManagedScriptFactory getInstance() {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					GroovySystem.stopThreadedReferenceManager();
					GroovySystem.setKeepJavaMetaClasses(false);
					instance = new ManagedScriptFactory();
				}
			}
		}
		return instance;
	}
	
	public static void main(String[] args) {
		System.setProperty(CONFIG_ROOT_DIR, "./src/test/resources/test-root");
		JMXHelper.fireUpJMXMPServer("0.0.0.0:3456");
		final Instrumentation instr = LocalAgentInstaller.getInstrumentation();
		getInstance();
		StdInCommandHandler.getInstance()
			.registerCommand("gc", new Runnable(){
				@Override
				public void run() {
					System.gc();
				}
			}).registerCommand("cls", new Runnable(){
				@Override
				public void run() {
//					final Map<String, int[]> map = new HashMap<String, int[]>();
					final StringBuilder b = new StringBuilder("======== GCL Loaded Classes:");
					for(Class<?> clazz: instr.getAllLoadedClasses()) {
						final ClassLoader cl = clazz.getClassLoader();
						if(cl!=null && (cl instanceof GroovyClassLoader)) {
							b.append("\n\t").append(clazz.getName()).append(" : ").append(cl.toString());
						}
					}
					b.append("\n==========");
					System.err.println(b);
				}
			})
		.run();
	}
	
	protected final Set<Class<?>> metaClasses = new CopyOnWriteArraySet<Class<?>>();
	
	/**
	 * {@inheritDoc}
	 * @see groovy.lang.MetaClassRegistryChangeEventListener#updateConstantMetaClass(groovy.lang.MetaClassRegistryChangeEvent)
	 */
	@Override
	public void updateConstantMetaClass(final MetaClassRegistryChangeEvent cmcu) {
		final Set<Class<?>> metaClasses = new HashSet<Class<?>>();
		metaClasses.add(cmcu.getClassToUpdate());
		metaClasses.add(cmcu.getNewMetaClass().getClass());
		metaClasses.add(cmcu.getOldMetaClass().getClass());
		final StringBuilder b = new StringBuilder();
		for(Class<?> clazz: metaClasses) {
			b.append(clazz.getName()).append(":[").append(clazz.getClassLoader()).append("@").append(System.identityHashCode(clazz.getClassLoader())).append("]\n");
		}
		log.info("MetaClassRegistry Updates:\n{}", b.toString());
		metaClasses.addAll(metaClasses);				
	}
	
	/**
	 * Creates a new ManagedScriptFactory
	 */
	private ManagedScriptFactory() {
		log.info(">>>>> Starting ManagedScriptFactory...");
		final String rootDirName = ConfigurationHelper.getSystemThenEnvProperty(CONFIG_ROOT_DIR, DEFAULT_ROOT_DIR);		
		rootDirectory = new File(rootDirName);
		initSubDirs(rootDirectory);
		log.info("Collector Service root directory: [{}]", rootDirectory);
		rootDirectory.mkdirs();
		libDirectory = new File(rootDirectory, "lib");
		jdbcLibDirectory = new File(libDirectory, "jdbc");
		jdbcLibDirectory.mkdir();
		scriptDirectory = new File(rootDirectory, "collectors").getAbsoluteFile();
		fixtureDirectory = new File(rootDirectory, "fixtures").getAbsoluteFile();
		confDirectory = new File(rootDirectory, "conf").getAbsoluteFile();
		tmpDirectory = new File(rootDirectory, "tmp").getAbsoluteFile();		
		tracerFactory = initTracing(confDirectory);
		scriptPath = scriptDirectory.toPath();
		
		System.setProperty("helios.collectors.script.root", scriptDirectory.getAbsolutePath());
		if(!rootDirectory.isDirectory()) throw new RuntimeException("Failed to create root directory [" + rootDirectory + "]");
		
		libDirClassLoader = new URLClassLoader(listLibJarUrls(libDirectory, new HashSet<URL>()));
		loadJDBCDrivers();
				//HeliosURLClassLoader.getOrCreateLoader(getClass().getSimpleName() + "LibClassLoader", listLibJarUrls(new File(rootDirectory, "lib"), new HashSet<URL>()));
//		ServiceLoader<Driver> sl = ServiceLoader.load(Driver.class, libDirClassLoader);
//		for(Driver d: sl) {
//			log.info("Loaded Driver: [{}]", d.getClass().getName());
//		}
//		Thread.currentThread().setContextClassLoader(libDirClassLoader);
		FileHelper.cleanDir(tmpDirectory);
		MetaClassRegistryCleaner.createAndRegister();
//		GroovySystem.setKeepJavaMetaClasses(false);
//		GroovySystem.stopThreadedReferenceManager();
		customizeCompiler();
		collectorExecutionService = CollectorExecutionService.getInstance();
		jdbcDataSourceManager = new JDBCDataSourceManager(new File(rootDirectory, "datasources"));
		SharedScheduler.getInstance();
		try { JMXHelper.registerMBean(this, OBJECT_NAME); } catch (Exception ex) {
			log.warn("Failed to register ManagedScriptFactory management interface. Will continue without.", ex);
		}
		log.info("<<<<< ManagedScriptFactory started. Async script deployment starting now.");
		sourceFinder = FileFinder.newFileFinder(scriptDirectory.getAbsolutePath())
			.maxDepth(20)
			.filterBuilder()
			.caseInsensitive(false)
			.endsWithMatch(".groovy")
			.fileAttributes(FileMod.READABLE)
			.shouldBeFile()
			.fileFinder();
		startScriptDeployer();
	}
	
	
	private static TracerFactory initTracing(final File trcConfigDir) {
		final File jsonFile = new File(trcConfigDir, "tracing.json");
		if(!jsonFile.canRead()) {
			throw new IllegalStateException("No tracing json defined at [" + jsonFile + "]");
		}
		return TracerFactory.getInstance(URLHelper.toURL(jsonFile));
	}
	
	
	/** 
	 * Starts the script deployer
	 */
	protected void startScriptDeployer() {
		final long start = System.currentTimeMillis();
		final File[] sourceFiles = sourceFinder.find();
		if(sourceFiles!=null && sourceFiles.length > 0) {
			final List<ForkJoinTask<Boolean>> compilationTasks = new ArrayList<ForkJoinTask<Boolean>>(sourceFiles.length);
			for(final File sourceFile : sourceFiles) {
				compilationTasks.add(collectorExecutionService.submit(new Callable<Boolean>(){
					@Override
					public Boolean call() throws Exception {
						try {
							compileScript(sourceFile);
							return true;
						} catch (Exception ex) {
							return false;
						}
					}
				}));				
			}
			log.info("Waiting for [{}] source files to be compiled", sourceFiles.length);
			for(ForkJoinTask<Boolean> task: compilationTasks) {
				try {
					task.get();
				} catch (Exception e) {					
					e.printStackTrace();
				}
			}
			final long elapsed = System.currentTimeMillis() - start;
			log.info("Startup compilation completed for [{}] source files. Successful: [{}], Failed: [{}], Elapsed: [{}] ms.", sourceFiles.length, successfulCompiles.longValue(), failedCompiles.longValue(), elapsed);
		}
		fileChangeWatcher = sourceFinder.watch(5, true, this);
		fileChangeWatcher.startWatcher(5);
	}
	
	
	
	/**
	 * Creates a new groovy class loader for a new managed script
	 * @return the groovy class loader
	 */
	public GroovyClassLoader newGroovyClassLoader() {
		return newGroovyClassLoader(null);
	}
	
	
	/**
	 * Creates a new groovy class loader for a new managed script
	 * @param altConfig An optional alternate compiler configuration. If null, uses the factory's default config
	 * @return the groovy class loader
	 */
	public GroovyClassLoader newGroovyClassLoader(final CompilerConfiguration altConfig) {	
		
		final GroovyClassLoader groovyClassLoader = new GroovyClassLoader(libDirClassLoader, (altConfig==null ? compilerConfig : altConfig), false);
		groovyClassLoader.addURL(URLHelper.toURL(fixtureDirectory));
//		groovyClassLoader.addClasspath(new File(rootDirectory, "fixtures").getAbsolutePath());
//		groovyClassLoader.addClasspath(new File(rootDirectory, "conf").getAbsolutePath());
		final long gclId = groovyClassLoaderSerial.incrementAndGet();
		ReferenceService.getInstance().newWeakReference(groovyClassLoader, groovyClassLoaderUnloader(gclId)); 
//		groovyClassLoaders.put(gclId, groovyClassLoader);		
		return groovyClassLoader;
	}
	
	private static Runnable groovyClassLoaderUnloader(final long gclId) {
		return new Runnable(){
			@Override
			public void run() {
				instance.log.info("GroovyClassLoader #{} Unloaded", gclId);
			}
		};
	}
	
	
	/**
	 * Compiles and deploys the script in the passed file
	 * @param source The file to compile the source from
	 * @return the script instance
	 */
	public ManagedScript compileScript(final File source) {
		if(source==null) throw new IllegalArgumentException("The passed source file was null");
		if(!source.canRead()) throw new IllegalArgumentException("The passed source file [" + source + "] could not be read");
		final long startTime = System.currentTimeMillis();
		final String sourceName = source.getAbsolutePath().replace(rootDirectory.getAbsolutePath(), "");
		final GroovyClassLoader gcl = newGroovyClassLoader();
		boolean success = false;
		String errMsg = null;
		try {
			log.info("Compiling script [{}]...", sourceName);
			final ByteBufReaderSource bSource = new ByteBufReaderSource(source, scriptPath); 
			final GroovyCodeSource gcs = new GroovyCodeSource(bSource.getReader(), bSource.getClassName(), bSource.getURI().toString());
			gcs.setCachable(false);
			final Class<ManagedScript> msClazz = gcl.parseClass(gcs);
			ReferenceService.getInstance().newWeakReference(msClazz, null);
			//final ManagedScript ms = PrivateAccessor.createNewInstance(msClazz, new Object[]{new Binding(bSource.getBindingMap())}, Binding.class);
			final ManagedScript ms = msClazz.newInstance();
			final long elapsedTime = System.currentTimeMillis() - startTime;
			ms.initialize(gcl, getGlobalBindings(), bSource, rootDirectory.getAbsolutePath(), elapsedTime);
			success = true;
			managedScripts.put(source, ms);
			successfulCompiles.increment();
			compiledScripts.add(sourceName);
			failedScripts.remove(sourceName);
			
			log.info("Successfully Compiled script [{}] --> [{}].[{}] in [{}] ms.", sourceName, msClazz.getPackage().getName(), msClazz.getSimpleName(), elapsedTime);
			return ms;
		} catch (CompilationFailedException cex) {
			errMsg = "Failed to compile source ["+ source + "]";
			cex.printStackTrace(System.err);
			throw new RuntimeException(errMsg, cex);
		} catch (IOException iex) {
			errMsg = "Failed to read source ["+ source + "]";
			throw new RuntimeException(errMsg, iex);
		} catch (Exception ex) {
			errMsg = "Failed to instantiate script for source ["+ source + "]";
			log.error(errMsg, ex);
			throw new RuntimeException(errMsg, ex);
		} finally {
			if(!success) {
				failedCompiles.increment();
				if(!compiledScripts.contains(sourceName)) {
					failedScripts.add(sourceName);
				}
				log.warn("Script [{}] compilation failed: [{}]", sourceName, errMsg);
				
			}
		}
	}

	/**
	 * Creates any missing subdirectories
	 * @param rootDirectory The root directory
	 */
	public static void initSubDirs(final File rootDirectory) {
		for(String dirName: DIR_NAMES) {
			final File f = new File(rootDirectory, dirName);
			if(!f.isDirectory()) {
				if(!f.mkdir()) {
					System.err.println("Failed to create subdirectory [" + f + "]");
				}
			}
		}		
		
	}
	
	private void customizeCompiler() {
		final File compilerPropsFile = new File(confDirectory, "compiler.properties");
		if(compilerPropsFile.canRead()) {
			final String propsStr = URLHelper.getTextFromFile(compilerPropsFile);
			final String resolvedPropsStr = StringHelper.resolveTokens(propsStr);
			final Properties p = Props.strToProps(resolvedPropsStr);
			compilerConfig.configure(p);
		} else {
			compilerConfig.setDebug(true);
			compilerConfig.setMinimumRecompilationInterval(5);
			compilerConfig.setRecompileGroovySource(false);
			compilerConfig.setOptimizationOptions(Collections.singletonMap("indy", true));
			compilerConfig.setTolerance(100);
			compilerConfig.setVerbose(true);
			compilerConfig.setWarningLevel(WarningMessage.NONE);
			
		}
		compilerConfig.setScriptBaseClass(ManagedScript.class.getName());
		compilerConfig.setTargetDirectory(tmpDirectory);

		final String[] imports = ConfigurationHelper.getArraySystemThenEnvProperty(CONFIG_AUTO_IMPORTS, DEFAULT_AUTO_IMPORTS);
		Collections.addAll(autoImports, DEFAULT_AUTO_IMPORTS);
		if(imports!=DEFAULT_AUTO_IMPORTS) {
			Collections.addAll(autoImports, imports);
		}
		if(fixtureDirectory.isDirectory()) {
			compilerConfig.getClasspath().add(fixtureDirectory.getAbsolutePath() + "/");
			for(File f: fixtureDirectory.listFiles()) {
				if(f.isDirectory()) {
//					autoImports.add("import " + f.getName() + ";");
				}
			}
		}
		applyImports(false);
		compilerConfig.addCompilationCustomizers(importCustomizer, new PackageNameCustomizer());
	}
	
	
	
	/**
	 * Applies the configured imports to the compiler configuration
	 * @param reset Clears the existing imports before adding
	 * @param impCustomizer The import customizer to add the imports to
	 * @param imps  The imports to add
	 */
	@SuppressWarnings("unchecked")
	private synchronized void applyImports(final boolean reset) {	
		if(reset) {
			((List<Import>)PrivateAccessor.getFieldValue(importCustomizer, "imports")).clear();
		}
		for(String imp: autoImports) {
			String _imp = imp.trim().replaceAll("\\s+", " ");
			if(!_imp.startsWith("import")) {
				log.warn("Unrecognized import [", imp, "]"); 
				continue;
			}
			if(_imp.startsWith("import static ")) {
				if(_imp.endsWith(".*")) {
					importCustomizer.addStaticStars(_imp.replace("import static ", "").replace(".*", ""));
				} else {
					String cleaned = _imp.replace("import static ", "").replace(".*", "");
					int index = cleaned.lastIndexOf('.');
					if(index==-1) {
						log.warn("Failed to parse non-star static import [", imp, "]");
						continue;
					}
					importCustomizer.addStaticImport(cleaned.substring(0, index), cleaned.substring(index+1));
				}
			} else {
				if(_imp.endsWith(".*")) {
					importCustomizer.addStarImports(_imp.replace("import ", "").replace(".*", ""));
				} else {
					importCustomizer.addImports(_imp.replace("import ", ""));
				}
			}
		}
		compilerConfig.addCompilationCustomizers(importCustomizer);
	}
	
	
	private void loadJDBCDrivers() {
		final File[] jdbcDrivers = jdbcLibDirectory.listFiles(new FileFilter(){
			@Override
			public boolean accept(final File f) {
				return f.isFile() && f.getName().endsWith(".jar");
			}
		});
		if(jdbcDrivers.length > 0) {
			final Instrumentation instr = LocalAgentInstaller.getInstrumentation();
			for(File f : jdbcDrivers) {
				try {
					final JarFile jar = new JarFile(f, false);
					instr.appendToSystemClassLoaderSearch(jar);
				} catch (Exception ex) {
					log.error("Failed to load JDBC jar file [{}]",  f, ex);
				}
			}
		}
	}
	
	private URL[] listLibJarUrls(final File dir, final Set<URL> accum) {
		final Set<URL> _accum = accum==null ? new HashSet<URL>() : accum;
		for(File f: dir.listFiles()) {
			if(f.isDirectory()) {
				listLibJarUrls(f, _accum);
			} else {
				if(f.getName().toLowerCase().endsWith(".jar")) {
					if(f.getParent().equals(libDirectory)) continue;
					final URL jarUrl = URLHelper.toURL(f.getAbsolutePath());
					_accum.add(jarUrl);
					log.info("Adding [{}] to classpath", jarUrl);
				}
			}
		}
		return _accum.toArray(new URL[_accum.size()]);		
	}
	
	

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.utils.file.FileChangeEventListener#onChange(java.io.File)
	 */
	@Override
	public void onChange(final File file) {
		log.info("Detected change on source file [{}]", file);
		collectorExecutionService.submit(new Callable<Boolean>(){
			@Override
			public Boolean call() throws Exception {
				try {
					compileScript(file);
					return true;
				} catch (Exception ex) {
					return false;
				}
			}
		});
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptFactoryMBean#launchConsole()
	 */
	@Override
	public void launchConsole() {
		launchConsole(null);
	}
	
	
	
	
	private Map<String, Object> getGlobalBindings() {
		if(globalBindings.isEmpty()) {
			synchronized(globalBindings) {
				if(globalBindings.isEmpty()) {
					globalBindings.put("globalCache", GlobalCacheService.getInstance());
					globalBindings.put("dsManager", jdbcDataSourceManager);
					globalBindings.put("tunnelManager", SSHTunnelManager.getInstance());
					globalBindings.put("sshconn", SSHConnection.class);
					globalBindings.put("mbs", JMXHelper.getHeliosMBeanServer());
					globalBindings.put("jmxHelper", JMXHelper.class);
					globalBindings.put("urlHelper", URLHelper.class);
					globalBindings.put("stringHelper", StringHelper.class);					
				}
			}
		}
		return new HashMap<String, Object>(globalBindings);		
		
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptFactoryMBean#launchConsole(java.lang.String)
	 */
	@Override
	public void launchConsole(final String fileName) {
		final CompilerConfiguration cc = new CompilerConfiguration(CompilerConfiguration.DEFAULT);		
		cc.addCompilationCustomizers(importCustomizer);
		final GroovyClassLoader consoleClassLoader = newGroovyClassLoader(cc);
		try {
			final Class<?> clazz = Class.forName("groovy.ui.Console");
			Constructor<?> ctor = clazz.getDeclaredConstructor(ClassLoader.class, Binding.class);
			final Map<String, Object> bmap = getGlobalBindings();
			bmap.put("log", LogManager.getLogger("groovy.ui.Console"));
			final Binding binding = new Binding(bmap);
			final Object console = ctor.newInstance(consoleClassLoader, binding);
			final Method method = console.getClass()
				.getDeclaredMethod("run");
			final CountDownLatch launchLatch = new CountDownLatch(1);
			final Throwable[] launchFail = new Throwable[1];
			final Thread t = new Thread("HeliosGroovyConsoleThread") {
				@Override
				public void run() {
					
					try {
						method.invoke(console);
						
						final String _fileName = (fileName==null || fileName.trim().isEmpty() || !new File(fileName.trim()).canRead()) ? null : fileName.trim(); 
						if(_fileName!=null) {
							File f = new File(_fileName);
							clazz.getDeclaredMethod("loadScriptFile", File.class).invoke(console, f);
						}
					} catch (Throwable t) {
						launchFail[0] = t;
					} finally {
						launchLatch.countDown();
					}
				}
			};
			t.setDaemon(true);
			t.start();
			if(!launchLatch.await(5, TimeUnit.SECONDS)) {
				throw new RuntimeException("Timed out waiting for console launch");
			}
			if(launchFail[0]!=null) {
				throw new RuntimeException("Console launch failed", launchFail[0]);
			}
		} catch (Exception e) {
			log.error("Failed to launch console", e);
			if(e.getCause()!=null) {
				log.error("Failed to launch console cause", e.getCause());
			}
			throw new RuntimeException("Failed to launch console", e);
		}		
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptFactoryMBean#getGroovyClassLoaderCount()
	 */
	@Override
	public long getGroovyClassLoaderCount() {
		return groovyClassLoaders.size();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptFactoryMBean#getManagedScriptCount()
	 */
	@Override
	public long getManagedScriptCount() {
		return managedScripts.size();
	}
	

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.utils.file.FileChangeEventListener#onDelete(java.io.File)
	 */
	@Override
	public void onDelete(final File file) {
		log.info("Detected deleted source file [{}]", file);
		final ManagedScript ms = managedScripts.getIfPresent(file);
		if(ms!=null) {
			managedScripts.invalidate(file);
			try { ms.close(); } catch (Exception x) {/* No Op */}
		}
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.utils.file.FileChangeEventListener#onNew(java.io.File)
	 */
	@Override
	public void onNew(final File file) {
		log.info("Detected new source file [{}]", file);
		collectorExecutionService.submit(new Callable<Boolean>(){
			@Override
			public Boolean call() throws Exception {
				try {
					compileScript(file);
					return true;
				} catch (Exception ex) {
					return false;
				}
			}
		});		
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.utils.file.FileChangeEventListener#getInterest()
	 */
	@Override
	public FileChangeEvent[] getInterest() {
		return FileChangeEvent.values();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.utils.file.FileChangeEventListener#setFileChangeWatcher(com.heliosapm.utils.file.FileChangeWatcher)
	 */
	@Override
	public void setFileChangeWatcher(final FileChangeWatcher fileChangeWatcher) {
		/* No Op */		
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptFactoryMBean#getSuccessfulCompileCount()
	 */
	@Override
	public long getSuccessfulCompileCount() {
		return successfulCompiles.longValue();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptFactoryMBean#getFailedCompileCount()
	 */
	@Override
	public long getFailedCompileCount() {
		return failedCompiles.longValue();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptFactoryMBean#getCompiledScripts()
	 */
	@Override
	public Set<String> getCompiledScripts() {
		return new LinkedHashSet<String>(compiledScripts);
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptFactoryMBean#getFailedScripts()
	 */
	@Override
	public Set<String> getFailedScripts() {
		return new LinkedHashSet<String>(failedScripts);
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptFactoryMBean#getCompiledScriptCount()
	 */
	@Override
	public int getCompiledScriptCount() {
		return compiledScripts.size();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptFactoryMBean#getFailedScriptCount()
	 */
	@Override
	public int getFailedScriptCount() {
		return failedScripts.size();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptFactoryMBean#getAutoImports()
	 */
	@Override
	public Set<String> getAutoImports() {
		return new HashSet<String>(autoImports);
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptFactoryMBean#addAutoImport(java.lang.String)
	 */
	@Override
	public Set<String> addAutoImport(final String importStatement) {
		if(importStatement==null || importStatement.trim().isEmpty()) throw new IllegalArgumentException("The passed importStatement was null or empty");
		if(autoImports.add(importStatement.trim())) {
			applyImports(true);
		}
		return getAutoImports();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptFactoryMBean#removeAutoImport(java.lang.String)
	 */
	@Override
	public Set<String> removeAutoImport(String importStatement) {
		if(importStatement==null || importStatement.trim().isEmpty()) throw new IllegalArgumentException("The passed importStatement was null or empty");
		if(autoImports.remove(importStatement.trim())) {
			applyImports(true);
		}
		return getAutoImports();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptFactoryMBean#clearAutoImports()
	 */
	@Override
	public void clearAutoImports() {
		autoImports.clear();
		applyImports(true);		
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptFactoryMBean#getWarningLevel()
	 */
	@Override
	public int getWarningLevel() {
		return compilerConfig.getWarningLevel();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptFactoryMBean#setWarningLevel(int)
	 */
	@Override
	public void setWarningLevel(final int level) {
		compilerConfig.setWarningLevel(level);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptFactoryMBean#getSourceEncoding()
	 */
	@Override
	public String getSourceEncoding() {
		return compilerConfig.getSourceEncoding();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptFactoryMBean#getTargetDirectory()
	 */
	@Override
	public File getTargetDirectory() {
		return compilerConfig.getTargetDirectory();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptFactoryMBean#getClasspath()
	 */
	@Override
	public List<String> getClasspath() {
		return compilerConfig.getClasspath();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptFactoryMBean#isVerbose()
	 */
	@Override
	public boolean isVerbose() {
		return compilerConfig.getVerbose();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptFactoryMBean#setVerbose(boolean)
	 */
	@Override
	public void setVerbose(final boolean verbose) {
		compilerConfig.setVerbose(verbose);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptFactoryMBean#isDebug()
	 */
	@Override
	public boolean isDebug() {
		return compilerConfig.getDebug();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptFactoryMBean#setDebug(boolean)
	 */
	@Override
	public void setDebug(final boolean debug) {
		compilerConfig.setDebug(debug);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptFactoryMBean#getTolerance()
	 */
	@Override
	public int getTolerance() {
		return compilerConfig.getTolerance();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptFactoryMBean#setTolerance(int)
	 */
	@Override
	public void setTolerance(final int tolerance) {
		compilerConfig.setTolerance(tolerance);
	}


	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptFactoryMBean#getTargetBytecode()
	 */
	@Override
	public String getTargetBytecode() {
		return compilerConfig.getTargetBytecode();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptFactoryMBean#getOptimizationOptions()
	 */
	@Override
	public Map<String, Boolean> getOptimizationOptions() {
		return compilerConfig.getOptimizationOptions();
	}

}
