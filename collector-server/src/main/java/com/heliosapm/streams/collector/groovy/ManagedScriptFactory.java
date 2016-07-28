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
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.groovy.control.CompilationFailedException;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.control.customizers.ImportCustomizer;
import org.codehaus.groovy.control.messages.WarningMessage;


import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.heliosapm.streams.collector.ds.JDBCDataSourceManager;
import com.heliosapm.streams.collector.execution.CollectorExecutionService;
import com.heliosapm.utils.classload.HeliosURLClassLoader;
import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.file.FileChangeEvent;
import com.heliosapm.utils.file.FileChangeEventListener;
import com.heliosapm.utils.file.FileChangeWatcher;
import com.heliosapm.utils.file.FileFinder;
import com.heliosapm.utils.file.Filters.FileMod;
import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.ref.ReferenceService;
import com.heliosapm.utils.url.URLHelper;

import groovy.lang.GroovyClassLoader;
import jsr166e.LongAdder;
import jsr166y.ForkJoinTask;

/**
 * <p>Title: ManagedScriptFactory</p>
 * <p>Description: The factory for creating {@link ManagedScript} instances</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.groovy.ManagedScriptFactory</code></p>
 */

public class ManagedScriptFactory implements ManagedScriptFactoryMBean, FileChangeEventListener {
	/** The singleton instance */
	private static volatile ManagedScriptFactory instance;
	/** The singleton instance ctor lock */
	private static final Object lock = new Object();
	
	/** The configuration key for the collector service root directory */
	public static final String CONFIG_ROOT_DIR = "collector.service.rootdir";
	/** The default collector service root directory */
	public static final String DEFAULT_ROOT_DIR = new File(new File(System.getProperty("user.home")), ".heliosapm-collector").getAbsolutePath();
	/** The configuration key for the groovy compiler auto imports */
	public static final String CONFIG_AUTO_IMPORTS = "collector.service.groovy.autoimports";
	/** The default groovy compiler auto imports */
	public static final String[] DEFAULT_AUTO_IMPORTS = {"import javax.management.*", "import java.lang.management.*"};
	
	
	/** The expected directory names under the collector-service root */
	public static final Set<String> DIR_NAMES = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(
			"lib", "bin", "conf", "datasources", "web", "collectors", "cache", "db", "chronicle", "ssh", "fixtures"
	)));
	
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** The collector service root directory */
	protected final File rootDirectory;
	/** The lib (jar) directory class loader */
	protected final HeliosURLClassLoader libDirClassLoader;
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
	
	
	/**
	 * Acquires and returns the ManagedScriptFactory singleton instance
	 * @return the ManagedScriptFactory singleton instance
	 */
	public static ManagedScriptFactory getInstance() {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					instance = new ManagedScriptFactory();
				}
			}
		}
		return instance;
	}
	
	public static void main(String[] args) {
		System.setProperty(CONFIG_ROOT_DIR, "./src/test/resources/test-root");
		JMXHelper.fireUpJMXMPServer(3456);
		getInstance();
		StdInCommandHandler.getInstance().run();
	}
	
	/**
	 * Creates a new ManagedScriptFactory
	 */
	private ManagedScriptFactory() {
		log.info(">>>>> Starting ManagedScriptFactory...");
		final String rootDirName = ConfigurationHelper.getSystemThenEnvProperty(CONFIG_ROOT_DIR, DEFAULT_ROOT_DIR);
		rootDirectory = new File(rootDirName);
		log.info("Collector Service root directory: [{}]", rootDirectory);
		rootDirectory.mkdirs();
		if(!rootDirectory.isDirectory()) throw new RuntimeException("Failed to create root directory [" + rootDirectory + "]");
		initSubDirs();
		libDirClassLoader = HeliosURLClassLoader.getOrCreateLoader(getClass().getSimpleName() + "LibClassLoader", listLibJarUrls(new File(rootDirectory, "lib"), new HashSet<URL>()));
//		ServiceLoader<Driver> sl = ServiceLoader.load(Driver.class, libDirClassLoader);
//		for(Driver d: sl) {
//			log.info("Loaded Driver: [{}]", d.getClass().getName());
//		}
		Thread.currentThread().setContextClassLoader(libDirClassLoader);
		customizeCompiler();
		collectorExecutionService = CollectorExecutionService.getInstance();
		jdbcDataSourceManager = new JDBCDataSourceManager(new File(rootDirectory, "datasources"));
		try { JMXHelper.registerMBean(this, OBJECT_NAME); } catch (Exception ex) {
			log.warn("Failed to register ManagedScriptFactory management interface. Will continue without.", ex);
		}
		log.info("<<<<< ManagedScriptFactory started. Async script deployment starting now.");
		sourceFinder = FileFinder.newFileFinder(new File(rootDirectory, "collectors").getAbsolutePath())
			.maxDepth(20)
			.filterBuilder()
			.caseInsensitive(false)
			.endsWithMatch(".groovy")
			.fileAttributes(FileMod.READABLE)
			.shouldBeFile()
			.fileFinder();
		startScriptDeployer();
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
							log.info("Compiling script [{}]...", sourceFile.getAbsolutePath());
							compileScript(sourceFile);
							successfulCompiles.increment();
							compiledScripts.add(sourceFile.getAbsolutePath());
							log.info("Successfully Compiled script [{}].", sourceFile.getAbsolutePath());
							return true;
						} catch (Exception ex) {
							failedCompiles.increment();
							failedScripts.add(sourceFile.getAbsolutePath());
							log.warn("Script [{}] compilation failed: [{}]", sourceFile.getAbsolutePath(), ex.getMessage());
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
		fileChangeWatcher = sourceFinder.watch(5, false, this);				
	}
	
	
	
	
	
	/**
	 * Creates a new groovy class loader for a new managed script
	 * @return the groovy class loader
	 */
	public GroovyClassLoader newGroovyClassLoader() {	
		final GroovyClassLoader groovyClassLoader = new GroovyClassLoader(libDirClassLoader, compilerConfig, false);
		groovyClassLoader.addClasspath(new File(rootDirectory, "fixtures").getAbsolutePath());
		groovyClassLoader.addClasspath(new File(rootDirectory, "conf").getAbsolutePath());
		final long gclId = groovyClassLoaderSerial.incrementAndGet();
		ReferenceService.getInstance().newWeakReference(groovyClassLoader, new Runnable(){
			public void run() {
				log.info("GroovyClassLoader #{} Unloaded", gclId);
			}
		});
		groovyClassLoaders.put(gclId, groovyClassLoader);		
		return groovyClassLoader;
	}
	
	
	
	/**
	 * Compiles and deploys the script in the passed file
	 * @param source The file to compile the source from
	 * @return the script instance
	 */
	public ManagedScript compileScript(final File source) {
		if(source==null) throw new IllegalArgumentException("The passed source file was null");
		if(!source.canRead()) throw new IllegalArgumentException("The passed source file [" + source + "] could not be read");
		final GroovyClassLoader gcl = newGroovyClassLoader();
		try {
			final ManagedScript ms = (ManagedScript)gcl.parseClass(source).newInstance();
			ms.initialize(gcl, source, rootDirectory.getAbsolutePath());
			return ms;
		} catch (CompilationFailedException cex) {
			throw new RuntimeException("Failed to compile source ["+ source + "]", cex);
		} catch (IOException iex) {
			throw new RuntimeException("Failed to read source ["+ source + "]", iex);			
		} catch (Exception ex) {
			log.error("Failed to instantiate script for source [" + source + "]", ex);
			throw new RuntimeException("Failed to instantiate script for source ["+ source + "]", ex);
		}
	}

	/**
	 * Creates any missing subdirectories
	 */
	private void initSubDirs() {
		for(String dirName: DIR_NAMES) {
			final File f = new File(rootDirectory, dirName);
			if(!f.isDirectory()) {
				if(!f.mkdir()) {
					log.warn("Failed to create subdirectory [{}]", f);
				}
			}
		}		
	}
	
	private void customizeCompiler() {
		compilerConfig.setDebug(true);
		compilerConfig.setMinimumRecompilationInterval(5);
		compilerConfig.setRecompileGroovySource(true);
		compilerConfig.setOptimizationOptions(Collections.singletonMap("indy", true));
		compilerConfig.setScriptBaseClass(ManagedScript.class.getName());
		compilerConfig.setTargetDirectory(rootDirectory);
		compilerConfig.setTolerance(0);
		compilerConfig.setVerbose(true);
		compilerConfig.setWarningLevel(WarningMessage.PARANOIA);
		final String[] imports = ConfigurationHelper.getArraySystemThenEnvProperty(CONFIG_AUTO_IMPORTS, DEFAULT_AUTO_IMPORTS);
		applyImports(imports);
		if(imports!=DEFAULT_AUTO_IMPORTS) {
			applyImports(DEFAULT_AUTO_IMPORTS);
		}
		compilerConfig.addCompilationCustomizers(importCustomizer);
	}
	
	/**
	 * Applies the configured imports to the compiler configuration
	 * @param impCustomizer The import customizer to add the imports to
	 * @param imps  The imports to add
	 */
	private void applyImports(final String...imps) {		
		for(String imp: imps) {
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
	
	
	private URL[] listLibJarUrls(final File dir, final Set<URL> accum) {
		final Set<URL> _accum = accum==null ? new HashSet<URL>() : accum;
		for(File f: dir.listFiles()) {
			if(f.isDirectory()) {
				listLibJarUrls(f, _accum);
			} else {
				if(f.getName().toLowerCase().endsWith(".jar")) {
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
		// TODO Auto-generated method stub		
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.utils.file.FileChangeEventListener#onDelete(java.io.File)
	 */
	@Override
	public void onDelete(final File file) {
		// TODO Auto-generated method stub
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.utils.file.FileChangeEventListener#onNew(java.io.File)
	 */
	@Override
	public void onNew(final File file) {
		// TODO Auto-generated method stub
		
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
	

}
