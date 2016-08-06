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

import java.beans.PropertyEditor;
import java.beans.PropertyEditorManager;
import java.io.File;
import java.io.IOException;
import java.lang.instrument.Instrumentation;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
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
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.groovy.control.CompilationFailedException;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.control.customizers.ImportCustomizer;
import org.codehaus.groovy.control.messages.WarningMessage;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.heliosapm.shorthand.attach.vm.agent.LocalAgentInstaller;
import com.heliosapm.streams.collector.ds.JDBCDataSourceManager;
import com.heliosapm.streams.collector.execution.CollectorExecutionService;
import com.heliosapm.utils.collections.Props;
import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.enums.Primitive;
import com.heliosapm.utils.file.FileChangeEvent;
import com.heliosapm.utils.file.FileChangeEventListener;
import com.heliosapm.utils.file.FileChangeWatcher;
import com.heliosapm.utils.file.FileFinder;
import com.heliosapm.utils.file.Filters.FileMod;
import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.jmx.SharedScheduler;
import com.heliosapm.utils.lang.StringHelper;
import com.heliosapm.utils.ref.ReferenceService;
import com.heliosapm.utils.url.URLHelper;

import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyCodeSource;
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
	private static final Logger log = LogManager.getLogger(ManagedScriptFactory.class);
	
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
			"import com.heliosapm.streams.collector.groovy.*"
	};
	
	/** Cache injection substitution pattern */
	public static final Pattern CACHE_PATTERN = Pattern.compile("\\$cache\\{(.*?)(?::(\\d+))?(?::(nanoseconds|microseconds|milliseconds|seconds|minutes|hours|days))??\\}");
	/** Typed value substitution pattern */
	public static final Pattern TYPED_PATTERN = Pattern.compile("\\$typed\\{(.*?):(.*)\\}");
	
	/** Injected field template */
	public static final String INJECT_TEMPLATE = "@Dependency(value=\"%s\", timeout=%s, unit=%s) def %s;"; 
	/** The UTF8 char set */
	public static final Charset UTF8 = Charset.forName("UTF8");
	/** The platform end of line character */
	public static final String EOL = System.getProperty("line.separator");
	
	
	
	
	/** The expected directory names under the collector-service root */
	public static final Set<String> DIR_NAMES = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(
			"lib", "bin", "conf", "datasources", "web", "collectors", "cache", "db", "chronicle", "ssh", "fixtures"
	)));
	
	/** The collector service root directory */
	protected final File rootDirectory;
	/** The collector service script directory */
	protected final File scriptDirectory;
	
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
					instance = new ManagedScriptFactory();
				}
			}
		}
		return instance;
	}
	
	public static void main(String[] args) {
		System.setProperty(CONFIG_ROOT_DIR, "./src/test/resources/test-root");
		JMXHelper.fireUpJMXMPServer(3456);
		final Instrumentation instr = LocalAgentInstaller.getInstrumentation();
		getInstance();
		StdInCommandHandler.getInstance()
			.registerCommand("gc", new Runnable(){
				public void run() {
					System.gc();
				}
			}).registerCommand("cls", new Runnable(){
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
		log.info("Collector Service root directory: [{}]", rootDirectory);
		rootDirectory.mkdirs();
		scriptDirectory = new File(rootDirectory, "collectors").getAbsoluteFile();
		System.setProperty("helios.collectors.script.root", scriptDirectory.getAbsolutePath());
		if(!rootDirectory.isDirectory()) throw new RuntimeException("Failed to create root directory [" + rootDirectory + "]");
		initSubDirs();
		libDirClassLoader = new URLClassLoader(listLibJarUrls(new File(rootDirectory, "lib"), new HashSet<URL>()));
				//HeliosURLClassLoader.getOrCreateLoader(getClass().getSimpleName() + "LibClassLoader", listLibJarUrls(new File(rootDirectory, "lib"), new HashSet<URL>()));
//		ServiceLoader<Driver> sl = ServiceLoader.load(Driver.class, libDirClassLoader);
//		for(Driver d: sl) {
//			log.info("Loaded Driver: [{}]", d.getClass().getName());
//		}
//		Thread.currentThread().setContextClassLoader(libDirClassLoader);
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
	
	/**
	 * Processes any found dependencies and returns the prejected code
	 * @param sourceFile The source file to process
	 * @param bindingMap The forthcoming script's binding map
	 * @return the [possibly empty] prejected code
	 */
	protected static StringBuilder processDependencies(final File sourceFile, final Map<String, Object> bindingMap) {
		final StringBuilder b = new StringBuilder();
		final Properties p = readConfig(sourceFile);
		if(p.isEmpty()) return b;
		for(final String key: p.stringPropertyNames()) {
			final String value = p.getProperty(key, "").trim();
			if(value.isEmpty()) continue;
			try {				
				final Matcher m = CACHE_PATTERN.matcher(value);
				if(m.matches()) {
					final String t = m.group(2);
					final String u = m.group(3);
				    final String cacheKey = m.group(1).trim();
				    final long timeout = (t==null || t.trim().isEmpty()) ? 0 : Long.parseLong(t.trim());			    
				    final TimeUnit unit = (u==null || u.trim().isEmpty()) ? TimeUnit.HOURS : TimeUnit.valueOf(u.trim().toUpperCase());
				    b.append(String.format(INJECT_TEMPLATE, cacheKey, timeout, unit, key)).append(EOL);
				    continue;
				}
			} catch (Exception ex) {
				log.warn("Failed to process injected property [{}]:[{}]", key, value, ex);
				continue;
			}
			try {				
				final Matcher m = TYPED_PATTERN.matcher(value);
				if(m.matches()) {
					final String type = m.group(1);
					final String val = m.group(2);
					if(type==null || type.trim().isEmpty() || val==null || val.trim().isEmpty()) continue;
				    if(Primitive.ALL_CLASS_NAMES.contains(type.trim())) {
				    	Class<?> clazz = Primitive.PRIMNAME2PRIMCLASS.get(type.trim());
				    	PropertyEditor pe = PropertyEditorManager.findEditor(clazz);
				    	pe.setAsText(val.trim());
				    	bindingMap.put(key, pe.getValue());
				    	continue;
				    }
				}
			} catch (Exception ex) {
				log.warn("Failed to process injected property [{}]:[{}]", key, value, ex);
				continue;
			}
		}
		return b;
	}
	
	/**
	 * Groovy source files may have an accompanying config properties file which by convention is
	 * the same file, except with an extension of <b><code>.properties</code></b> instead of <b><code>.groovy</code></b>.
	 * Groovy source files may also be symbolic links to a template, which in turn may have it's own configuration properties, similarly named.
	 * This method attempts to read from both, merges the output with the local config overriding the template's config
	 * and returns the result.
	 * @param sourceFile this script's source file
	 * @return the merged properties
	 */
	protected static Properties readConfig(final File sourceFile) {
		final Path sourcePath = sourceFile.toPath();
		final Properties p = new Properties();
		try {
			final Path linkedSourcePath = sourceFile.toPath().toRealPath();
			if(!linkedSourcePath.equals(sourcePath)) {
				final File linkedProps = new File(linkedSourcePath.toFile().getAbsolutePath().replace(".groovy", ".properties"));
				if(linkedProps.canRead()) {
					final Properties linkedProperties = Props.strToProps(StringHelper.resolveTokens(URLHelper.getStrBuffFromURL(URLHelper.toURL(linkedProps))), UTF8);
					p.putAll(linkedProperties);
				}
			}
		} catch (Exception x) {/* No Op */} 
		final File localProps = new File(sourcePath.toFile().getAbsolutePath().replace(".groovy", ".properties"));
		if(localProps.canRead()) {
			final Properties localProperties = Props.strToProps(StringHelper.resolveTokens(URLHelper.getStrBuffFromURL(URLHelper.toURL(localProps))), UTF8);
			p.putAll(localProperties);
		}
		
		
		return p;
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
							final Map<String, Object> bindingMap = new HashMap<String, Object>(128);
							final StringBuilder b = processDependencies(sourceFile, bindingMap);
							final ByteBufReaderSource originalCode = new ByteBufReaderSource(sourceFile);
							final ByteBufReaderSource prejectedCode;
							if(b.length()>1) {
								prejectedCode = new ByteBufReaderSource(b, originalCode);
							} else {
								prejectedCode = null;
							}
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
		final GroovyClassLoader groovyClassLoader = new GroovyClassLoader(libDirClassLoader, compilerConfig, false);
//		groovyClassLoader.addClasspath(new File(rootDirectory, "fixtures").getAbsolutePath());
//		groovyClassLoader.addClasspath(new File(rootDirectory, "conf").getAbsolutePath());
		final long gclId = groovyClassLoaderSerial.incrementAndGet();
		ReferenceService.getInstance().newWeakReference(groovyClassLoader, new Runnable(){
			public void run() {
				log.info("GroovyClassLoader #{} Unloaded", gclId);
			}
		}); 
//		groovyClassLoaders.put(gclId, groovyClassLoader);		
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
		final String sourceName = source.getAbsolutePath().replace(rootDirectory.getAbsolutePath(), "");
		final GroovyClassLoader gcl = newGroovyClassLoader();
		boolean success = false;
		String errMsg = null;
		try {
			log.info("Compiling script [{}]...", sourceName);
			final GroovyCodeSource gcs = new GroovyCodeSource(source, compilerConfig.getSourceEncoding());
			
			gcs.setCachable(false);
			final Class<ManagedScript> msClazz = gcl.parseClass(gcs);
			ReferenceService.getInstance().newWeakReference(msClazz, null);
			final ManagedScript ms = msClazz.newInstance();
			ms.initialize(gcl, source, rootDirectory.getAbsolutePath());
			success = true;
			managedScripts.put(source, ms);
			successfulCompiles.increment();
			compiledScripts.add(sourceName);
			failedScripts.remove(sourceName);
			log.info("Successfully Compiled script [{}].", sourceName);
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
	

}
