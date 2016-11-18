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
package com.heliosapm.streams.collector.ds;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
//import org.springframework.beans.BeansException;
//import org.springframework.beans.factory.support.DefaultListableBeanFactory;
//import org.springframework.context.ApplicationContext;
//import org.springframework.context.ApplicationContextAware;

import com.heliosapm.streams.collector.cache.GlobalCacheService;
import com.heliosapm.streams.collector.ds.pool.PoolConfig;
import com.heliosapm.streams.collector.execution.CollectorExecutionService;
import com.heliosapm.streams.common.metrics.SharedMetricsRegistry;
import com.heliosapm.utils.file.FileChangeEvent;
import com.heliosapm.utils.file.FileChangeEventListener;
import com.heliosapm.utils.file.FileChangeWatcher;
import com.heliosapm.utils.file.FileFinder;
import com.heliosapm.utils.file.Filters.FileMod;
import com.heliosapm.utils.url.URLHelper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import jsr166e.LongAdder;

/**
 * <p>Title: JDBCDataSourceManager</p>
 * <p>Description: Service to manage JDBC data sources</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.ds.JDBCDataSourceManager</code></p>
 */

public class JDBCDataSourceManager implements FileChangeEventListener { //, ApplicationContextAware {
	
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** The data source definition directory */
	protected final File dsDirectory;
	/** A cache of datasources keyed by the definition file name */
	protected final NonBlockingHashMap<String, ManagedHikariDataSource> dataSources = new NonBlockingHashMap<String, ManagedHikariDataSource>();
	/** A cache of object pools keyed by the definition file name */
	protected final NonBlockingHashMap<String, GenericObjectPool<?>> objectPools = new NonBlockingHashMap<String, GenericObjectPool<?>>();
	
	/** The global cache */
	protected final GlobalCacheService gcache = GlobalCacheService.getInstance(); 
	/** The file finder */
	protected final FileFinder finder;
	/** The file watcher */
	protected FileChangeWatcher fileChangeWatcher = null;
	/** The deployment executor */
	final CollectorExecutionService collectorExecutionService;
	/** A counter of successful deployments */
	protected final LongAdder successfulDeploys = new LongAdder();
	/** A counter of failed deployments */
	protected final LongAdder failedDeploys = new LongAdder();
	
	
	/** Placeholder countdown latch */
	private static final CountDownLatch PLACEHOLDER = new CountDownLatch(1);
	
	static {
		PLACEHOLDER.countDown();
	}
	
	/** The recognized data source and object pool configuration file extensions */
	public static final Set<String> POOL_EXT = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList("ds", "pool")));
	
	/** Deployment completion latches by ds file, removed when deployment completes (successfully or not) */
	private final NonBlockingHashMap<File, CountDownLatch> deploymentLatches = new NonBlockingHashMap<File, CountDownLatch>(); 
	
	
//	/** The spring app context */
//	protected ApplicationContext appCtx = null;
	
	private CountDownLatch placeLatch(final File dsFile) {
		CountDownLatch latch = deploymentLatches.putIfAbsent(dsFile, PLACEHOLDER);
		if(latch==null || latch==PLACEHOLDER) {
			latch = new CountDownLatch(1);
			deploymentLatches.replace(dsFile, latch);
		}
		return latch;
	}


	/**
	 * Creates a new JDBCDataSourceManager
	 * @param dsDirectory The data source definition directory
	 * @param collectorExecutionService the executor service to use for ds deployments
	 */
	public JDBCDataSourceManager(final File dsDirectory, final CollectorExecutionService collectorExecutionService) {
		log.info(">>>>> Starting JDBCDataSourceManager...");
		this.dsDirectory = dsDirectory;
		// TODO: delete all in dynamic
		this.collectorExecutionService = collectorExecutionService;
		finder = FileFinder.newFileFinder(dsDirectory.getAbsolutePath())
		.maxDepth(10)
		.maxFiles(Integer.MAX_VALUE)
		.maxDepth(Integer.MAX_VALUE)
		
		.filterBuilder()
		.caseInsensitive(false)
		.patternMatch(".*\\.ds$|.*\\.pool$")
		
		.fileAttributes(FileMod.READABLE)
		.shouldBeFile()
		.fileFinder();
		final long start = System.currentTimeMillis();
		final File[] dsFiles = finder.find();
		final List<Future<Boolean>> deploymentTasks = new ArrayList<Future<Boolean>>(dsFiles.length);
		for(final File dsFile : dsFiles) {
			deploymentTasks.add(collectorExecutionService.submit(new Callable<Boolean>(){
				@Override
				public Boolean call() throws Exception {
					try {
						deploy(dsFile);
						return true;
					} catch (Exception ex) {
						return false;
					}
				}
			}));				
		}
		log.info("Waiting for [{}] data sources to be deployed", dsFiles.length);
		for(Future<Boolean> task: deploymentTasks) {
			try {
				task.get();
			} catch (Exception e) {					
				e.printStackTrace();
			}
		}
		final long elapsed = System.currentTimeMillis() - start;
		log.info("Startup data source deployment completed for [{}] data sources. Successful: [{}], Failed: [{}], Elapsed: [{}] ms.", dsFiles.length, successfulDeploys.longValue(), failedDeploys.longValue(), elapsed);
		
		fileChangeWatcher = finder.watch(5, true, this);
		fileChangeWatcher.startWatcher(5);
		log.info("<<<<< JDBCDataSourceManager started.");
	}
	
	/**
	 * Awaits deployment of the datasource defined by the passed file
	 * @param dsDef The datasource (or objectpool) definition file
	 * @param timeout The await timeout
	 * @param unit The await timeout unit
	 * @return true if datasource was deployed, false otherwise
	 */
	public boolean awaitDeployment(final File dsDef, final long timeout, final TimeUnit unit) {
		if(dsDef==null) throw new IllegalArgumentException("The passed file was null");
		final CountDownLatch latch = placeLatch(dsDef);
		try {
			final boolean complete = latch.await(timeout, unit);
			if(complete) {
				deploymentLatches.remove(dsDef, latch);
			}
			return complete;
		} catch (InterruptedException iex) {
			throw new RuntimeException("Thread interrupted while waiting for deployment of [" + dsDef + "]", iex);
		}
	}
	
	/**
	 * Awaits deployment of the datasource defined by the passed file using the default timeout of 10s.
	 * @param dsDef The datasource (or objectpool) definition file
	 * @return true if datasource was deployed, false otherwise
	 */
	public boolean awaitDeployment(final File dsDef) {
		return awaitDeployment(dsDef, 10, TimeUnit.SECONDS);
	}
	
	/**
	 * Deploys the passed data source definition file
	 * @param dsDef the data source definition file
	 */
	protected void deploy(final File dsDef) {
		if(dsDef==null) throw new IllegalArgumentException("The passed file was null");
		final CountDownLatch latch = placeLatch(dsDef);
		try {
			final String ext = URLHelper.getFileExtension(dsDef);
			if(ext==null || ext.trim().isEmpty() || !POOL_EXT.contains(ext.toLowerCase())) {
				throw new IllegalArgumentException("The passed file [" + dsDef + "] does not have a recognized extension [" + ext + "]. Recognized extensions are: " + POOL_EXT);
			}
			if(!dsDef.canRead()) throw new IllegalArgumentException("The passed file [" + dsDef + "] cannot be read");
			final String name = dsDef.getName().toLowerCase(); 
			if(name.endsWith(".ds")) {
				deployJDBCDataSource(dsDef);
			} else if(name.endsWith(".pool")) {
				deployObjectPool(dsDef);
			}
		} finally {
			latch.countDown();
			deploymentLatches.remove(dsDef, latch);
		}
	}
	
	/**
	 * Deploys the passed file as a generic object pool
	 * @param poolDef The generic object pool configuration file
	 */
	protected void deployObjectPool(final File poolDef) {
		log.info(">>> Deploying ObjectPool from [{}]", poolDef);
		final GenericObjectPool<?> pool = PoolConfig.deployPool(poolDef);
		objectPools.put(poolDef.getAbsolutePath(), pool);
		final String name = pool.getJmxName().getKeyProperty("name");
		log.info("<<< ObjectPool [{}] deployed from [{}]", name, poolDef);
		
	}
	
	/**
	 * Deploys the passed file as a JDBC data source
	 * @param dsDef The JDBC data source configuration file
	 */
	protected void deployJDBCDataSource(final File dsDef) {
		try {
			final Properties p = URLHelper.readProperties(URLHelper.toURL(dsDef));
			for(String key: p.stringPropertyNames()) {
				if(key.trim().equalsIgnoreCase("disabled")) {
					if(p.getProperty(key, "false").trim().equalsIgnoreCase("true")) {
						log.info("DataSource Definition [{}] is marked disabled. Skipping.", dsDef);
						return;
					}
				}
			}
			log.info(">>> Deploying DataSource from [{}]", dsDef);
			final HikariConfig config = new HikariConfig(p);
			config.setMetricRegistry(SharedMetricsRegistry.getInstance());
			final String name = dsDef.getName().substring(0, dsDef.getName().length()-3);
			config.setPoolName(name);
			config.setRegisterMbeans(true);
			final String dsClassName = p.getProperty("dataSourceClassName");
			log.info("DataSource Class Name: [{}]", dsClassName);
			try {
				Class.forName(dsClassName, true, Thread.currentThread().getContextClassLoader());
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
			
			final ManagedHikariDataSource ds = new ManagedHikariDataSource(config, new DataSourceListener(){
				@Override
				public void onDataSourceStopped(String poolName, String dataSourceCacheName, String groovySqlCacheName) {
					dataSources.remove(dsDef.getAbsolutePath());
				}
			});
			dataSources.put(dsDef.getAbsolutePath(), ds);
			gcache.put(ds.dsCacheKey, this);
			gcache.put(ds.groovydsCacheKey, ds.groovySql);
			
			successfulDeploys.increment();
			log.info("<<< DataSource [{}] deployed from [{}]", name, dsDef);
		} catch (Exception ex) {
			final HikariDataSource ds = dataSources.remove(dsDef.getAbsolutePath());
			if(ds!=null) {
				try { ds.close(); } catch (Exception x) {/* No Op */}
			}
			failedDeploys.increment();
			log.error("Failed to deploy datasource from [{}]", dsDef, ex);
			throw new RuntimeException("Failed to deploy datasource", ex);
		}
	}
	
	/**
	 * Undeploys the data source defined by the passed data source definition file.
	 * Ignored if not found.
	 * @param dsDef The data source definition file
	 */
	protected void undeploy(final File dsDef) {
		if(dsDef==null) throw new IllegalArgumentException("The passed file was null");
		final String ext = URLHelper.getFileExtension(dsDef);
		if(ext==null || ext.trim().isEmpty() || !POOL_EXT.contains(ext.toLowerCase())) {
			throw new IllegalArgumentException("The passed file [" + dsDef + "] does not have a recognized extension [" + ext + "]. Recognized extensions are: " + POOL_EXT);
		}
		if(!dsDef.canRead()) throw new IllegalArgumentException("The passed file [" + dsDef + "] cannot be read");
		final String key = dsDef.getAbsolutePath();
		if(dsDef.getName().toLowerCase().endsWith(".ds")) {
			final ManagedHikariDataSource ds = dataSources.remove(key);
			if(ds!=null) {
				log.info(">>> Stopping DataSource from [{}]", dsDef);
				try { ds.close(); } catch (Exception x) {/* No Op */}
				try { GlobalCacheService.getInstance().remove(ds.dsCacheKey); } catch (Exception x) {/* No Op */}
				try { GlobalCacheService.getInstance().remove(ds.groovydsCacheKey); } catch (Exception x) {/* No Op */}
				log.info("<<< DataSource [{}] stopped", dsDef);
			}
		} else {
			PoolConfig.undeployPool(dsDef);
			objectPools.remove(key);
		}
	}


	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.utils.file.FileChangeEventListener#onChange(java.io.File)
	 */
	@Override
	public void onChange(final File file) {
		collectorExecutionService.execute(new Runnable(){
			@Override
			public void run() {
				undeploy(file);
				try { 
					deploy(file);
				} catch (Exception ex) {
					log.error("Failed to deploy data source from [{}]",  file, ex);
				}						
			}
		});
	}


	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.utils.file.FileChangeEventListener#onDelete(java.io.File)
	 */
	@Override
	public void onDelete(final File file) {
		collectorExecutionService.execute(new Runnable(){
			@Override
			public void run() {
				undeploy(file);							
			}
		});
		
				
	}


	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.utils.file.FileChangeEventListener#onNew(java.io.File)
	 */
	@Override
	public void onNew(final File file) {
		collectorExecutionService.execute(new Runnable(){
			@Override
			public void run() {
				try { 
					deploy(file);
				} catch (Exception ex) {
					log.error("Failed to deploy data source from [{}]",  file, ex);
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


//	/**
//	 * {@inheritDoc}
//	 * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
//	 */
//	@Override
//	public void setApplicationContext(final ApplicationContext appCtx) throws BeansException {
//		this.appCtx = appCtx;
//		for(HikariDataSource ds: dataSources.values()) {
//			appCtx.getAutowireCapableBeanFactory().initializeBean(ds, "ds/" + ds.getPoolName());
//		}
//	}

}
