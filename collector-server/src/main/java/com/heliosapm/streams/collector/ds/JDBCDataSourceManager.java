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
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
//import org.springframework.beans.BeansException;
//import org.springframework.beans.factory.support.DefaultListableBeanFactory;
//import org.springframework.context.ApplicationContext;
//import org.springframework.context.ApplicationContextAware;

import com.heliosapm.streams.collector.cache.GlobalCacheService;
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
//	/** The spring app context */
//	protected ApplicationContext appCtx = null;


	/**
	 * Creates a new JDBCDataSourceManager
	 * @param dsDirectory The data source definition directory
	 * @param collectorExecutionService the executor service to use for ds deployments
	 */
	public JDBCDataSourceManager(final File dsDirectory, final CollectorExecutionService collectorExecutionService) {
		log.info(">>>>> Starting JDBCDataSourceManager...");
		this.dsDirectory = dsDirectory;
		this.collectorExecutionService = collectorExecutionService;
		finder = FileFinder.newFileFinder(dsDirectory.getAbsolutePath())
		.maxDepth(5)
		.filterBuilder()
		.caseInsensitive(false)
		.endsWithMatch(".ds")
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
	 * Deploys the passed data source definition file
	 * @param dsDef the data source definition file
	 */
	protected void deploy(final File dsDef) {
		if(dsDef==null) throw new IllegalArgumentException("The passed file was null");
		if(!dsDef.getName().toLowerCase().endsWith(".ds")) return;
		if(!dsDef.canRead()) throw new IllegalArgumentException("The passed file [" + dsDef + "] cannot be read");		
		
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
		if(dsDef!=null) {
			final String key = dsDef.getAbsolutePath();
			final ManagedHikariDataSource ds = dataSources.remove(key);
			if(ds!=null) {
				log.info(">>> Stopping DataSource from [{}]", dsDef);
				try { ds.close(); } catch (Exception x) {/* No Op */}
				try { GlobalCacheService.getInstance().remove(ds.dsCacheKey); } catch (Exception x) {/* No Op */}
				try { GlobalCacheService.getInstance().remove(ds.groovydsCacheKey); } catch (Exception x) {/* No Op */}
//				if(appCtx!=null) {
//					try {
//						((DefaultListableBeanFactory)appCtx.getAutowireCapableBeanFactory()).destroySingleton("ds/" + ds.getPoolName());
//					} catch (Exception x) {/* No Op */}
//					
//				}
				log.info("<<< DataSource [{}] stopped", dsDef);
			}
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
