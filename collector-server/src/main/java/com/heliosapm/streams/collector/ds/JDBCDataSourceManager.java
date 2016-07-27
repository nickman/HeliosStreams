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
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import com.heliosapm.streams.collector.cache.GlobalCacheService;
import com.heliosapm.streams.common.metrics.SharedMetricsRegistry;
import com.heliosapm.utils.url.URLHelper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import groovy.sql.Sql;

/**
 * <p>Title: JDBCDataSourceManager</p>
 * <p>Description: Service to manage JDBC data sources</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.ds.JDBCDataSourceManager</code></p>
 */

public class JDBCDataSourceManager {
	
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** The data source definition directory */
	protected final File dsDirectory;
	/** A cache of datasources keyed by the definition file name */
	protected final NonBlockingHashMap<String, HikariDataSource> dataSources = new NonBlockingHashMap<String, HikariDataSource>();
	/** The global cache */
	protected final GlobalCacheService gcache = GlobalCacheService.getInstance(); 


	/**
	 * Creates a new JDBCDataSourceManager
	 * @param dsDirectory The data source definition directory
	 */
	public JDBCDataSourceManager(final File dsDirectory) {
		log.info(">>>>> Starting JDBCDataSourceManager...");
		this.dsDirectory = dsDirectory;
		for(File f: this.dsDirectory.listFiles()) {
			deploy(f);
		}
		log.info("<<<<< JDBCDataSourceManager started.");
	}
	
	
	protected void deploy(final File dsDef) {
		if(dsDef==null) throw new IllegalArgumentException("The passed file was null");
		if(!dsDef.getName().toLowerCase().endsWith(".ds")) return;
		if(!dsDef.canRead()) throw new IllegalArgumentException("The passed file [" + dsDef + "] cannot be read");		
		log.info(">>> Deploying DataSource from [{}]", dsDef);
		final Properties p = URLHelper.readProperties(URLHelper.toURL(dsDef));
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
		
		final HikariDataSource ds = new HikariDataSource(config);
		dataSources.put(dsDef.getAbsolutePath(), ds);
		gcache.put("ds/" + name, ds);
		gcache.put("groovyds/" + name, new Sql(ds));
		log.info("<<< DataSource [{}] deployed from [{}]", name, dsDef);
	}

}
