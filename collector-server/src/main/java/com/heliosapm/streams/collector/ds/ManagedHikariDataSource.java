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
package com.heliosapm.streams.collector.ds;

import org.cliffc.high_scale_lib.NonBlockingHashSet;

import com.heliosapm.streams.collector.cache.GlobalCacheService;
import com.heliosapm.utils.jmx.SharedNotificationExecutor;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import groovy.sql.Sql;

/**
 * <p>Title: ManagedHikariDataSource</p>
 * <p>Description: An extension of {@link HikariDataSource} that emits shutdown events</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.ds.ManagedHikariDataSource</code></p>
 */

public class ManagedHikariDataSource extends HikariDataSource {
	
	/** The global cache prefix for the data source */
	public static final String CACHE_DS_PREFIX = "ds/";
	/** The global cache prefix for the data source based Groovy SQL */
	public static final String CACHE_GSQL_PREFIX = "groovyds/";
	
	/** The groovy sql wrapping the data source */
	protected final Sql groovySql;
	/** The cache where the ds and groovy sql are registered */
	protected final GlobalCacheService cache;
	/** The pool name */
	protected final String poolName;
	/** The data source cache key */
	protected final String dsCacheKey;
	/** The data source's groovy sql cache key */
	protected final String groovydsCacheKey;
	/** A set of data source listeners to be notified on events emitted from this datasource */
	protected final NonBlockingHashSet<DataSourceListener> listeners = new NonBlockingHashSet<DataSourceListener>();

	/**
	 * Creates a new ManagedHikariDataSource
	 * @param configuration the data source configuration
	 * @param cache The cache where the ds and groovy sql are registered
	 * @param listeners Optional close listeners
	 */
	public ManagedHikariDataSource(final HikariConfig configuration, final GlobalCacheService cache, final DataSourceListener... listeners) {
		super(configuration);
		poolName = getPoolName();
		groovySql = new Sql(this);
		this.cache = cache;
		dsCacheKey = CACHE_DS_PREFIX + poolName;
		groovydsCacheKey = CACHE_GSQL_PREFIX + poolName;
		cache.put(dsCacheKey, this);
		cache.put(groovydsCacheKey, groovySql);
		if(listeners!=null) {
			for(DataSourceListener listener: listeners) {
				if(listener==null) continue;
				this.listeners.add(listener);
			}
		}
	}
	
	/**
	 * Registers a new data source listener
	 * @param listener The listener to add
	 */
	public void addListener(final DataSourceListener listener) {
		if(listener!=null) {
			this.listeners.add(listener);
		}
	}
	
	/**
	 * Removes a registered data source listener.
	 * Note that all listeners will be removed after being notified of datasource close.
	 * @param listener The listener to remove
	 */
	public void removeListener(final DataSourceListener listener) {
		if(listener!=null) {
			this.listeners.add(listener);
		}
	}
	
	
	/**
	 * <p>Closes the groovy sql instance and removes all related entries from cache, calls super, then emits a close event</p>
	 * {@inheritDoc}
	 * @see com.zaxxer.hikari.HikariDataSource#shutdown()
	 */
	@Override
	public void shutdown() {
		cache.remove(dsCacheKey);
		cache.remove(groovydsCacheKey);		
		try { groovySql.close(); } catch (Exception x) {/* No Op */}
		super.shutdown();		
		for(final DataSourceListener listener: listeners) {
			SharedNotificationExecutor.getInstance().execute(new Runnable(){
				@Override
				public void run() {
					listener.onDataSourceStopped(poolName, dsCacheKey, groovydsCacheKey);					
				}
			});
		}
		listeners.clear();
	}


	/**
	 * Returns the data source cache key
	 * @return the data source cache key
	 */
	public String getDsCacheKey() {
		return dsCacheKey;
	}

	/**
	 * Returns the groovy sql cache key
	 * @return the groovy sql cache key
	 */
	public String getGroovydsCacheKey() {
		return groovydsCacheKey;
	}

	
	
}
