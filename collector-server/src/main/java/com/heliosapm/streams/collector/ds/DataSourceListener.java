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

/**
 * <p>Title: DataSourceListener</p>
 * <p>Description: Defines a class that wants to listen on data source events</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.ds.DataSourceListener</code></p>
 */

public interface DataSourceListener {
//	/**
//	 * Callback when the {@link JDBCDataSourceManager} starts a new data source
//	 * @param poolName The name of the deployed pool
//	 * @param dataSourceCacheName The cache key where the data source can be accessed
//	 * @param groovySqlCacheName The cache key where the data source's groovy sql can be accessed
//	 */
//	public void onDataSourceStarted(final String poolName, final String dataSourceCacheName, final String groovySqlCacheName);
	
	/**
	 * Callback when a {@link JDBCDataSourceManager} deployed data source is shutdown
	 * @param poolName The name of the closed pool
	 * @param dataSourceCacheName The cache key where the data source was registered
	 * @param groovySqlCacheName The cache key where the data source's groovy sql was registered
	 */
	public void onDataSourceStopped(final String poolName, final String dataSourceCacheName, final String groovySqlCacheName);
	
}
