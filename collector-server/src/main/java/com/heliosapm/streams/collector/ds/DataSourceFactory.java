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

import com.fasterxml.jackson.databind.JsonNode;

/**
 * <p>Title: DataSourceFactory</p>
 * <p>Description: Defines a dynamic non-datasource factory</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.ds.DataSourceFactory</code></p>
 */

public interface DataSourceFactory<T> {
	/**
	 * Creates a new data source or object pool
	 * @param json The json configuration object
	 */
	public T createDataSource(final JsonNode json);

	
	/**
	 * Returns the type name
	 * @return the type name
	 */
	public String getTypeName();
	
	/**
	 * Closes a data source created by this factory
	 * @param dsName the name of a data source created by this factory
	 * @return true if the data source was found and closed, false otherwise
	 */
	public boolean close(String dsName);
	

}
