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
package com.heliosapm.streams.metrichub;

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.CachedGauge;
import com.heliosapm.streams.sqlbinder.SQLWorker;

/**
 * <p>Title: TSDBEndpoint</p>
 * <p>Description: Expiring cache that queries and caches the online known OpenTSDB instance URIs.</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrichub.TSDBEndpoint</code></p>
 */

public class TSDBEndpoint {
	/** The sqlworker to poll the db when the cache expires */
	protected final SQLWorker sqlWorker;
	/** The cache of known on line servers */
	protected final CachedGauge<String[]> knownServers;
	
	/**
	 * Creates a new TSDBEndpoint
	 */
	public TSDBEndpoint(final SQLWorker sqlWorker) {
		this.sqlWorker = sqlWorker;
		knownServers = new CachedGauge<String[]>(60, TimeUnit.SECONDS) {
			@Override
			protected String[] loadValue() {
				return sqlWorker.sqlForFormat(null, null, "SELECT HOST, PORT, URI FROM TSD_KNOWNSERVERS WHERE UP = 'Y'", "http://##0##:##1##/##2##");
			}
		};
	}
	
	/**
	 * Returns the known up server api URLs
	 * @return an array of URLS
	 */
	public String[] getUpServers() {
		return knownServers.getValue(); 
	}

}
