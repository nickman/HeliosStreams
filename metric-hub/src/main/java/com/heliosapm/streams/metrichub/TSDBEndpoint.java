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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.codahale.metrics.CachedGauge;
import com.heliosapm.streams.sqlbinder.SQLWorker;

/**
 * <p>Title: TSDBEndpoint</p>
 * <p>Description: Expiring cache that queries and caches the online known OpenTSDB instance URIs.</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrichub.TSDBEndpoint</code></p>
 */

public class TSDBEndpoint {
	/** A map of endpoints keyed by the DB key they're connected to */
	private static final ConcurrentHashMap<String, TSDBEndpoint> endpoints = new ConcurrentHashMap<String, TSDBEndpoint>();
	/** Placeholder */
	private static final TSDBEndpoint PLACEHOLDER = new TSDBEndpoint();
	/** The sqlworker to poll the db when the cache expires */
	protected final SQLWorker sqlWorker;
	/** The cache of known on line servers */
	protected final CachedGauge<String[]> knownServers;
	/** flag indicating the ordering */
	protected final AtomicBoolean asc = new AtomicBoolean(true);
	
	
	/**
	 * Acquires the unique TSDBEndpoint for the passed SQLWorker
	 * @param sqlWorker The SQLWorker that will drive the endpoint lookup
	 * @return the unique TSDBEndpoint 
	 */
	public static TSDBEndpoint getEndpoint(final SQLWorker sqlWorker) {
		if(sqlWorker==null) throw new IllegalArgumentException("The passed SQLWorker was null");
		final String key = sqlWorker.getDBKey();
		TSDBEndpoint endpoint = endpoints.putIfAbsent(key, PLACEHOLDER);
		if(endpoint==null || endpoint==PLACEHOLDER) {
			endpoint = new TSDBEndpoint(sqlWorker);
		}		
		return endpoint;
	}
	
	private TSDBEndpoint() {
		sqlWorker = null;
		knownServers = null;
	}
	
	/**
	 * Creates a new TSDBEndpoint
	 * @param sqlWorker The SQLWorker that will drive the lookups
	 */
	private TSDBEndpoint(final SQLWorker sqlWorker) {
		this.sqlWorker = sqlWorker;
		knownServers = new CachedGauge<String[]>(60, TimeUnit.SECONDS) {
			@Override
			protected String[] loadValue() {
				final String order = (asc.getAndSet(asc.get()) ? "ASC" : "DESC"); 
				return sqlWorker.sqlForFormat(null, null, "SELECT HOST, PORT, URI FROM TSD_KNOWNSERVERS WHERE UP = 'Y' ORDER BY HOST " + order + ", PORT " + order, "http://##0##:##1##/##2##");
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
