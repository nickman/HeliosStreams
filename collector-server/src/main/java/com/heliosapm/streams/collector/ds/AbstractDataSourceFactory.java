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

import java.util.regex.Pattern;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.codahale.metrics.ObjectNameFactory;
import com.fasterxml.jackson.databind.JsonNode;



/**
 * <p>Title: AbstractDataSourceFactory</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.ds.AbstractDataSourceFactory</code></p>
 */

public abstract class AbstractDataSourceFactory<T> implements DataSourceFactory<T>, ObjectNameFactory {
	/** Dot splitter */
	public static Pattern DOT_SPLITTER = Pattern.compile("\\.");

	
	/**
	 * Reads the generic object pool configuration from the passed JSONObject
	 * @param jsonConfig The json configuration for the current data source
	 * @return the pool config
	 */
	protected GenericObjectPoolConfig getPoolConfig(final JsonNode jsonConfig) {
		if(jsonConfig==null) throw new IllegalArgumentException("The passed JSONObject was null");
		final GenericObjectPoolConfig config = new  GenericObjectPoolConfig();
		if(jsonConfig.has("maxPoolSize")) config.setMaxTotal(jsonConfig.get("maxPoolSize").intValue());
		if(jsonConfig.has("maxPoolIdle")) config.setMaxIdle(jsonConfig.get("maxPoolIdle").intValue());
		if(jsonConfig.has("minPoolIdle")) config.setMinIdle(jsonConfig.get("minPoolIdle").intValue());
		if(jsonConfig.has("registerMbeans")) config.setJmxEnabled(jsonConfig.get("registerMbeans").booleanValue());
		if(jsonConfig.has("fair")) config.setFairness(jsonConfig.get("fair").booleanValue());
		if(jsonConfig.has("lifo")) config.setLifo(jsonConfig.get("lifo").booleanValue());
		if(jsonConfig.has("maxWait")) config.setMaxWaitMillis(jsonConfig.get("maxWait").longValue());
		
		final boolean testWhileIdle;
		if(jsonConfig.has("testWhileIdle")) {
			testWhileIdle = jsonConfig.get("testWhileIdle").booleanValue();
		} else {
			testWhileIdle = false;
		}
		if(testWhileIdle) {
			config.setTestWhileIdle(true);
			long testPeriod = 15000;
			if(jsonConfig.has("testPeriod")) {
				testPeriod = jsonConfig.get("testPeriod").longValue();
			}
			config.setTimeBetweenEvictionRunsMillis(testPeriod);			
		} else {
			config.setTestWhileIdle(false);
		}
		// ALWAYS test on borrow
		config.setTestOnBorrow(true);
		config.setTestOnCreate(true);
		config.setTestOnReturn(false);
		return config;
	}
	
	


}
