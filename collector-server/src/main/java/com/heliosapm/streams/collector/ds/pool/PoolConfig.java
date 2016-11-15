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
package com.heliosapm.streams.collector.ds.pool;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.Properties;
import java.util.function.BiFunction;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.heliosapm.streams.collector.cache.GlobalCacheService;
import com.heliosapm.utils.lang.StringHelper;
import com.heliosapm.utils.url.URLHelper;

/**
 * <p>Title: PoolConfig</p>
 * <p>Description: A functional enumeration of the configuration items for a generic pool configuration</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.ds.pool.PoolConfig</code></p>
 */

public enum PoolConfig implements BiFunction<GenericObjectPoolConfig, Object, Void> {
	/** The maximum number of idle objects in the pool */
	MAXIDLE{public Void apply(final GenericObjectPoolConfig p, final Object o) {p.setMaxIdle(Integer.parseInt(o.toString().trim())); return null;}},
	/** The maximum total number of objects in the pool */
	MAXTOTAL{public Void apply(final GenericObjectPoolConfig p, final Object o) {p.setMaxTotal(Integer.parseInt(o.toString().trim())); return null;}},
	/** The minimum number of idle objects in the pool */
	MINIDLE{public Void apply(final GenericObjectPoolConfig p, final Object o) {p.setMinIdle(Integer.parseInt(o.toString().trim())); return null;}},
	/** Indicates if the pool should block when exhausted */
	BLOCKONEX{public Void apply(final GenericObjectPoolConfig p, final Object o) {p.setBlockWhenExhausted(o==null ? true : Boolean.parseBoolean(o.toString().trim())); return null;}},
	/** The max wait time in ms. */
	MAXWAIT{public Void apply(final GenericObjectPoolConfig p, final Object o) {p.setMaxWaitMillis(Long.parseLong(o.toString().trim())); return null;}},
	/** Indicates if objects should be tested when borrowed */
	TESTONGET{public Void apply(final GenericObjectPoolConfig p, final Object o) {p.setTestOnBorrow(o==null ? true : Boolean.parseBoolean(o.toString().trim())); return null;}},
	/** Indicates if objects should be tested when created */
	TESTONCREATE{public Void apply(final GenericObjectPoolConfig p, final Object o) {p.setTestOnCreate(o==null ? true : Boolean.parseBoolean(o.toString().trim())); return null;}},
	/** The name of the pool */
	NAME{public Void apply(final GenericObjectPoolConfig p, final Object o) {p.setJmxNamePrefix(",name=" + o.toString().trim()); return null;}};
	
	
	private static final GenericObjectPoolConfig DEFAULT_CONFIG;
	
	/** The configuration key for the pooled object factory */
	public static final String POOLED_OBJECT_FACTORY_KEY = "factory"; 
	
	
	static {
		DEFAULT_CONFIG = new GenericObjectPoolConfig();
		DEFAULT_CONFIG.setMaxIdle(1);
		DEFAULT_CONFIG.setMinIdle(1);
		DEFAULT_CONFIG.setMaxTotal(5);
		DEFAULT_CONFIG.setMaxWaitMillis(5000);
		DEFAULT_CONFIG.setBlockWhenExhausted(true);
		DEFAULT_CONFIG.setFairness(true);
		DEFAULT_CONFIG.setJmxEnabled(true);
		DEFAULT_CONFIG.setJmxNameBase("com.heliosapm.streams.collector.ds.pool:service=ObjectPool");
		//DEFAULT_CONFIG.setJmxNamePrefix("");
		
		DEFAULT_CONFIG.setLifo(false);
		DEFAULT_CONFIG.setTestOnBorrow(true);
		DEFAULT_CONFIG.setTestOnCreate(true);
		DEFAULT_CONFIG.setTestOnReturn(false);		
		
		// ObjectName:    objName = new ObjectName(base + jmxNamePrefix);
	}
	
	public static void main(String[] args) {
		for(PoolConfig p: values()) {
			System.out.println(p.name().toLowerCase() + "=");
		}
	}
	
	/**
	 * Decodes the passed name to a PoolConfig
	 * @param name the name to decode
	 * @return the decoded PoolConfig
	 */
	public static PoolConfig decode(final String name) {
		if(name==null || name.trim().isEmpty()) throw new IllegalArgumentException("The passed name was null or empty");
		try {
			return valueOf(name.trim().toUpperCase());
		} catch (Exception ex) {
			throw new IllegalArgumentException("The passed name [" + name + "] is not a valid PoolConfig");
		}
	}
	
	/**
	 * Determines if the passed name is a valid PoolConfig
	 * @param name The name to test
	 * @return true if valid, false otherwise
	 */
	public static boolean isPoolConfig(final String name) {
		try {
			decode(name);
			return true;
		} catch (Exception ex) {
			return false;
		}
	}
	
	
	
	/**
	 * Creates and deploys an ObjectPool based on the passed config props
	 * @param configProps The configuration properties file
	 * @return the created GenericObjectPool
	 */
	public static GenericObjectPool<?> deployPool(final File configProps) {
		final Properties p = URLHelper.readProperties(URLHelper.toURL(configProps));
		if(!p.containsKey("name")) {
			p.setProperty("name", StringHelper.splitString(configProps.getName(), '.', true)[0]);
		}
		return deployPool(p);
	}

	/**
	 * Creates and deploys an ObjectPool based on the passed config props
	 * @param configProps The configuration properties
	 * @return the created GenericObjectPool
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static GenericObjectPool<?> deployPool(final Properties p) {
		final String factoryName = (String)p.remove(POOLED_OBJECT_FACTORY_KEY);
		final String poolName = p.getProperty(NAME.name().toLowerCase());
		if(poolName==null || poolName.trim().isEmpty()) throw new RuntimeException("Pool was not assigned a name");
		if(factoryName==null || factoryName.trim().isEmpty()) throw new RuntimeException("No pooled object factory defined");
		final GenericObjectPoolConfig cfg = DEFAULT_CONFIG.clone();
		try {			
			final Class<PooledObjectFactoryBuilder<?>> clazz = (Class<PooledObjectFactoryBuilder<?>>)Class.forName(factoryName);
			final Constructor<PooledObjectFactoryBuilder<?>> ctor = clazz.getDeclaredConstructor(Properties.class);
			final PooledObjectFactoryBuilder<?> factory = ctor.newInstance(p);
			for(final String key: p.stringPropertyNames()) {
				if(isPoolConfig(key)) {
					final PoolConfig pc = decode(key);
					pc.apply(cfg, p.get(key));
				}
			}
			final GenericObjectPool<?> pool = new GenericObjectPool(factory.factory(), cfg);
			GlobalCacheService.getInstance().put("pool/" + poolName, pool);
			return pool;
		} catch (Exception ex) {
			throw new RuntimeException("Failed to create GenericObjectPool from properties [" + p + "]", ex);
		}
	}
	
	/**
	 * Undeploys the named pool
	 * @param name The name of the pool
	 */
	public static void undeployPool(final String name) {
		if(name==null || name.trim().isEmpty()) throw new IllegalArgumentException("The passed pool name was null or empty");
		final String key = "pool/" + name;
		final GenericObjectPool<?> pool = GlobalCacheService.getInstance().remove(key);
		if(pool!=null) {
			try {
				pool.close();
			} catch (Exception x) {/* No Op */} 
		}		
	}
	
	/**
	 * Undeploys the pool defined in the passed properties
	 * @param config The pool configuration properties
	 */
	public static void undeployPool(final Properties config) {
		final String poolName = config.getProperty(NAME.name().toLowerCase());
		if(poolName==null || poolName.trim().isEmpty()) throw new RuntimeException("The passed properties do not have a pool name");
		undeployPool(poolName);
		
	}
	
	
	/**
	 * Undeploys the pool defined in the passed file
	 * @param file The pool configuration file
	 */
	public static void undeployPool(final File file) {
		if(file==null) throw new IllegalArgumentException("The passed file was null");
		if(!file.canRead()) throw new IllegalArgumentException("Cannot read file [" + file + "]");
		undeployPool(URLHelper.readProperties(URLHelper.toURL(file)));
		
	}
	

	
}
