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
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.util.EnumSet;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.SwallowedExceptionListener;
import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.heliosapm.streams.collector.cache.GlobalCacheService;
import com.heliosapm.utils.lang.StringHelper;
import com.heliosapm.utils.url.URLHelper;

/**
 * <p>Title: PoolConfig</p>
 * <p>Description: A functional enumeration of the configuration items for a generic pool configuration</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.ds.pool.PoolConfig</code></p>
 * <p><pre>
 * maxidle=
 * maxtotal=
 * minidle=
 * blockonex=
 * maxwait=
 * testonget=
 * testoncreate=
 * name=
 * logabandon=true
 * remaoveabandon=true
 * timeoutabandon=60
 * trackabandon=false 
 * </pre></p>
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
	
	
	/**
	 * <p>Title: Abandoned</p>
	 * <p>Description: Functional enum to configure the pool's abandoned object policy</p> 
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>com.heliosapm.streams.collector.ds.pool.PoolConfig.Abandoned</code></p>
	 */
	public static enum Abandoned implements BiFunction<AbandonedConfig, Object, Void> {
		/** Indicates if abandoned objects should be logged */
		LOGABANDON("true"){public Void apply(final AbandonedConfig p, final Object o) {p.setLogAbandoned(o==null ? true : Boolean.parseBoolean(o.toString().trim())); return null;}},
		/** Flag to remove abandoned objects if they exceed the removeAbandonedTimeout when borrowObject is invoked. */
		REMAOVEABANDON("true"){public Void apply(final AbandonedConfig p, final Object o) {p.setRemoveAbandonedOnBorrow(o==null ? true : Boolean.parseBoolean(o.toString().trim())); return null;}},
		/** The timeout in seconds before an abandoned object can be removed */
		TIMEOUTABANDON("60"){public Void apply(final AbandonedConfig p, final Object o) {p.setRemoveAbandonedTimeout(Integer.parseInt(o.toString().trim())); return null;}},
		/** Enables debug tracking of all borrowed objects */
		TRACKABANDON("false"){public Void apply(final AbandonedConfig p, final Object o) {p.setUseUsageTracking(o==null ? true : Boolean.parseBoolean(o.toString().trim())); return null;}};
		
		private Abandoned(final String defaultValue) {
			this.defaultValue = defaultValue;
		}
		
		/** The default value */
		public final String defaultValue;
		
		/**
		 * Decodes the passed name to a Abandoned
		 * @param name the name to decode
		 * @return the decoded Abandoned
		 */
		public static Abandoned decode(final String name) {
			if(name==null || name.trim().isEmpty()) throw new IllegalArgumentException("The passed name was null or empty");
			try {
				return valueOf(name.trim().toUpperCase());
			} catch (Exception ex) {
				throw new IllegalArgumentException("The passed name [" + name + "] is not a valid Abandoned");
			}
		}
		
		/**
		 * Determines if the passed name is a valid Abandoned
		 * @param name The name to test
		 * @return true if valid, false otherwise
		 */
		public static boolean isAbandoned(final String name) {
			try {
				decode(name);
				return true;
			} catch (Exception ex) {
				return false;
			}
		}
		
		/**
		 * Creates a new AbandonedConfig from the passed properties
		 * @param p The config properties for the pool
		 * @return the AbandonedConfig
		 */
		public static AbandonedConfig create(final Properties p) {
			final AbandonedConfig ac = new AbandonedConfig();
			final EnumSet<Abandoned> unapplied = EnumSet.allOf(Abandoned.class);
			for(final String key: p.stringPropertyNames()) {
				if(isAbandoned(key)) {
					final Abandoned a = decode(key);
					final String cfg = p.getProperty(key, "").trim();
					try {
						a.apply(ac, cfg);
					} catch (Exception ex) {
						a.apply(ac, a.defaultValue);
					}
					unapplied.remove(a);
				}
			}
			for(Abandoned a: unapplied) {
				a.apply(ac, a.defaultValue);
			}
			if(ac.getLogAbandoned()) {
				ac.setLogWriter(new PrintWriter(System.err));
			}
			return ac;
		}
		
	}
	
	/** Static class logger */
	public static final Logger LOG = LogManager.getLogger(PoolConfig.class);
	
	private static final SwallowedExceptionListener EX_LISTENER = new SwallowedExceptionListener() {

		@Override
		public void onSwallowException(final Exception e) {
			e.printStackTrace(System.err);
			LOG.error("Swallowed Exception", e);
			
		}
		
	};
	
	/** The default pooled object factory implementation package */
	public static final String DEFAULT_FACTORY_PACKAGE = "com.heliosapm.streams.collector.ds.pool.impls.";

	
	/** The default pool configuration */
	private static final GenericObjectPoolConfig DEFAULT_CONFIG;
	/** Placeholder pool */
	private static final GenericObjectPool<?> PLACEHOLDER;
	
	/** The configuration key for the pooled object factory */
	public static final String POOLED_OBJECT_FACTORY_KEY = "factory"; 
	
	/** All active pools keyed by name */
	private static final ConcurrentHashMap<File, GenericObjectPool<?>> pools = new ConcurrentHashMap<File, GenericObjectPool<?>>(512, 0.75f, Runtime.getRuntime().availableProcessors()); 
	
	
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
		
		DEFAULT_CONFIG.setLifo(true);
		DEFAULT_CONFIG.setTestOnBorrow(true);
		DEFAULT_CONFIG.setTestOnCreate(true);
		DEFAULT_CONFIG.setTestOnReturn(false);
		
		
		PLACEHOLDER = new GenericObjectPool<Object>(new BasePooledObjectFactory<Object>() {
			@Override
			public Object create() throws Exception {
				return null;
			}

			@Override
			public PooledObject<Object> wrap(Object obj) {
				return null;
			}
		});
		
		// ObjectName:    objName = new ObjectName(base + jmxNamePrefix);
	}
	
	public static void main(String[] args) {
		for(PoolConfig p: values()) {
			System.out.println(p.name().toLowerCase() + "=");
		}
		for(Abandoned p: Abandoned.values()) {
			System.out.println(p.name().toLowerCase() + "=" + p.defaultValue);
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
		if(configProps==null) throw new IllegalArgumentException("The passed file was null");
		if(!configProps.canRead()) throw new IllegalArgumentException("The passed file [" + configProps + "] cannot be read");
		GenericObjectPool<?> pool = pools.putIfAbsent(configProps, PLACEHOLDER);
		if(pool==null || pool==PLACEHOLDER) {
			final Properties p = URLHelper.readProperties(URLHelper.toURL(configProps));
			if(!p.containsKey("name")) {
				p.setProperty("name", StringHelper.splitString(configProps.getName(), '.', true)[0]);
			}
			try {
				pool = deployPool(p);
				pools.replace(configProps, pool);
				return pool;
			} catch (Exception ex) {
				final String msg = "Failed to deploy object pool from file [" + configProps + "]";
				LOG.error(msg, ex);
				throw new RuntimeException(msg, ex);
			}
		} else {
			throw new RuntimeException("The pool defined in the file [" + configProps + "] has already been deployed");
		}
	}

	/**
	 * Creates and deploys an ObjectPool based on the passed config props
	 * @param p The configuration properties
	 * @return the created GenericObjectPool
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static GenericObjectPool<?> deployPool(final Properties p) {
		final String factoryName = (String)p.remove(POOLED_OBJECT_FACTORY_KEY);
		final String poolName = p.getProperty(NAME.name().toLowerCase());
		if(poolName==null || poolName.trim().isEmpty()) throw new RuntimeException("Pool was not assigned a name");
		if(factoryName==null || factoryName.trim().isEmpty()) throw new RuntimeException("No pooled object factory defined");
		final GenericObjectPoolConfig cfg = DEFAULT_CONFIG.clone();
		try {			
			final Class<PooledObjectFactoryBuilder<?>> clazz = loadFactoryClass(factoryName);
			final Constructor<PooledObjectFactoryBuilder<?>> ctor = clazz.getDeclaredConstructor(Properties.class);
			final PooledObjectFactoryBuilder<?> factory = ctor.newInstance(p);
			for(final String key: p.stringPropertyNames()) {
				if(isPoolConfig(key)) {
					final PoolConfig pc = decode(key);
					pc.apply(cfg, p.get(key));
				}
			}
			final PooledObjectFactory<?> pooledObjectFactory = factory.factory();
			GenericObjectPool<?> pool = new GenericObjectPool(pooledObjectFactory, cfg);
			pool.setSwallowedExceptionListener(EX_LISTENER);
			if(factory instanceof PoolAwareFactory) {
				((PoolAwareFactory)factory).setPool(pool);
			}
			GlobalCacheService.getInstance().put("pool/" + poolName, pool);
			pool.setAbandonedConfig(Abandoned.create(p));
			pool.preparePool();
			return pool;
		} catch (Exception ex) {
			throw new RuntimeException("Failed to create GenericObjectPool from properties [" + p + "]", ex);
		}			
	}
	
	
	@SuppressWarnings("unchecked")
	private static Class<PooledObjectFactoryBuilder<?>> loadFactoryClass(final String className) throws ClassNotFoundException {
		try {
			return (Class<PooledObjectFactoryBuilder<?>>)Class.forName(className);
		} catch (ClassNotFoundException cne) {
			return (Class<PooledObjectFactoryBuilder<?>>)Class.forName(DEFAULT_FACTORY_PACKAGE + className);
		}
	}
	
	/**
	 * Undeploys the named pool
	 * @param name The name of the pool
	 */
	private static void undeployPool(final String name) {
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
	private static void undeployPool(final Properties config) {
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
		if(pools.remove(file)!=null) {
			final Properties p = URLHelper.readProperties(URLHelper.toURL(file));
			final String poolName = p.getProperty(NAME.name().toLowerCase());
			if(poolName==null) {
				p.setProperty("name", StringHelper.splitString(file.getName(), '.', true)[0]);
			}
			undeployPool(p);
		}		
	}
	

	
}
