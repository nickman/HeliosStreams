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
package com.heliosapm.streams.collector.groovy;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.heliosapm.streams.collector.cache.CacheEventListener;
import com.heliosapm.streams.collector.cache.GlobalCacheService;

/**
 * <p>Title: DependencyManager</p>
 * <p>Description: Manages the lifecycle of declared dependencies in a managed script</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.groovy.DependencyManager</code></p>
 * @param <T> The actual script type
 */

public class DependencyManager<T extends ManagedScript> implements CacheEventListener {
	/** The script to manage dependencies for */
	protected final T script;
	/** The script fully qualified script name */
	protected final String scriptName;
	
	/** The script's logger */
	protected final Logger log;
	
	/** The global cache service reference */
	protected final GlobalCacheService cache;
	/** A map of managed fields in the script keyed by the cache key name */
	protected final Map<String, Field> depFields = new HashMap<String, Field>();
	/** A map of the dependency annotation instances on managed fields in the script keyed by the cache key name */
	protected final Map<String, Dependency> depDefs = new HashMap<String, Dependency>();
	/** A map of atomic field updaters for updating each field keyed by the cache key name */
	protected final Map<String, AtomicReferenceFieldUpdater<T, Object>> depUpdaters = new HashMap<String, AtomicReferenceFieldUpdater<T, Object>>();
	
	
	
	/**
	 * Creates a new DependencyManager
	 * @param script The script to manage dependencies for
	 * @param type The script type
	 */
	public DependencyManager(final T script, final Class<T> type) {
		this.script = script;
		scriptName = this.script.getClass().getName();
		log = LogManager.getLogger(type);

		cache = GlobalCacheService.getInstance();
		try {
			for(Field f: type.getDeclaredFields()) {
				final Dependency d = f.getAnnotation(Dependency.class);
				if(d!=null) {
					final String cacheKey = script.getBinding().getVariables().get(d.value().trim()).toString();
					depFields.put(cacheKey, f);
					depDefs.put(cacheKey, d);
					f.setAccessible(true);
					final AtomicReferenceFieldUpdater<T, Object> updater =  AtomicReferenceFieldUpdater.newUpdater(type, Object.class, f.getName());
					depUpdaters.put(cacheKey, updater);
					final Object o = cache.getOrNotify(cacheKey, d.type(), this, d.timeout(), d.unit());
					if(o==null) {
						script.addPendingDependency(cacheKey);
					} else {
						log.info("Seting dependent value [{}] on [{}] from cache entry [{}]", o.getClass().getName(), scriptName, cacheKey);
						updater.set(script, o);
						//PrivateAccessor.setFieldValue(script, f.getName(), o);
						cache.addCacheEventListener(this, cacheKey);
						log.info("Dependent value [{}] initialized on [{}] from cache entry [{}]", o.getClass().getName(), scriptName, cacheKey);
					}
				}
			}
		} catch (Exception ex) {
			throw new RuntimeException("Failed to scan script [" + script.sourceReader + "] for dependencies", ex);
		}
	}
	
	/**
	 * Unregisters any still registered cache listeners
	 */
	public void close() {
		cache.removeCacheEventListener(this);
		depFields.clear();
		depDefs.clear();
		depUpdaters.clear();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.cache.CacheEventListener#onValueAdded(java.lang.String, java.lang.Object)
	 */
	@Override
	public void onValueAdded(final String key, final Object value) {
		final AtomicReferenceFieldUpdater<T, Object> updater = depUpdaters.get(key);
		if(updater!=null) {
			updater.set(script, value);
			script.getBinding().setVariable("_" + depFields.get(key).getName(), value);
			script.removePendingDependency(key);
			log.info("Dependent value [{}] initialized on [{}] from cache entry [{}]", value.getClass().getName(), scriptName, key);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.cache.CacheEventListener#onValueRemoved(java.lang.String, java.lang.Object)
	 */
	@Override
	public void onValueRemoved(final String key, final Object removedValue) {
		final AtomicReferenceFieldUpdater<T, Object> updater = depUpdaters.get(key);
		if(updater!=null) {
			script.addPendingDependency(key);
			updater.set(script, null);		
			script.getBinding().getVariables().remove("_" + depFields.get(key).getName());
			log.info("Dependent value removed from [{}] based on cleared cache entry [{}]", scriptName, key);
		}		
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.cache.CacheEventListener#onValueReplaced(java.lang.String, java.lang.Object, java.lang.Object)
	 */
	@Override
	public void onValueReplaced(final String key, final Object oldValue, final Object newValue) {
		final AtomicReferenceFieldUpdater<T, Object> updater = depUpdaters.get(key);
		if(updater!=null) {
			script.removePendingDependency(key);
			updater.set(script, newValue);
			script.getBinding().setVariable("_" + depFields.get(key).getName() , newValue);
			log.info("Dependent value [{}] replaced on [{}] from cache entry [{}]", newValue.getClass().getName(), scriptName, key);
		}				
	}

}