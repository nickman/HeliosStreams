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
import java.util.concurrent.TimeUnit;

import com.heliosapm.streams.collector.cache.CacheEventListener;
import com.heliosapm.streams.collector.cache.GlobalCacheService;
import com.heliosapm.utils.reflect.PrivateAccessor;

/**
 * <p>Title: DependencyManager</p>
 * <p>Description: Manages the lifecycle of declared dependencies in a managed script</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.groovy.DependencyManager</code></p>
 */

public class DependencyManager implements CacheEventListener {
	/** The script to manage dependencies for */
	protected final ManagedScript script;
	/** The global cache service reference */
	protected final GlobalCacheService cache;
	/** A map of managed fields in the script keyed by the cache key name */
	protected final Map<String, Field> depFields = new HashMap<String, Field>();
	/** A map of the dependency annotation instances on managed fields in the script keyed by the cache key name */
	protected final Map<String, Dependency> depDefs = new HashMap<String, Dependency>();
	
	/**
	 * Creates a new DependencyManager
	 * @param script The script to manage dependencies for
	 */
	public DependencyManager(final ManagedScript script) {
		this.script = script;
		cache = GlobalCacheService.getInstance();
		try {
			for(Field f: script.getClass().getDeclaredFields()) {
				final Dependency d = f.getAnnotation(Dependency.class);
				if(d!=null) {
					final String cacheKey = d.cacheKey().trim();
					depFields.put(cacheKey, f);
					depDefs.put(cacheKey, d);
					final Object o = cache.getOrNotify(cacheKey, d.type(), this, d.timeout(), d.unit());
					if(o==null) {
						script.addPendingDependency(cacheKey);
					} else {
						PrivateAccessor.setFieldValue(script, f, o);
					}
				}
			}
		} catch (Exception ex) {
			throw new RuntimeException("Failed to scan script [" + script.sourceFile + "] for dependencies", ex);
		}
	}
	
	/**
	 * Unregisters any still registered cache listeners
	 */
	public void close() {
		cache.removeCacheEventListener(this);
		depFields.clear();
		depDefs.clear();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.cache.CacheEventListener#onValueAdded(java.lang.String, java.lang.Object)
	 */
	@Override
	public void onValueAdded(final String key, final Object value) {
		// TODO Auto-generated method stub
		
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.cache.CacheEventListener#onValueRemoved(java.lang.String, java.lang.Object)
	 */
	@Override
	public void onValueRemoved(final String key, final Object removedValue) {
		// TODO Auto-generated method stub
		
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.cache.CacheEventListener#onValueReplaced(java.lang.String, java.lang.Object, java.lang.Object)
	 */
	@Override
	public void onValueReplaced(final String key, final Object oldValue, final Object newValue) {
		// TODO Auto-generated method stub
		
	}

}