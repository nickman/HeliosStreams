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
package com.heliosapm.streams.opentsdb.mocks;

import java.util.EnumMap;
import java.util.Map;

import javax.management.ObjectName;

import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.reflect.PrivateAccessor;

import net.opentsdb.core.TSDB;
import net.opentsdb.uid.UniqueId;

/**
 * <p>Title: UniqueIdRegistry</p>
 * <p>Description: Service to expose the TSDB's {@link UniqueId} instances so all consumers are using the same instance and to provide JMX based cache stats</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.opentsdb.mocks.UniqueIdRegistry</code></p>
 */

public class UniqueIdRegistry implements UniqueIdRegistryMXBean {
	/** The singleton instance */
	private static volatile UniqueIdRegistry instance = null;
	/** The singleton instance ctor lock */
	private static final Object lock = new Object();
	
	/** The TSDB instance */
	private final TSDB tsdb;
	/** Unique IDs for the metric names. */
	private final UniqueId metrics;
	/** Unique IDs for the tag names. */
	private final UniqueId tag_names;
	/** Unique IDs for the tag values. */
	private final UniqueId tag_values;
	
	/** A map of UniqueIds keyed by the UniqueId type  */
	private final Map<UniqueId.UniqueIdType, UniqueId> byType = new EnumMap<UniqueId.UniqueIdType, UniqueId>(UniqueId.UniqueIdType.class);
	
	/** The JMX service MBean's ObjectName */
	public static final ObjectName OBJECT_NAME = JMXHelper.objectName(new StringBuilder(UniqueIdRegistry.class.getPackage().getName()).append(":service=").append(UniqueIdRegistry.class.getSimpleName()));
	
	
	/**
	 * Acquires the UniqueIdRegistry singleton instance
	 * @param tsdb The TSDB instance to initialize with
	 * @return the UniqueIdRegistry singleton instance
	 */
	public static UniqueIdRegistry getInstance(TSDB tsdb) {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					instance = new UniqueIdRegistry(tsdb);
				}
			}
		}
		return instance;
	}
	
	
	/**
	 * Creates a new UniqueIdRegistry
	 * @param The TSDB used by this service
	 */
	private UniqueIdRegistry(TSDB t) {
		this.tsdb = t;
		metrics = (UniqueId) PrivateAccessor.getFieldValue(tsdb, "metrics"); 
		byType.put(UniqueId.UniqueIdType.METRIC, metrics);
		tag_names = (UniqueId) PrivateAccessor.getFieldValue(tsdb, "tag_names");
		byType.put(UniqueId.UniqueIdType.TAGK, tag_names);
		tag_values = (UniqueId) PrivateAccessor.getFieldValue(tsdb, "tag_values");
		byType.put(UniqueId.UniqueIdType.TAGV, tag_values);
		JMXHelper.registerMBean(this, OBJECT_NAME);
	}

	/**
	 * Returns the Metrics UniqueId instance
	 * @return the Metrics UniqueId instance
	 */
	public UniqueId getMetricsUniqueId() {
		return metrics;
	}

	/**
	 * Returns the tagk UniqueId instance
	 * @return the tagk UniqueId instance
	 */
	public UniqueId getTagKUniqueId() {
		return tag_names;
	}

	/**
	 * Returns the tagv UniqueId instance
	 * @return the tagv UniqueId instance
	 */
	public UniqueId getTagVUniqueId() {
		return tag_values;
	}
	
	/**
	 * Returns the UniqueId for the passed UniqueIdType
	 * @param type The UniqueIdType to get the UniqueId for
	 * @return the UniqueIdType
	 */
	public UniqueId forType(UniqueId.UniqueIdType type) {
		if(type==null) throw new IllegalArgumentException("The passed UniqueIdType was null");
		return byType.get(type);
	}
	
	/**
	 * Returns the UniqueId for the passed UniqueIdType name
	 * @param name The UniqueIdType name to get the UniqueId for
	 * @return the UniqueIdType
	 */
	public UniqueId forType(String name) {
		if(name==null) throw new IllegalArgumentException("The passed UniqueIdType name was null");
		name = name.trim().toUpperCase();
		UniqueId.UniqueIdType type = null;
		try {
			type = UniqueId.UniqueIdType.valueOf(name);
			return forType(type);
		} catch (Exception ex) {
			throw new IllegalArgumentException("The name [" + name + "] is not a valid UniqueIdType");
		}
	}
	
	/**
	 * Returns the Metrics UniqueId cache hits
	 * @return  the Metrics UniqueId cache hits
	 */
	@Override
	public int getMetricCacheHits() {
		return metrics.cacheHits();
	}
	
	/**
	 * Returns the Metrics UniqueId cache misses
	 * @return  the Metrics UniqueId cache misses
	 */
	@Override
	public int getMetricCacheMisses() {
		return metrics.cacheMisses();
	}
	
	/**
	 * Returns the Metrics UniqueId cache size
	 * @return  the Metrics UniqueId cache size
	 */
	@Override
	public int getMetricCacheSize() {
		return metrics.cacheSize();
	}
	
	/**
	 * Returns the TAGK UniqueId cache hits
	 * @return  the TAGK UniqueId cache hits
	 */
	@Override
	public int getTagKCacheHits() {
		return tag_names.cacheHits();
	}
	
	/**
	 * Returns the TAGK UniqueId cache misses
	 * @return  the TAGK UniqueId cache misses
	 */
	@Override
	public int getTagKCacheMisses() {
		return tag_names.cacheMisses();
	}
	
	/**
	 * Returns the TAGK UniqueId cache size
	 * @return  the TAGK UniqueId cache size
	 */
	@Override
	public int getTagKCacheSize() {
		return tag_names.cacheSize();
	}
	
	/**
	 * Returns the TAGV UniqueId cache hits
	 * @return  the TAGV UniqueId cache hits
	 */
	@Override
	public int getTagVCacheHits() {
		return tag_values.cacheHits();
	}
	
	/**
	 * Returns the TAGV UniqueId cache misses
	 * @return  the TAGV UniqueId cache misses
	 */
	@Override
	public int getTagVCacheMisses() {
		return tag_values.cacheMisses();
	}
	
	/**
	 * Returns the TAGV UniqueId cache size
	 * @return  the TAGV UniqueId cache size
	 */
	@Override
	public int getTagVCacheSize() {
		return tag_values.cacheSize();
	}


	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.tsdb.core.UniqueIdRegistryMXBean#purgeMetricCache()
	 */
	@Override
	public void purgeMetricCache() {
		metrics.dropCaches();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.tsdb.core.UniqueIdRegistryMXBean#purgeTagKCache()
	 */
	@Override
	public void purgeTagKCache() {
		tag_names.dropCaches();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.tsdb.core.UniqueIdRegistryMXBean#purgeTagVCache()
	 */
	@Override
	public void purgeTagVCache() {
		tag_values.dropCaches();		
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.tsdb.core.UniqueIdRegistryMXBean#purgeAllCaches()
	 */
	@Override
	public void purgeAllCaches() {
		tag_names.dropCaches();
		tag_values.dropCaches();
		metrics.dropCaches();
	}
	
	/**
	 * 
	 */
	public void dumpMetricNameCache() {
		// TODO
	}
	
	
	

}
