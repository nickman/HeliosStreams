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
package com.heliosapm.streams.tracing.deltas;

import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.jmx.JMXHelper;

import gnu.trove.map.hash.TObjectDoubleHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.map.hash.TObjectLongHashMap;


/**
 * <p>Title: DeltaManager</p>
 * <p>Description: Manages deltas per unique metric ids</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tracing.deltas.DeltaManager</code></p>
 */

public class DeltaManager implements DeltaManagerMBean {
	/** The singleton instance */
	private static volatile DeltaManager instance = null;
	/** The singleton instance ctor lock */
	private static final Object lock = new Object();
	
	/** The container for tracking long deltas */
	protected final TObjectLongHashMap<java.lang.String> longDeltas;
	/** The container for tracking double deltas */
	protected final TObjectDoubleHashMap<java.lang.String> doubleDeltas;
	/** The container for tracking integer deltas */
	protected final TObjectIntHashMap<java.lang.String> intDeltas;
	
	/** The container for tracking the last computed long delta */
	protected final TObjectLongHashMap<java.lang.String> longVDeltas;
	/** The container for tracking the last computed double delta */
	protected final TObjectDoubleHashMap<java.lang.String> doubleVDeltas;
	/** The container for tracking the last computed integer delta */
	protected final TObjectIntHashMap<java.lang.String> intVDeltas;
	
	
	/** The default initial capacity of the delta container */
	public static final int DELTA_CAPACITY_DEFAULT = 100;	
	/** The default load factor of the delta container */
	public static final float DELTA_LOAD_FACTOR_DEFAULT = 0.5F;
	/** The name of the system property to override the configured initial capacity of the delta container */
	protected static final String DELTA_CAPACITY = "deltas.initialcapacity";
	/** The name of the system property to override the configured load capacity of the delta container */
	protected static final String DELTA_LOAD_FACTOR = "deltas.loadfactor";
	
	/**
	 * Returns the DeltaManager singleton instance
	 * @return the DeltaManager singleton instance
	 */
	public static DeltaManager getInstance() {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					instance = new DeltaManager();
				}
			}
		}
		return instance;
	}
	
	/**
	 * Creates a new DeltaManager
	 */
	private DeltaManager() {
		final int initialDeltaCapacity = ConfigurationHelper.getIntSystemThenEnvProperty(DELTA_CAPACITY, DELTA_CAPACITY_DEFAULT);
		final float initialDeltaLoadFactor = ConfigurationHelper.getFloatSystemThenEnvProperty(DELTA_LOAD_FACTOR, DELTA_LOAD_FACTOR_DEFAULT);
		longDeltas = new TObjectLongHashMap<java.lang.String>(initialDeltaCapacity, initialDeltaLoadFactor, Long.MIN_VALUE);
		doubleDeltas = new TObjectDoubleHashMap<java.lang.String>(initialDeltaCapacity, initialDeltaLoadFactor, Double.MIN_NORMAL);
		intDeltas = new TObjectIntHashMap<java.lang.String>(initialDeltaCapacity, initialDeltaLoadFactor, Integer.MIN_VALUE);
		longVDeltas = new TObjectLongHashMap<java.lang.String>(initialDeltaCapacity, initialDeltaLoadFactor, Long.MIN_VALUE);
		doubleVDeltas = new TObjectDoubleHashMap<java.lang.String>(initialDeltaCapacity, initialDeltaLoadFactor, Double.MIN_NORMAL);
		intVDeltas = new TObjectIntHashMap<java.lang.String>(initialDeltaCapacity, initialDeltaLoadFactor, Integer.MIN_VALUE);		
		JMXHelper.registerMBean(OBJECT_NAME, this);		
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.deltas.DeltaManagerMBean#getLongDeltaSize()
	 */
	@Override
	public int getLongDeltaSize() {
		synchronized(longDeltas) {
			return longDeltas.size();
		}
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.deltas.DeltaManagerMBean#getDoubleDeltaSize()
	 */
	@Override
	public int getDoubleDeltaSize() {
		synchronized(doubleDeltas) {
			return doubleDeltas.size();
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.deltas.DeltaManagerMBean#getIntDeltaSize()
	 */
	@Override
	public int getIntDeltaSize() {
		synchronized(intDeltas) {
			return intDeltas.size();
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.deltas.DeltaManagerMBean#compact()
	 */
	@Override
	public void compact() {
		synchronized(longDeltas) { longDeltas.compact(); }
		synchronized(doubleDeltas) { doubleDeltas.compact(); }
		synchronized(intDeltas) { intDeltas.compact(); }
		synchronized(longVDeltas) { longVDeltas.compact(); }
		synchronized(doubleVDeltas) { doubleVDeltas.compact(); }
		synchronized(intVDeltas) { intVDeltas.compact(); }
		
	}
	

	/**
	 * Registers a sample value and returns the delta between this sample and the prior
	 * @param key The delta sample key
	 * @param value The absolute sample value
	 * @return The delta or null if this was the first sample, or the last sample caused a reset
	 */
	public Long delta(final String key, final long value) {
		Long result = null;
		long prior;
		synchronized(longDeltas) {
			prior = longDeltas.put(key, value);
		}
		if(prior!=Long.MIN_VALUE && prior <= value) {
			result = value - prior;
		}
		if(result!=null) {
			synchronized(longVDeltas) { longVDeltas.put(key, result); }
		}
		return result;
	}
	
	/**
	 * Registers a sample value and returns the delta between this sample and the prior
	 * @param key The delta sample key
	 * @param value The absolute sample value
	 * @return The delta or null if this was the first sample, or the last sample caused a reset
	 */
	public Double delta(final String key, final double value) {
		Double result = null;
		double prior;
		synchronized(doubleDeltas) {
			prior = doubleDeltas.put(key, value);
		}
		if(prior!=Double.MIN_VALUE && prior <= value) {			
			result = value - prior;
		}
		if(result!=null) {
			synchronized(doubleVDeltas) { doubleVDeltas.put(key, result); }
		}
		return result;
	}
	
	/**
	 * Registers a sample value and returns the delta between this sample and the prior
	 * @param key The delta sample key
	 * @param value The absolute sample value
	 * @return The delta or null if this was the first sample, or the last sample caused a reset
	 */
	public Integer delta(final String key, final int value) {
		Integer result = null;
		int prior;
		synchronized(intDeltas) {
			prior = intDeltas.put(key, value);
		}
		if(prior!=Integer.MIN_VALUE && prior <= value) {			
			result = value - prior;
		}
		if(result!=null) {
			synchronized(intVDeltas) { intVDeltas.put(key, result); }
		}
		return result;
	}
	
	/**
	 * Returns the most recently recorded delta for the passed key
	 * @param key The key to get the delta for
	 * @return The last recorded delta or null if one was not found
	 */
	public Integer intDeltav(final String key) {		
		synchronized(intVDeltas) {
			if(intVDeltas.containsKey(key)) return intVDeltas.get(key); 
		}
		return null;
	}
	
	/**
	 * Returns the most recently recorded delta for the passed key
	 * @param key The key to get the delta for
	 * @return The last recorded delta or null if one was not found
	 */
	public Double doubleDeltav(final String key) {		
		synchronized(doubleVDeltas) {
			if(doubleVDeltas.containsKey(key)) return doubleVDeltas.get(key); 
		}
		return null;
	}
	
	
	/**
	 * Returns the most recently recorded delta for the passed key
	 * @param key The key to get the delta for
	 * @return The last recorded delta or null if one was not found
	 */
	public Long longDeltav(final String key) {		
		synchronized(longVDeltas) {
			if(longVDeltas.containsKey(key)) return longVDeltas.get(key); 
		}
		return null;
	}
	
	
	/**
	 * Resets the named delta
	 * @param key the key of the delta to reset
	 * @return the most recent state or null if none existed
	 */
	public Integer resetInt(final String key) {
		final int d = intDeltas.remove(key);
		return d==Integer.MIN_VALUE ? null : d;		
	}
	
	/**
	 * Resets the named delta
	 * @param key the key of the delta to reset
	 * @return the most recent state or null if none existed
	 */
	public Long resetLong(final String key) {
		final long d = longDeltas.remove(key);
		return d==Long.MIN_VALUE ? null : d;		
	}
	
	/**
	 * Resets the named delta
	 * @param key the key of the delta to reset
	 * @return the most recent state or null if none existed
	 */
	public Double resetDouble(final String key) {
		final double d = doubleDeltas.remove(key);
		return d==Double.MIN_VALUE ? null : d;		
	}
	

	

}
