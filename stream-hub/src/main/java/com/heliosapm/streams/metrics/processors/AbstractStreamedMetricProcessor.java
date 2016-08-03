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
package com.heliosapm.streams.metrics.processors;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.heliosapm.streams.common.metrics.SharedMetricsRegistry;
import com.heliosapm.streams.metrics.ValueType;

/**
 * <p>Title: AbstractStreamedMetricProcessor</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.processors.AbstractStreamedMetricProcessor</code></p>
 * @param <K> The key type
 * @param <V> The value type
 */

public abstract class AbstractStreamedMetricProcessor<K,V> implements Processor<K, V> {
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** The application id of the context */
	protected String applicationId = null;
	/** The value type processed by this processor */
	protected final ValueType valueType;
	/** The names of state stores used by this processor */
	protected final String[] stateStoreNames;
	/** The injected processor context */
	protected ProcessorContext context = null;
	/** The punctuation period */
	protected final long period;
	/** The state stores allocated for this processor */
	protected Map<String, StateStore> stateStores = new HashMap<String, StateStore>();
	/** The processing timer */
	//protected final Timer timer = SharedMetricsRegistry.getInstance().timer("StreamedMetricProcessor." + getClass().getSimpleName() + ".processed");
	protected final Timer timer = SharedMetricsRegistry.getInstance().timer(getClass().getSimpleName() + ".processed");
	/** The dropped metric counter */
	//protected final Counter dropCounter = SharedMetricsRegistry.getInstance().counter("StreamedMetricProcessor." + getClass().getSimpleName() + ".dropped");
	protected final Counter dropCounter = SharedMetricsRegistry.getInstance().counter(getClass().getSimpleName() + ".dropped");
	

	/**
	 * Creates a new AbstractStreamedMetricProcessor
	 * @param valueType The value type this processor supplies stream processing for
	 * @param period The punctuation period (ignored if less than 1)
	 * @param stateStoreNames The names of the state stores used by this processor
	 */
	protected AbstractStreamedMetricProcessor(final ValueType valueType, final long period, final String...stateStoreNames) {
		this.valueType = valueType;
		this.period = period;
		this.stateStoreNames = stateStoreNames;
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.processor.Processor#init(org.apache.kafka.streams.processor.ProcessorContext)
	 */
	@Override
	public void init(final ProcessorContext context) {
		this.context = context;
		if(stateStoreNames!=null && stateStoreNames.length != 0) {
			for(String ssName: stateStoreNames) {
				StateStore store = context.getStateStore(ssName);
				stateStores.put(ssName, store);
			}
		}
		if(period > 0L) {
			context.schedule(period);
		}
		applicationId = context.applicationId();
	}
	
	/**
	 * Returns the named state store
	 * @param name the name of the state store
	 * @return the named state store or null if no state store was bound to the passed name
	 */
	public StateStore getStateStore(final String name) {
		return stateStores.get(name);
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.processor.Processor#process(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void process(final K key, final V value) {
		log.debug("Processing Metric [{}]", key);
		final Context ctx = timer.time();
		if(doProcess(key, value)) {
			ctx.stop();
		} else {
			dropCounter.inc();
		}
	}
	
	/**
	 * Processes the passed streamed metric
	 * @param key The streamed metric key
	 * @param value The streamed metric to process
	 * @return true if the metric was processed, false otherwise
	 */
	protected abstract boolean doProcess(final K key, final V value);

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.processor.Processor#punctuate(long)
	 */
	@Override
	public void punctuate(final long timestamp) {
		/* No Op */
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.processor.Processor#close()
	 */
	@Override
	public void close() {
//		log.info(">>>>>  Stopping [{}]...", getClass().getSimpleName());
//		if(!stateStores.isEmpty()) {
//			for(String key: new HashSet<String>(stateStores.keySet())) {
//				final StateStore store = stateStores.remove(key);
//				if(store!=null) {					
//					log.info("\tClosing Store [{}]...", store.name());
//					try { store.flush(); } catch (Exception x) {/* No Op */}
//					try { store.close(); } catch (Exception x) {/* No Op */}
//					log.info("\tStore Closed [{}].", store.name());
//				}
//			}
//		}
//		log.info("<<<<< Stopped [{}].", getClass().getSimpleName());
	}
	
	/**
	 * Returns the application id
	 * @return the application id
	 */
	public String getApplicationId() {
		return applicationId;
	}

}
