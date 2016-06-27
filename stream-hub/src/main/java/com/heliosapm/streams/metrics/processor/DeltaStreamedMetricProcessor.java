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
package com.heliosapm.streams.metrics.processor;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.StreamedMetricValue;
import com.heliosapm.streams.metrics.ValueType;
import com.heliosapm.streams.metrics.processor.TimestampedMetricKey.TimestampedMetricKeyDeserializer;
import com.heliosapm.streams.metrics.processor.TimestampedMetricKey.TimestampedMetricKeySerializer;

/**
 * <p>Title: DeltaStreamedMetricProcessor</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.processor.DeltaStreamedMetricProcessor</code></p>
 */

public class DeltaStreamedMetricProcessor extends AbstractStreamedMetricProcessor {
	/** The long metric deltas */
	public static final String LONG_DELTA_STORE = "longMetricDeltas";
	/** The double metric deltas */
	public static final String DOUBLE_DELTA_STORE = "doubleMetricDeltas";
	
	/** An array of the store names */
	private static final String[] DATA_STORES = {LONG_DELTA_STORE, DOUBLE_DELTA_STORE};
	
	/** The state store for long deltas */
	protected KeyValueStore<String, Long> longMetricDeltas = null;
	/** The state store for double deltas */
	protected KeyValueStore<String, Double> doubleMetricDeltas = null;

	/**
	 * Creates a new DeltaStreamedMetricProcessor
	 * @param period The scheduler period in ms.
	 * @param sink The destination sink
	 */
	public DeltaStreamedMetricProcessor(final long period, final String sink) {
		super(ValueType.D, period, sink, DATA_STORES);
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.processor.StreamedMetricProcessor#getStateStores()
	 */
	@Override
	public StateStoreSupplier[] getStateStores() {
		final StateStoreSupplier[] ss = new StateStoreSupplier[] {			
			Stores.create(LONG_DELTA_STORE).withStringKeys().withLongValues().inMemory().build(),
			Stores.create(DOUBLE_DELTA_STORE).withStringKeys().withDoubleValues().inMemory().build(),
		};		
		return ss;
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.processor.AbstractStreamedMetricProcessor#close()
	 */
	@Override
	public void close() {
		try { doubleMetricDeltas.close(); } catch (Exception x) {/* No Op */}
		try { longMetricDeltas.close(); } catch (Exception x) {/* No Op */}
		super.close();
	}
	

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.processor.AbstractStreamedMetricProcessor#doProcess(java.lang.String, com.heliosapm.streams.metrics.StreamedMetric)
	 */
	@Override
	protected boolean doProcess(final String key, final StreamedMetric value) {
		if(!value.isValued()) return false;
		final StreamedMetricValue smv = value.forValue(-1L);
		if(smv.isDoubleValue()) {
			final double d = smv.getDoubleValue();
			Double state = doubleMetricDeltas.get(smv.metricKey());
			if(state!=null) {
				final double delta = d - state;
				context.forward(smv.metricKey(), smv.update(delta));
				context.commit();
			}
			doubleMetricDeltas.put(smv.metricKey(), d);
		} else {
			final long l = smv.getLongValue();
			Long state = longMetricDeltas.get(smv.metricKey());
			if(state!=null) {
				final long delta = l - state;
				context.forward(smv.metricKey(), smv.update(delta));
				context.commit();
			}
			longMetricDeltas.put(smv.metricKey(), l);			
		}
		return true;
	}

}
