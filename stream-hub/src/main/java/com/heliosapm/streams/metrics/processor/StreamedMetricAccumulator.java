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

import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.ValueType;
import com.heliosapm.streams.metrics.processor.TimestampedMetricKey.TimestampedMetricKeyDeserializer;
import com.heliosapm.streams.metrics.processor.TimestampedMetricKey.TimestampedMetricKeySerializer;

/**
 * <p>Title: StreamedMetricAccumulator</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.processor.StreamedMetricAccumulator</code></p>
 */

public class StreamedMetricAccumulator extends AbstractStreamedMetricProcessor {
	/** The baseline timestamp per metric store name */
	public static final String TS_STORE = "metricTimestamps";
	
	/** An array of the store names */
	private static final String[] DATA_STORES = {TS_STORE};
	
	/** The first timestamp for each unique metric key in the current period */
	protected KeyValueStore<String, TimestampedMetricKey> metricTimestampStore;
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.processor.StreamedMetricProcessor#getStateStores()
	 */
	@Override
	public StateStoreSupplier[] getStateStores() {
		final StateStoreSupplier[] ss = new StateStoreSupplier[] {			
			Stores.create(TS_STORE).withStringKeys().withValues(Serdes.serdeFrom(
					new TimestampedMetricKeySerializer(),
					new TimestampedMetricKeyDeserializer()
			)).inMemory().build()
		};		
		return ss;
	}
 
	/** The window size in seconds */
	protected final long windowSecs;
	
	
	
	/**
	 * Creates a new AbstractStreamedMetricProcessor
	 * @param period The punctuation period in ms.
	 * @param sink The name of the sink topic name this processor published to
	 */
	public StreamedMetricAccumulator(final long period, final String sink) {
		super(ValueType.A, period, sink, DATA_STORES);
		windowSecs = TimeUnit.MILLISECONDS.toSeconds(period);
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.processor.AbstractStreamedMetricProcessor#init(org.apache.kafka.streams.processor.ProcessorContext)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void init(final ProcessorContext context) {
		super.init(context);		
		metricTimestampStore = (KeyValueStore<String, TimestampedMetricKey>)context.getStateStore(TS_STORE);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.processor.AbstractStreamedMetricProcessor#doProcess(java.lang.String, com.heliosapm.streams.metrics.StreamedMetric)
	 */
	@Override
	protected void doProcess(final String key, final StreamedMetric sm) {		
		TimestampedMetricKey tmk = metricTimestampStore.get(key);
		if(tmk==null) {
			tmk = new TimestampedMetricKey(TimeUnit.MILLISECONDS.toSeconds(sm.getTimestamp()), sm.forValue(1L).getValueAsLong(), sm.metricKey());
			metricTimestampStore.put(key, tmk);
			log.info("Wrote MTS: [{}]", tmk);
		} else {
			log.info("MTS from Store: [{}]", tmk);
			if(!tmk.isSameSecondAs(sm.getTimestamp(), sm.forValue(1L).getValueAsLong(), windowSecs)) {
				log.info("Commiting Batch: [{}]:[{}]", tmk.getMetricKey(), tmk.getCount());
				final StreamedMetric f = StreamedMetric.fromKey(System.currentTimeMillis(), tmk.getMetricKey(), tmk.getCount());
				boolean ok = true;
				if(context==null) {
					log.warn("Context was null");
					ok = false;
				}
				if(f==null) {
					log.warn("Metric was null");
					ok = false;
				}
				if(ok && f.metricKey()==null) {
					log.warn("Metric key was null");
					ok = false;
				}
				if(ok) {
					context.forward(f.metricKey(), f);
					context.commit();
				}
				tmk = new TimestampedMetricKey(TimeUnit.MILLISECONDS.toSeconds(sm.getTimestamp()), sm.forValue(1L).getValueAsLong(), sm.metricKey());
				metricTimestampStore.put(key, tmk);				
			}
		}		
	}	
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.processor.AbstractStreamedMetricProcessor#punctuate(long)
	 */
	@Override
	public void punctuate(final long timestamp) {
		final KeyValueIterator<String, TimestampedMetricKey> iter = metricTimestampStore.all();
		try {
			final KeyValue<String, TimestampedMetricKey> kv = iter.next();
			if(kv.value.isExpired(timestamp, windowSecs)) {
				iter.remove();
				context.forward(kv.key, StreamedMetric.fromKey(timestamp, kv.value.getMetricKey(), kv.value.getCount()));
				context.commit();
			}
		} finally {
			iter.close();
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.processor.AbstractStreamedMetricProcessor#close()
	 */
	@Override
	public void close() {
		try { metricTimestampStore.close(); } catch (Exception x) {/* No Op */}
		super.close();
	}

}
 