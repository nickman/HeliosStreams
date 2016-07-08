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
package com.heliosapm.streams.metrics.processors.impl;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Required;

import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.ValueType;
import com.heliosapm.streams.metrics.processors.AbstractStreamedMetricProcessor;
import com.heliosapm.streams.metrics.processors.AbstractStreamedMetricProcessorSupplier;



/**
 * <p>Title: StreamedMetricAccumulatorSupplier</p>
 * <p>Description: Streamed metric processor for metered metrics</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.processors.impl.StreamedMetricAccumulatorSupplier</code></p>
 */

public class StreamedMetricAccumulatorSupplier extends AbstractStreamedMetricProcessorSupplier<String, StreamedMetric, String, StreamedMetric> {
	/** The flush period in seconds */
	protected long flushPeriod = 5L;
	
	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.processor.ProcessorSupplier#get()
	 */
	@Override
	public Processor<String, StreamedMetric> get() {
		final StreamedMetricAccumulator processor = new StreamedMetricAccumulator(flushPeriod, getStateStoreNames()[0]);
		startedProcessors.add(processor);
		return processor;
	}
	
	/**
	 * <p>Title: StreamedMetricAccumulator</p>
	 * <p>Description: A simple absolute aggregator counting instances of metrics submitted</p> 
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>com.heliosapm.streams.metrics.processors.impl.StreamedMetricAccumulatorSupplier.StreamedMetricAccumulator</code></p>
	 */
	static class StreamedMetricAccumulator extends AbstractStreamedMetricProcessor {
		/** The frequency of aggregation flushes in seconds */
		protected final long flushPeriod;
		/** The accumulated counts keyed by the metric key */
		protected KeyValueStore<String, Long> metricCountsStore;
		
		/**
		 * Creates a new StreamedMetricAccumulator
		 * @param period The frequency of aggregation flushes in seconds
		 * @param accumulatorStoreName The name of the accumulator state store
		 */
		protected StreamedMetricAccumulator(final long period, final String accumulatorStoreName) {
			super(ValueType.METER, TimeUnit.SECONDS.toMillis(period), new String[]{accumulatorStoreName});
			flushPeriod = period;
			log.info("Created Instance [" + System.identityHashCode(this) + "]");
		}
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.metrics.processors.AbstractStreamedMetricProcessor#init(org.apache.kafka.streams.processor.ProcessorContext)
		 */
		@Override
		@SuppressWarnings("unchecked")
		public void init(final ProcessorContext context) {
			super.init(context);		
			metricCountsStore = (KeyValueStore<String, Long>)getStateStore("metricCountsStoreDefinition");			
			log.info("Set context on Instance [" + System.identityHashCode(this) + "]: {}", context);
		}
		

		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.metrics.processors.AbstractStreamedMetricProcessor#doProcess(java.lang.String, com.heliosapm.streams.metrics.StreamedMetric)
		 */
		@Override
		protected boolean doProcess(final String key, final StreamedMetric sm) {
			Long tmk = metricCountsStore.get(key);
			if(tmk==null) {
				tmk = 1L;
				metricCountsStore.put(key, tmk);
				log.debug("Inited MTS: [{}]", key);
			} else {
				tmk++;
				metricCountsStore.put(key, tmk);				
			}
			return true;
		}
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.metrics.processor.AbstractStreamedMetricProcessor#punctuate(long)
		 */
		@Override
		public void punctuate(final long timestamp) {
			final KeyValueIterator<String, Long> iter = metricCountsStore.all();
			try {
				while(iter.hasNext()) {
					try {
						final KeyValue<String, Long> kv = iter.next();
						context.forward(kv.key, StreamedMetric.fromKey(timestamp, kv.key, kv.value));
					} catch (Exception x) {/* No Op */}
				}
				context.commit();
			} finally {
				iter.close();
			}
		}
	}
	
	
	

	/**
	 * Returns the frequency of aggregation flushes in seconds
	 * @return the frequency of aggregation flushes
	 */
	public long getFlushPeriod() {
		return flushPeriod;
	}

	/**
	 * Sets the frequency of aggregation flushes in seconds
	 * @param flushPeriod the flush period to set
	 */
	@Required
	public void setFlushPeriod(final long flushPeriod) {
		if(flushPeriod<1) throw new IllegalArgumentException("Invalid flushPeriod period: " + flushPeriod);
		this.flushPeriod = flushPeriod;
	}


}
