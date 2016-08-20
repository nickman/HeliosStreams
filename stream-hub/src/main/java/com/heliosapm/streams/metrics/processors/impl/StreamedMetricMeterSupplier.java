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
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.ValueType;
import com.heliosapm.streams.metrics.processors.AbstractStreamedMetricProcessor;
import com.heliosapm.streams.metrics.processors.AbstractStreamedMetricProcessorSupplier;
import com.heliosapm.streams.metrics.processors.TimestampedMetricKey;

/**
 * <p>Title: StreamedMetricMeterSupplier</p>
 * <p>Description: Streamed metric processor for metered metrics</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.processors.impl.StreamedMetricMeterSupplier</code></p>
 */

public class StreamedMetricMeterSupplier extends AbstractStreamedMetricProcessorSupplier<String, StreamedMetric, String, StreamedMetric> {
	/** The aggregation period to supply to created StreamedMetricMeter instances  */
	protected int aggregationPeriod = -1;
	/** The timeout period in seconds after which an idle TimestampedMetricKey will be removed from the store */
	protected int idleTimeout = -1;
	
	
	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.processor.ProcessorSupplier#get()
	 */
	@Override
	public Processor<String, StreamedMetric> get() {
		final StreamedMetricMeter processor = new StreamedMetricMeter(aggregationPeriod, period, getStateStoreNames()[0], idleTimeout);		
		startedProcessors.add(processor);
//		if(appCtx!=null) {
//			
//			appCtx.getAutowireCapableBeanFactory().applyBeanPostProcessorsAfterInitialization(processor, beanName);
//			appCtx.getAutowireCapableBeanFactory().autowireBean(processor);
//			appCtx.getAutowireCapableBeanFactory().configureBean(processor, beanName + "Instance");
//		}
		return processor;
	}
	
	/**
	 * <p>Title: StreamedMetricMeter</p>
	 * <p>Description: A metered aggregator counting instances of metrics submitted through here with the specified aggregation window</p> 
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>com.heliosapm.streams.metrics.processors.impl.StreamedMetricMeterSupplier.StreamedMetricMeter</code></p>
	 */
	@ManagedResource
	static class StreamedMetricMeter extends AbstractStreamedMetricProcessor<String, StreamedMetric> {
		/** The aggregation period of this meter in seconds */
		protected final int aggregationPeriod;
		/** The first timestamp for each unique metric key in the current period */
		protected KeyValueStore<String, TimestampedMetricKey> metricTimestampStore;
		/** The timeout period in seconds after which an idle TimestampedMetricKey will be removed from the store */
		protected final int idleTimeout;
		
		
		/**
		 * Creates a new StreamedMetricMeter
		 * @param aggregationPeriod The aggregation period of this meter in seconds.
		 * @param period The punctuation period in ms.
		 * @param metricTimestampStoreName The name of the metric timestamp state store
		 * @param idleTimeout the idle timeout period in secs.
		 */
		protected StreamedMetricMeter(final int aggregationPeriod, final long period, final String metricTimestampStoreName, final int idleTimeout) {
			super(ValueType.METER, period, new String[]{metricTimestampStoreName});
			this.aggregationPeriod = aggregationPeriod;
			this.idleTimeout = idleTimeout;
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
			metricTimestampStore = (KeyValueStore<String, TimestampedMetricKey>)getStateStore("metricTimestampStoreDefinition");			
			log.info("Set context on Instance [" + System.identityHashCode(this) + "]: {}", context);
			
		}
		

		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.metrics.processors.AbstractStreamedMetricProcessor#doProcess(java.lang.Object, java.lang.Object)
		 */
		@SuppressWarnings("null")
		@Override
		protected boolean doProcess(final String key, final StreamedMetric sm) {
			final String mkey = sm.metricKey();
			TimestampedMetricKey tmk = metricTimestampStore.get(mkey);
			if(tmk==null) {
				tmk = new TimestampedMetricKey(TimeUnit.MILLISECONDS.toSeconds(sm.getTimestamp()), sm.forValue(1L).getValueAsLong(), sm.metricKey());
				metricTimestampStore.put(mkey, tmk);
				log.debug("Wrote MTS: [{}]", tmk);
			} else {
				log.debug("MTS from Store: [{}]", tmk);
				if(!tmk.isSameSecondAs(sm.getTimestamp(), sm.forValue(1L).getValueAsLong(), aggregationPeriod)) {
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
						forwardCounter.inc();
						log.info("Committed Batch: [{}]:[{}]",  tmk.getMetricKey(), tmk.getCount());
					}
					tmk = new TimestampedMetricKey(TimeUnit.MILLISECONDS.toSeconds(sm.getTimestamp()), sm.forValue(1L).getValueAsLong(), sm.metricKey());
					metricTimestampStore.put(mkey, tmk);				
				}
			}
			return true;
		}
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.metrics.processors.AbstractStreamedMetricProcessor#punctuate(long)
		 */
		@Override
		public void punctuate(final long timestamp) {			
			final KeyValueIterator<String, TimestampedMetricKey> iter = metricTimestampStore.all();
			try {
				while(iter.hasNext()) {
					try {
						final KeyValue<String, TimestampedMetricKey> kv = iter.next();
						if(kv.value.isExpired(timestamp, aggregationPeriod, idleTimeout)) {
							context.forward(kv.key, StreamedMetric.fromKey(timestamp, kv.value.getMetricKey(), kv.value.reset()));
							metricTimestampStore.put(kv.key, kv.value);
						} else {
							metricTimestampStore.delete(kv.key);
						}
					} catch (Exception x) {
						/* No Op */
					}
				}
				context.commit();
			} finally {
				iter.close();
			}
		}

		/**
		 * Returns the length of the time in seconds window within which incoming metrics will be aggregated and forwarded 
		 * @return the length of the time in seconds window within which incoming metrics will be aggregated and forwarded
		 */
		@ManagedAttribute(description="The the length of the time in seconds window within which incoming metrics will be aggregated and forwarded")
		public int getAggregationPeriod() {
			return aggregationPeriod;
		}

		/**
		 * Returns the timeout period in seconds after which an idle TimestampedMetricKey will be removed from the store 
		 * @return the timeout period in seconds after which an idle TimestampedMetricKey will be removed from the store
		 */
		@ManagedAttribute(description="The timeout period in seconds after which an idle TimestampedMetricKey will be removed from the store")
		public int getIdleTimeout() {
			return idleTimeout;
		}
	}
	
	
	

	/**
	 * Returns the the aggregation period to supply to created StreamedMetricMeter instances
	 * @return the aggregationPeriod
	 */
	public int getAggregationPeriod() {
		return aggregationPeriod;
	}

	/**
	 * Sets the aggregation period to supply to created StreamedMetricMeter instances
	 * @param aggregationPeriod the aggregationPeriod to set
	 */
	@Required
	public void setAggregationPeriod(final int aggregationPeriod) {
		if(aggregationPeriod<1) throw new IllegalArgumentException("Invalid aggregation period: " + aggregationPeriod);
		this.aggregationPeriod = aggregationPeriod;
	}

	/**
	 * Returns the timeout period in seconds after which an idle TimestampedMetricKey will be removed from the store
	 * @return the idle timeout
	 */
	public int getIdleTimeout() {
		return idleTimeout;
	}

	/**
	 * Sets the timeout period in seconds after which an idle TimestampedMetricKey will be removed from the store
	 * @param idleTimeout the idle timeout in seconds
	 */
	public void setIdleTimeout(final int idleTimeout) {
		if(idleTimeout<1) throw new IllegalArgumentException("Invalid idle timeout: " + idleTimeout);
		this.idleTimeout = idleTimeout;
	}

}
