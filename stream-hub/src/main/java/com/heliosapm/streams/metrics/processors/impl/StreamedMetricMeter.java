package com.heliosapm.streams.metrics.processors.impl;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.jmx.export.annotation.ManagedAttribute;

import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.ValueType;
import com.heliosapm.streams.metrics.processors.AbstractStreamedMetricProcessor;
import com.heliosapm.streams.metrics.processors.TimestampedMetricKey;

/**
	 * <p>Title: StreamedMetricMeter</p>
	 * <p>Description: A metered aggregator counting instances of metrics submitted through here with the specified aggregation window</p> 
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>com.heliosapm.streams.metrics.processors.impl.StreamedMetricMeter</code></p>
	 */
//	@ManagedResource
//	@Configurable
	public class StreamedMetricMeter extends AbstractStreamedMetricProcessor<String, StreamedMetric> {
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
		 * @param maxForwards The maximum number of uncommited forwards
		 * @param metricTimestampStoreName The name of the metric timestamp state store
		 * @param idleTimeout the idle timeout period in secs.
		 */
		protected StreamedMetricMeter(final int aggregationPeriod, final long period, final int maxForwards, final String metricTimestampStoreName, final int idleTimeout) {
			super(ValueType.METER, period, maxForwards, new String[]{metricTimestampStoreName});
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
				log.info("New MTS: [{}]", tmk);
			} else {
				log.info("MTS from Store: [{}]", tmk);
				if(!tmk.isSameAggPeriodAs(sm.getTimestamp(), sm.forValue(1L).getValueAsLong(), aggregationPeriod)) {
					log.info("Commiting Batch: [{}]:[{}]", tmk.getMetricKey(), tmk.getCount());
					final StreamedMetric f = StreamedMetric.fromKey(System.currentTimeMillis(), tmk.getMetricKey(), tmk.getCount());
					boolean ok = true;
					if(f==null) {
						log.warn("Metric was null");
						ok = false;
					}
					if(ok && f.metricKey()==null) {
						log.warn("Metric key was null");
						ok = false;
					}
					if(ok) {
						forward(f.metricKey(), f);
					}
					tmk = new TimestampedMetricKey(TimeUnit.MILLISECONDS.toSeconds(sm.getTimestamp()), sm.forValue(1L).getValueAsLong(), sm.metricKey());								
				}
				
			}
			metricTimestampStore.put(mkey, tmk);
//			commit();
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
							forward(kv.key, StreamedMetric.fromKey(timestamp, kv.value.getMetricKey(), kv.value.reset()));
							metricTimestampStore.put(kv.key, kv.value);
						} else {
							metricTimestampStore.delete(kv.key);
						}
					} catch (Exception x) {
						/* No Op */
					}
				}
				commit();
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