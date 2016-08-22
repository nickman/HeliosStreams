package com.heliosapm.streams.metrics.processors.impl;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.springframework.jmx.export.annotation.ManagedAttribute;

import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.ValueType;
import com.heliosapm.streams.metrics.processors.AbstractStreamedMetricProcessor;
import com.heliosapm.streams.metrics.processors.TimestampedMetricKey;
import com.heliosapm.utils.tuples.NVP;

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
		/** The timeout period in seconds after which an idle TimestampedMetricKey will be removed from the store */
		protected final int idleTimeout;
		/** Period accumulator, replaces metricTimestampStore */
		protected final NonBlockingHashMap<String, TimestampedMetricKey> periodAccumulator = new NonBlockingHashMap<String, TimestampedMetricKey>(1024); 
		
		
		/**
		 * Creates a new StreamedMetricMeter
		 * @param aggregationPeriod The aggregation period of this meter in seconds.
		 * @param period The punctuation period in ms.
		 * @param maxForwards The maximum number of uncommited forwards
		 * @param topicSink The topic sink for this processor
		 * @param sources The topic sources for this processor
		 * @param idleTimeout the idle timeout period in secs.
		 */
		protected StreamedMetricMeter(final int aggregationPeriod, final long period, final int maxForwards, final String topicSink, final String[] sources, final int idleTimeout) {
			super(ValueType.METER, period, maxForwards, topicSink, sources);
			this.aggregationPeriod = aggregationPeriod;
			this.idleTimeout = idleTimeout;
			log.info("Created Instance [" + System.identityHashCode(this) + "]");
		}
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.metrics.processors.AbstractStreamedMetricProcessor#init(org.apache.kafka.streams.processor.ProcessorContext)
		 */
		@Override		
		public void init(final ProcessorContext context) {
			super.init(context);					
			log.info("Set context on Instance [" + System.identityHashCode(this) + "]: {}", context);
			
		}
		

		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.metrics.processors.AbstractStreamedMetricProcessor#doProcess(java.lang.Object, java.lang.Object)
		 */
		@Override
		protected boolean doProcess(final String key, final StreamedMetric sm) {
			final String mkey = sm.metricKey();
			TimestampedMetricKey tmk = periodAccumulator.putIfAbsent(mkey, TimestampedMetricKey.PLACEHOLDER);
			if(tmk==null || tmk==TimestampedMetricKey.PLACEHOLDER) {
				tmk = new TimestampedMetricKey(TimeUnit.MILLISECONDS.toSeconds(sm.getTimestamp()), sm.forValue(1L).getValueAsLong(), sm.metricKey(), aggregationPeriod);
				periodAccumulator.replace(mkey, tmk);
				log.debug("New MTS: [{}]", tmk);
			} else {
				tmk.increment(sm.forValue(1L).getValueAsLong());
				// if is same period, we just add the count (done internally)
				// otherwise we forward the count for the prior window and start a new one
//				if(!tmk.isSameAggPeriodAs(sm.getTimestamp(), sm.forValue(1L).getValueAsLong())) {		
//					final long total = tmk.getCount();
//					final NVP<Long, Double> timeRate = tmk.reset(sm.getTimestamp(), sm.forValue(1L).getValueAsLong());
//					final StreamedMetric f = StreamedMetric.fromKey(TimeUnit.SECONDS.toMillis(timeRate.getKey()), tmk.getMetricKey(), timeRate.getValue());
//					log.info("Forwarding Batch: ts:[{}], key:[{}], total:[{}], tps:[{}]", new Date(f.getTimestamp()), tmk.getMetricKey(), total, timeRate.getValue());
//					forward(f.metricKey(), f);												
//				} 
			}
//			commit();
			return true;
		}
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.metrics.processors.AbstractStreamedMetricProcessor#punctuate(long)
		 */
		@Override
		public void punctuate(final long timestamp) {
			log.info("-----------Punctuate");
			for(Map.Entry<String, TimestampedMetricKey> entry: periodAccumulator.entrySet()) {
				final String metricKey = entry.getKey();
				final TimestampedMetricKey tmk = entry.getValue();
				final NVP<Long, Double> nvp = tmk.punctuate(timestamp);
				final StreamedMetric f;
				if(nvp!=null) {
					f = StreamedMetric.fromKey(TimeUnit.SECONDS.toMillis(nvp.getKey()), tmk.getMetricKey(), nvp.getValue());
					log.info("Forwarding Current Batch: key:[{}], tps:[{}]", tmk.getMetricKey(), nvp.getValue());					
				} else {
					if(tmk.isExpired(timestamp, idleTimeout)) {
						f = StreamedMetric.fromKey(timestamp, tmk.getMetricKey(), 0D);
						log.info("Forwarding Stale Batch: key:[{}], tps:[0.0]", tmk.getMetricKey());											
					} else { 
						if(tmk.isIdle(timestamp, idleTimeout)) {						
							log.info("Purging Idle Batch: [{}]", metricKey);
							periodAccumulator.remove(metricKey);
						}
						f = null;
					}
				}
				if(f!=null) forward(f.getMetricName(), f);
			}
			commit();
		}
		
		
		/**
		 * Returns the number of metrics in the period accumulator
		 * @return the number of metrics in the period accumulator
		 */
		@ManagedAttribute(description="The number of metrics in the period accumulator")
		public int getAccumulatorSize() {
			return periodAccumulator.size();
		}

		/**
		 * Returns the length of the time in seconds window within which incoming metrics will be aggregated and forwarded 
		 * @return the length of the time in seconds window within which incoming metrics will be aggregated and forwarded
		 */
		@ManagedAttribute(description="The length of the time in seconds window within which incoming metrics will be aggregated and forwarded")
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