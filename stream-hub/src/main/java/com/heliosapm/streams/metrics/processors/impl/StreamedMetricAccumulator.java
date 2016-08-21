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

/**
 * <p>Title: StreamedMetricAccumulator</p>
 * <p>Description: A simple absolute aggregator counting instances of metrics submitted</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.processors.impl.StreamedMetricAccumulator</code></p>
 */
public class StreamedMetricAccumulator extends AbstractStreamedMetricProcessor<String, StreamedMetric> {
	/** The accumulated counts keyed by the metric key */
	protected KeyValueStore<String, Long> metricCountsStore;
	
	/**
	 * Creates a new StreamedMetricAccumulator
	 * @param period The frequency of aggregation flushes in seconds
	 * @param maxForwards The maximum number of uncommited forwards
	 * @param accumulatorStoreName The name of the accumulator state store
	 */
	protected StreamedMetricAccumulator(final long period, final int maxForwards, final String accumulatorStoreName) {
		super(ValueType.METER, TimeUnit.SECONDS.toMillis(period), maxForwards, new String[]{accumulatorStoreName});
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
	 * @see com.heliosapm.streams.metrics.processors.AbstractStreamedMetricProcessor#doProcess(java.lang.Object, java.lang.Object)
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
	 * @see com.heliosapm.streams.metrics.processors.AbstractStreamedMetricProcessor#punctuate(long)
	 */
	@Override
	public void punctuate(final long timestamp) {
		final KeyValueIterator<String, Long> iter = metricCountsStore.all();
		try {
			while(iter.hasNext()) {
				try {
					final KeyValue<String, Long> kv = iter.next();
					forward(kv.key, StreamedMetric.fromKey(timestamp, kv.key, kv.value));
				} catch (Exception x) {/* No Op */}
			}
			commit();
		} finally {
			iter.close();
		}
	}
	
	
}