package com.heliosapm.streams.metrics.processors.impl;

import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.ValueType;
import com.heliosapm.streams.metrics.processors.AbstractStreamedMetricProcessor;

/**
 * <p>Title: StraightThroughMetricProcessor</p>
 * <p>Description: MetricProcessor that simply converts text line metrics to bin and forwards to the TSD.</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.processors.impl.StraightThroughMetricProcessor</code></p>
 */
class StraightThroughMetricProcessor extends  AbstractStreamedMetricProcessor<String, StreamedMetric> {

	/**
	 * Creates a new StraightThroughMetricProcessor
	 * @param period The period of the context commit if the max number of forwards has not been met. 
	 * @param maxForwards The maximum number of metrics to forward without a commit
	 */
	protected StraightThroughMetricProcessor(final long period, final int maxForwards) {
		super(ValueType.STRAIGHTTHROUGH, period, maxForwards);
		
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.processors.AbstractStreamedMetricProcessor#doProcess(java.lang.Object, java.lang.Object)
	 */
	@Override
	protected boolean doProcess(final String key, final StreamedMetric value) {
		forward(key, value);
		return true;
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.processors.AbstractStreamedMetricProcessor#punctuate(long)
	 */
	@Override
	public void punctuate(final long timestamp) {
		commit();
	}
	
}