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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.streams.processor.Processor;

import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.ValueType;
import com.heliosapm.streams.metrics.processors.AbstractStreamedMetricProcessor;
import com.heliosapm.streams.metrics.processors.AbstractStreamedMetricProcessorSupplier;

/**
 * <p>Title: StraightThroughMetricProcessorSupplier</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.processors.impl.StraightThroughMetricProcessorSupplier</code></p>
 */

public class StraightThroughMetricProcessorSupplier extends AbstractStreamedMetricProcessorSupplier<String, StreamedMetric, String, StreamedMetric> {
	/** The maximum number of metrics to forward without a commit */
	protected int maxForwards = 1000;

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.processor.ProcessorSupplier#get()
	 */
	@Override
	public Processor<String, StreamedMetric> get() {		
		return new StraightThroughMetricProcessor(period, maxForwards);
	}
	
	/**
	 * Returns the maximum number of metrics to forward without a commit
	 * @return the maxForwards
	 */
	public int getMaxForwards() {
		return maxForwards;
	}
	
	/**
	 * Sets the maximum number of metrics to forward without a commit
	 * @param maxForwards the maxForwards to set
	 */
	public void setMaxForwards(final int maxForwards) {
		if(maxForwards < 1) throw new IllegalArgumentException("Invalid maxForwards value:" + maxForwards);
		this.maxForwards = maxForwards;
	}
	

	/**
	 * <p>Title: StraightThroughMetricProcessor</p>
	 * <p>Description: MetricProcessor that simply converts text line metrics to bin and forwards to the TSD.</p> 
	 * <p>Company: Helios Development Group LLC</p>
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>com.heliosapm.streams.metrics.processors.impl.StraightThroughMetricProcessorSupplier.StraightThroughMetricProcessor</code></p>
	 */
	static class StraightThroughMetricProcessor extends  AbstractStreamedMetricProcessor {
		/** The number of forwards without a commit */
		protected final AtomicInteger forwardCounter = new AtomicInteger(0);
		/** CAS lock on commit */
		protected final AtomicBoolean inCommit = new AtomicBoolean(false);
		/** The maximum number of metrics to forward without a commit */
		protected final int maxForwards;

		/**
		 * Creates a new StraightThroughMetricProcessor
		 * @param period The period of the context commit if the max number of forwards has not been met.
		 * @param maxForwards The maximum number of metrics to forward without a commit
		 */
		protected StraightThroughMetricProcessor(final long period, final int maxForwards) {
			super(ValueType.STRAIGHTTHROUGH, period);
			this.maxForwards = maxForwards;
		}

		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.metrics.processors.AbstractStreamedMetricProcessor#doProcess(java.lang.String, com.heliosapm.streams.metrics.StreamedMetric)
		 */
		@Override
		protected boolean doProcess(final String key, final StreamedMetric value) {
			context.forward(key, value);
			if(forwardCounter.incrementAndGet() > maxForwards) {
				commit();
			}
			return true;
		}
		
		protected void commit() {
			for(; ;) {
				if(inCommit.compareAndSet(false, true)) {
					final int current = forwardCounter.get(); 
					if(current > 0) {
						try {
							context.commit();
							forwardCounter.set(0);							
						} finally {
							inCommit.set(false);
						}
						log.info("Comitted [{}] messages", current);
						return;
					}
				}
			}
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

}
