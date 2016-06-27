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

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.ValueType;
import com.heliosapm.streams.metrics.internal.SharedMetricsRegistry;

/**
 * <p>Title: AbstractStreamedMetricProcessor</p>
 * <p>Description: The base class for StreamedMetric processors</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.processor.AbstractStreamedMetricProcessor</code></p>
 */

public abstract class AbstractStreamedMetricProcessor implements StreamedMetricProcessor {
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** The value type processed by this processor */
	protected final ValueType valueType;
	/** The names of data stores used by this processor */
	protected final String[] dataStores;
	
	/** The name of the sink topic name this processor published to */
	protected final String sink;
	/** The injected processor context */
	protected ProcessorContext context = null;
	
	/** The punctuation period */
	protected final long period;
	
	protected final Timer timer = SharedMetricsRegistry.getInstance().timer("StreamedMetricProcessor." + getClass().getSimpleName());


	/**
	 * Creates a new AbstractStreamedMetricProcessor
	 * @param valueType The value type processed by this processor
	 * @param period The period to schedule in ms. Ignored if less than 1
	 * @param sink The name of the sink topic name this processor published to
	 * @param dataStores The names of data stores used by this processor
	 */
	public AbstractStreamedMetricProcessor(final ValueType valueType, final long period, final String sink, final String...dataStores) {
		this.valueType = valueType;
		this.sink = sink;
		this.dataStores = dataStores;
		this.period = period;
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.kstream.Predicate#test(java.lang.Object, java.lang.Object)
	 */
	@Override
	public boolean test(final String key, final StreamedMetric sm) {
		return sm.getValueType()==valueType;
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.processor.ProcessorSupplier#get()
	 */
	@Override
	public Processor<String, StreamedMetric> get() {
		return this;
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.processor.Processor#init(org.apache.kafka.streams.processor.ProcessorContext)
	 */
	@Override
	public void init(final ProcessorContext context) {
		this.context = context;		
		if(period > 0) {
			context.schedule(period);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.processor.Processor#process(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void process(final String key, final StreamedMetric value) {
		log.debug("Processing Metric [{}]", key);
		final Context ctx = timer.time();
		if(doProcess(key, value)) {
			ctx.stop();
		}
	}
	
	protected abstract boolean doProcess(final String key, final StreamedMetric value);

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
		/* No Op */
	}
	
	/**
	 * Returns the names of the data stores used by this processor
	 * @return the names of the data stores used by this processor
	 */
	@Override
	public String[] getDataStoreNames() {
		return dataStores.clone();
	}
	
	private static final StateStoreSupplier[] EMPTY_SS_ARR = {};
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.processor.StreamedMetricProcessor#getStateStores()
	 */
	@Override
	public StateStoreSupplier[] getStateStores() {
		return EMPTY_SS_ARR;
	}

	
	
	/**
	 * Returns the name of the topic this processor publishes to
	 * @return the name of the topic this processor publishes to
	 */
	public String getSink() {
		return sink;
	}
	
	/**
	 * Returns this processor's value type
	 * @return this processor's value type
	 */
	public ValueType getValueType() {
		return valueType;
	}
	
	/**
	 * Returns the scheduled punctuation period
	 * @return the scheduled punctuation period
	 */
	public long getPeriod() {
		return period;
	}
	

}
