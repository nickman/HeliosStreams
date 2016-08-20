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
package com.heliosapm.streams.metrics.processors;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.store.StateStoreDefinition;

/**
 * <p>Title: AbstractStreamedMetricProcessorSupplier</p>
 * <p>Description: The abstract base class for concrete {@link StreamedMetricProcessorSupplier} implementations</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.processors.impl.AbstractStreamedMetricProcessorSupplier</code></p>
 * @param <K> The expected type of the source key
 * @param <V> The expected type of the source value
 * @param <SK> The expected type of the sink key
 * @param <SV> The expected type of the sink value
 */
public abstract class AbstractStreamedMetricProcessorSupplier<K, V, SK, SV> implements ApplicationContextAware, BeanNameAware, StreamedMetricProcessorSupplier<K, V, SK, SV> {
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** The spring app context */
	protected ApplicationContext appCtx = null;
	/** The punctuation period in ms. */
	protected long period = -1L;
	/** The topic sink name for this processor */
	protected String topicSink  = null;
	/** The source names */
	protected String[] sources = null;
	/** The source key Serde */
	protected Serde<K> sourceKeySerde = null;
	/** The source value Serde */
	protected Serde<V> sourceValueSerde = null;
	/** The sink key Serde */
	protected Serde<SK> sinkKeySerde = null;
	/** The sink value Serde */
	protected Serde<SV> sinkValueSerde = null;
	
	/** All processors created from this supplier */
	protected final List<Processor<String, StreamedMetric>> startedProcessors = new CopyOnWriteArrayList<Processor<String, StreamedMetric>>();
	
	/** The topology name for this processor's source */
	protected String sourceName = null;
	/** The topology name for this processor */
	protected String processorName = null;
	/** The topology name for this processor's sink */
	protected String sinkName = null;
	
	/** The definitions of the state stores declared by this processor supplier */
	protected Set<StateStoreDefinition<?, ?>> stateStoreDefinitions = new HashSet<StateStoreDefinition<?, ?>>();
	
	/** The spring bean name */
	protected String beanName = null;
	
	/**
	 * Creates a new AbstractStreamedMetricProcessorSupplier
	 */
	protected AbstractStreamedMetricProcessorSupplier() {
		log.info("Created Processor Supplier [{}] : [{}]", getClass().getSimpleName(), System.identityHashCode(this));
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.springframework.beans.factory.BeanNameAware#setBeanName(java.lang.String)
	 */
	@Override
	public void setBeanName(final String name) {
		beanName = name;	
		sourceName = beanName + SOURCE_NAME_SUFFIX;
		processorName = beanName + PROCESSOR_NAME_SUFFIX;
		sinkName = beanName + SINK_NAME_SUFFIX;
		
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.processors.StreamedMetricProcessorSupplier#configure(org.apache.kafka.streams.processor.TopologyBuilder, java.lang.String)
	 */
	@Override
	public String configure(TopologyBuilder builder, String textLineSourceName) {
		
//		String ACC_TOPIC_NAME = "tsdb.metrics.accumulator";
//		String ACC_SOURCE_NAME = "accumulator";						//  beanName + ".SOURCE";
//		String ACC_PROCESSOR_NAME = "accumulationProcessor";
//		String ACC_SINK_NAME = "accumulationSink";
//		String ACC_TOPIC_OUT_NAME = "tsdb.metrics.binary";
		
		// HeliosSerdes.STREAMED_METRIC_DESER_FROM_STRING
		
		builder.addSource(sourceName, sourceKeySerde.deserializer(), sourceValueSerde.deserializer(), sources)
			.addProcessor(processorName, this, sourceName)
			.addSink(sinkName, topicSink, sinkKeySerde.serializer(), sinkValueSerde.serializer(), processorName);
		
		for(StateStoreDefinition<?,?> stateStoreDef: stateStoreDefinitions) {
			builder.addStateStore(stateStoreDef, processorName);
		}
		
//		builder.addSource(ACC_SOURCE_NAME, stringDeserializer, stringToMetricDeser, ACC_TOPIC_NAME)			
//		.addProcessor(ACC_PROCESSOR_NAME, router.route(ValueType.ACCUMULATOR, builder), ACC_SOURCE_NAME)
//		.addSink(ACC_SINK_NAME, ACC_TOPIC_OUT_NAME, stringSerializer, metricSerde.serializer(), ACC_PROCESSOR_NAME);
//		
		return processorName;
		
	}

	/**
	 * Returns the state store definitions
	 * @return the stateStoreDefinitions
	 */
	@Override
	public Set<StateStoreDefinition<?, ?>> getStateStoreDefinitions() {
		return new HashSet<StateStoreDefinition<?, ?>>(stateStoreDefinitions);
	}

	/**
	 * Adds the passed stateStoreDefinitions to this processor suppliers definitions
	 * @param stateStoreDefinitions the stateStoreDefinitions to set
	 */
	public void setStateStoreDefinitions(final Set<StateStoreDefinition<?, ?>> stateStoreDefinitions) {
		this.stateStoreDefinitions.addAll(stateStoreDefinitions);
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.processors.StreamedMetricProcessorSupplier#getStateStoreNames()
	 */
	@Override
	public String[] getStateStoreNames() {
		final Set<String> names = new HashSet<String>(stateStoreDefinitions.size());
		for(StateStoreDefinition<?, ?> ssd: stateStoreDefinitions) {
			names.add(ssd.name());
		}
		return names.toArray(new String[names.size()]);
	}

	/**
	 * Returns the punctuation period in ms.
	 * @return the period
	 */
	public long getPeriod() {
		return period;
	}

	/**
	 * Sets the punctuation period in ms.
	 * @param period the period to set
	 */	
	public void setPeriod(final long period) {
		this.period = period;
	}

	/**
	 * Returns the names of the source topics
	 * @return the sources
	 */
	public String[] getSources() {
		return sources;
	}

	/**
	 * Sets the names of the source topics
	 * @param sources the sources to set
	 */
	@Required
	public void setSources(final String[] sources) {
		this.sources = sources;
	}

	/**
	 * Returns the source key Serde
	 * @return the sourceKeySerde
	 */
	public Serde<K> getSourceKeySerde() {
		return sourceKeySerde;
	}

	/**
	 * Sets the source key Serde
	 * @param sourceKeySerde the sourceKeySerde to set
	 */
	@Required
	public void setSourceKeySerde(final Serde<K> sourceKeySerde) {
		this.sourceKeySerde = sourceKeySerde;
	}

	/**
	 * Returns the source value Serde
	 * @return the sourceValueSerde
	 */
	public Serde<V> getSourceValueSerde() {
		return sourceValueSerde;
	}

	/**
	 * Sets the source value Serde
	 * @param sourceValueSerde the sourceValueSerde to set
	 */
	@Required
	public void setSourceValueSerde(final Serde<V> sourceValueSerde) {
		this.sourceValueSerde = sourceValueSerde;
	}

	/**
	 * Returns the sink key Serde
	 * @return the sinkKeySerde
	 */
	public Serde<SK> getSinkKeySerde() {
		return sinkKeySerde;
	}

	/**
	 * Sets the sink key Serde
	 * @param sinkKeySerde the sinkKeySerde to set
	 */
	@Required
	public void setSinkKeySerde(final Serde<SK> sinkKeySerde) {
		this.sinkKeySerde = sinkKeySerde;
	}

	/**
	 * Returns the sink value Serde
	 * @return the sinkValueSerde
	 */
	public Serde<SV> getSinkValueSerde() {
		return sinkValueSerde;
	}

	/**
	 * Sets the sink value Serde
	 * @param sinkValueSerde the sinkValueSerde to set
	 */
	@Required
	public void setSinkValueSerde(final Serde<SV> sinkValueSerde) {
		this.sinkValueSerde = sinkValueSerde;
	}

	/**
	 * Returns the spring bean name 
	 * @return the beanName
	 */
	public String getBeanName() {
		return beanName;
	}

	/**
	 * Returns the name opf the topic to sink to
	 * @return the topicSink
	 */
	public String getTopicSink() {
		return topicSink;
	}

	/**
	 * Sets the name opf the topic to sink to
	 * @param topicSink the topicSink to set
	 */
	@Required
	public void setTopicSink(final String topicSink) {
		this.topicSink = topicSink;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.processors.StreamedMetricProcessorSupplier#shutdown()
	 */
	@Override
	public void shutdown() {
		log.info(">>>>>  Stopping [{}]...", getClass().getSimpleName());
		for(Processor<String, StreamedMetric> p: startedProcessors) {
			try {
				log.info(">>>>>  Stopping Processor [{}]...", p.getClass().getSimpleName());
				p.close();
				log.info("<<<<<  Processor [{}] Stopped.", p.getClass().getSimpleName());
			} catch (Exception ex) {
				log.error("Failed to close processor [{}]", p, ex);
			}
		}
		startedProcessors.clear();		
		log.info("<<<<<  Stopped [{}]", getClass().getSimpleName());
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
	 */
	@Override
	public void setApplicationContext(final ApplicationContext appCtx) throws BeansException {
		this.appCtx = appCtx;		
	}

//	/**
//	 * {@inheritDoc}
//	 * @see org.apache.kafka.streams.processor.ProcessorSupplier#get()
//	 */
//	@Override
//	public Processor<K, V> get() {
//		// TODO Auto-generated method stub
//		return null;
//	}


}
