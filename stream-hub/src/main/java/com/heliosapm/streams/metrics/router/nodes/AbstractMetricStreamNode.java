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
package com.heliosapm.streams.metrics.router.nodes;

import java.util.concurrent.atomic.AtomicLong;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedMetric;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.jmx.export.naming.SelfNaming;
import org.springframework.jmx.support.MetricType;

import com.heliosapm.streams.metrics.router.StreamHubKafkaClientSupplier;
import com.heliosapm.utils.jmx.JMXHelper;

import jsr166e.LongAdder;

/**
 * <p>Title: AbstractMetricStreamNode</p>
 * <p>Description: An abstract MetricStreamNode for extension. Supplies some spring boilder plate</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.router.nodes.AbstractMetricStreamNode</code></p>
 */
@ManagedResource
public abstract  class AbstractMetricStreamNode implements MetricStreamNode, BeanNameAware, SelfNaming {
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());	
	/** The node name */
	protected String nodeName = null;
	/** The router's JMX ObjectName */
	protected ObjectName objectName = null;
	/** A count of inbound messages */
	protected final LongAdder inboundCount = new LongAdder();
	/** A count of outbound messages */
	protected final LongAdder outboundCount = new LongAdder();
	/** The timestamp of the last metric reset */
	protected final AtomicLong lastMetricReset = new AtomicLong(-1);
	/** The source topics */
	protected String[] sourceTopics = null;
	/** The sink topic */
	protected String sinkTopic = null;
	/** Indicates if the key of forwarded messages should be the full metric key, or just the metric name */
	protected boolean fullKey = false;
	/** Indicates if stream stores created by this node should be persistent (true) or in-memory (false) */
	protected boolean persistentStores = false;
	
	/** The client supplier */
	protected KafkaClientSupplier clientSupplier = null;
	/** The streams engine */
	protected KafkaStreams kafkaStreams = null;
	/** The processor context for child classes that implement processor */
	protected ProcessorContext processorCtx = null;

	/** Indicates if this JVM is running in Windows */
	protected static final boolean IS_WINDOWS = System.getProperty("os.name").toLowerCase().contains("windows");
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.router.nodes.MetricStreamNode#close()
	 */
	public void close()  {
		/* No Op */
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.router.nodes.MetricStreamNode#onStart(com.heliosapm.streams.metrics.router.StreamHubKafkaClientSupplier, org.apache.kafka.streams.KafkaStreams)
	 */
	@Override
	public void onStart(final StreamHubKafkaClientSupplier clientSupplier, final KafkaStreams kafkaStreams) {
		this.clientSupplier = clientSupplier;		
		this.kafkaStreams = kafkaStreams;
	}	

	/**
	 * Resets this node's metrics
	 */
	@ManagedOperation(description="Resets this node's metrics")
	public void resetMetrics() {
		inboundCount.reset();
		outboundCount.reset();
		lastMetricReset.set(System.currentTimeMillis());
	}
	
	/**
	 * Acquires the current metric set for this node, then resets them.
	 * @return A long array where indexes are: <ul>
	 * 	<li><b>0</b>: The total number of inbound messages ingested by this node since the lasy reset</li>
	 * 	<li><b>1</b>: The total number of outbound messages emitted by this node since the lasy reset</li>
	 * </ul>
	 */
	@ManagedOperation(description="Acquires the current metric set for this node, then resets them")
	public long[] resetMetricsAndGet() {
		try {
			return new long[] {
				inboundCount.sumThenReset(),
				outboundCount.sumThenReset()
			};
		} finally {
			lastMetricReset.set(System.currentTimeMillis());
		}
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.router.nodes.MetricStreamNode#getName()
	 */
	@Override
	public String getName() {		
		return nodeName;
	}

	/**
	 * {@inheritDoc}
	 * @see org.springframework.beans.factory.BeanNameAware#setBeanName(java.lang.String)
	 */
	@Override
	public void setBeanName(final String name) {
		nodeName = name;
		objectName = JMXHelper.objectName("com.heliosapm.streams.metrics.router.node:name=" + nodeName);
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.springframework.jmx.export.naming.SelfNaming#getObjectName()
	 */
	@Override
	public ObjectName getObjectName() throws MalformedObjectNameException {	
		return objectName;
	}
	
	/**
	 * Returns the total number of inbound messages ingested by 
	 * this node since startup or the last reset.
	 * @return the total number of inbound messages
	 */
	@ManagedMetric(description="The total number of inbound messages", metricType=MetricType.COUNTER, category="MetricStreamNode", displayName="InboundMessages")
	public long getInboundCount() {
		return inboundCount.longValue();
	}


	
	/**
	 * Returns the total number of outbound messages dispatched by 
	 * this node since startup or the last reset.
	 * @return the total number of outbound messages
	 */
	@ManagedMetric(description="The total number of outbound messages", metricType=MetricType.COUNTER, category="MetricStreamNode", displayName="OutboundMessages")
	public long getOutboundCount() {
		return outboundCount.longValue();
	}

	/**
	 * Returns the source topics names this node will consume from
	 * @return the source topic names
	 */
	@ManagedAttribute(description="The source topics names this node will consume from")
	public String[] getSourceTopics() {
		return sourceTopics;
	}

	/**
	 * Sets the source topic names this node will consume from
	 * @param sourceTopics the source topic names to set
	 */
	public void setSourceTopics(final String[] sourceTopics) {
		if(sourceTopics==null || sourceTopics.length==0) throw new IllegalArgumentException("The passed source topic name array was null or zero length");
		for(int i = 0; i < sourceTopics.length; i++) {
			if(sourceTopics[i]==null) throw new IllegalArgumentException("The source topic name at index [" + i + "] was null");
			sourceTopics[i] = sourceTopics[i].trim();
			if(sourceTopics[i].isEmpty()) throw new IllegalArgumentException("The source topic name at index [" + i + "] was empty");
			
		}
		this.sourceTopics = sourceTopics; 
	}

	/**
	 * Returns the sink topic name or null if one was not assigned
	 * @return the sink topic name or null
	 */
	@ManagedAttribute(description="The sink topic name this node will sink tos")
	public String getSinkTopic() {
		return sinkTopic;
	}

	/**
	 * Sets the sink topic name
	 * @param sinkTopic the sink topic name
	 */
	public void setSinkTopic(final String sinkTopic) {
		if(sinkTopic==null || sinkTopic.trim().isEmpty()) throw new IllegalArgumentException("The passed sink topic name was empty or null");
		this.sinkTopic = sinkTopic.trim();
	}

	/**
	 * Indicates if this node is forwarding messages using the full metric key as the message key
	 * or only the metric name 
	 * @return the fullKey true for full metric key, false otherwise
	 */
	@ManagedAttribute(description="Indicates if forwarded messages use the full metric key or just the metric name")
	public boolean isFullKey() {
		return fullKey;
	}

	/**
	 * Set to true to use the full metric key as the forwarded message key, false for just the metric name 
	 * @param fullKey the fullKey to set
	 */
	public void setFullKey(final boolean fullKey) {
		this.fullKey = fullKey;
	}
	
	public void init(final ProcessorContext context) {
		this.processorCtx = context;
	}

	/**
	 * Indicates if created state stores should be persistent or in-memory
	 * @return true if created state stores should be persistent, false if in-memory
	 */
	@ManagedAttribute(description="Indicates if created state stores should be persistent or in-memory")
	public boolean isPersistentStores() {
		return persistentStores;
	}

	/**
	 * Specifies if created state stores should be persistent or in-memory.
	 * Note that windows does not support persistent stores yet, so if operating in windows, 
	 * this will be forced to false. 
	 * @param persistentStores true true if created state stores should be persistent, false if in-memory
	 */
	public void setPersistentStores(final boolean persistentStores) {
		this.persistentStores = IS_WINDOWS ? false : persistentStores;
	}

	


}
