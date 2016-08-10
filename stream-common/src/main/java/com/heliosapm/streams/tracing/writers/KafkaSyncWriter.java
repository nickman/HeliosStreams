/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.heliosapm.streams.tracing.writers;

import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.StreamedMetricSerializer;
import com.heliosapm.streams.tracing.AbstractMetricWriter;
import com.heliosapm.utils.collections.Props;
import com.heliosapm.utils.config.ConfigurationHelper;

/**
 * <p>Title: KafkaSyncWriter</p>
 * <p>Description: Metric writer that writes metrics to a kafka topic</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tracing.writers.KafkaSyncWriter</code></p>
 */

public class KafkaSyncWriter extends AbstractMetricWriter {
	
	/** The configuration properties prefix */
	public static final String CONFIG_PREFIX = "metricwriter.kafka.";
	
	/** The config key for the topic name[s] to publish to */
	public static final String CONFIG_TOPICS = CONFIG_PREFIX + "topics";
	/** The default topic name[s] to publish to (there are none) */
	public static final String[] DEFAULT_TOPICS = {};
	
	/** The config key for the shutdown time in secs. allowed to send remaining messages */
	public static final String CONFIG_SHUTDOWN_TIME = CONFIG_PREFIX + "shutdown.time";
	/** The default shutdown time in secs. allowed to send remaining messages */
	public static final int DEFAULT_SHUTDOWN_TIME = 5;
	
	/** The kafka producer used to forward metrics */ 
	protected KafkaProducer<String, StreamedMetric> producer = null;
	/** The kafka producer configuration properties */
	protected final Properties producerProperties = new Properties();
	/** The topic names to publish to */
	protected String[] topics = DEFAULT_TOPICS;
	/** The shutdown time in seconds */
	protected int shutdownTime = DEFAULT_SHUTDOWN_TIME;
	
	
	/**
	 * Creates a new KafkaSyncWriter
	 */
	public KafkaSyncWriter() {
		super(false, true);
	}
	
	/**
	 * Creates a new KafkaSyncWriter
	 * @param sync true for sync, false for subclassed async class
	 */
	protected KafkaSyncWriter(final boolean sync) {
		super(sync, true);
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.AbstractMetricWriter#configure(java.util.Properties)
	 */
	@Override
	public void configure(final Properties config) {
		super.configure(config);
		this.config = config;
		final Properties nonKafkaProps = Props.extract(CONFIG_PREFIX, config, false, true);
		topics = ConfigurationHelper.getArraySystemThenEnvProperty(CONFIG_TOPICS, DEFAULT_TOPICS, nonKafkaProps);
		shutdownTime = ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_SHUTDOWN_TIME, DEFAULT_SHUTDOWN_TIME, nonKafkaProps);
		if(topics.length==0) throw new IllegalArgumentException("No topics defined. Define some in the property [" + CONFIG_TOPICS + "]");
		final Properties p = Props.extract(CONFIG_PREFIX, config, true, false);
		final Properties sp = Props.extract(CONFIG_PREFIX, System.getProperties(), true, false);
		producerProperties.putAll(sp);
		producerProperties.putAll(p);		
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.google.common.util.concurrent.AbstractIdleService#startUp()
	 */
	@Override
	protected void startUp() throws Exception {
		producer = new KafkaProducer<String, StreamedMetric>(producerProperties, new StringSerializer(), new StreamedMetricSerializer());
	}



	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.AbstractMetricWriter#doMetrics(java.util.Collection)
	 */
	@Override
	protected void doMetrics(final Collection<StreamedMetric> metrics) {
		if(metrics==null || metrics.isEmpty()) return;
		for(StreamedMetric sm: metrics) {
			if(sm==null) continue;
			for(String topic: topics) {
				producer.send(new ProducerRecord<String, StreamedMetric>(topic, sm.getMetricName(), sm));
			}
		}
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.AbstractMetricWriter#doMetrics(com.heliosapm.streams.metrics.StreamedMetric[])
	 */
	@Override
	protected void doMetrics(StreamedMetric... metrics) {
		if(metrics==null || metrics.length==0) return;
		for(StreamedMetric sm: metrics) {
			if(sm==null) continue;
			for(String topic: topics) {
				producer.send(new ProducerRecord<String, StreamedMetric>(topic, sm.getMetricName(), sm));
			}
		}
	}


	/**
	 * {@inheritDoc}
	 * @see com.google.common.util.concurrent.AbstractIdleService#shutDown()
	 */
	@Override
	protected void shutDown() throws Exception {
		if(producer!=null) {
			producer.close(shutdownTime, TimeUnit.SECONDS);
		}
	}
	
	/**
	 * TODO: implement
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.MetricWriterMXBean#getCustomState()
	 */
	@Override
	public String getCustomState() {
		final StringBuilder b = new StringBuilder();
		return b.toString();
	}
	

}
