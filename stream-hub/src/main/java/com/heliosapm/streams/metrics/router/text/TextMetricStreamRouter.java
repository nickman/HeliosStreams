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
package com.heliosapm.streams.metrics.router.text;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.processors.StreamedMetricProcessorSupplier;
import com.heliosapm.streams.metrics.router.config.StreamsConfigBuilder;
import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.reflect.PrivateAccessor;

/**
 * <p>Title: TextMetricStreamRouter</p>
 * <p>Description: Subscribes to topics that supply {@link StreamedMetric}s in string format,
 * transforms them to {@link StreamedMetric}s in binary format and publishes them to binary metric endpooints.</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.router.text.TextMetricStreamRouter</code></p>
 */
@ManagedResource(objectName="com.heliosapm.streams.metrics.router.text:service=TextMetricStreamRouter")
public class TextMetricStreamRouter implements InitializingBean, DisposableBean, BeanNameAware {
	
	/** The configuration key for the names of the topics to listen on */
	public static final String CONFIG_TOPICS = "processor.topics";
	
	/** Instance logger */
	protected Logger log = LogManager.getLogger(getClass());
	/** The bean name */
	protected String beanName = null;
	/** The streams configuration properties */
	protected final Properties config;
	/** The streams config */
	protected final StreamsConfig streamsConfig;
	/** The injected processor suppliers */
	protected final List<StreamedMetricProcessorSupplier<?,?,?,?>> processorSuppliers = new ArrayList<StreamedMetricProcessorSupplier<?,?,?,?>>();
	
	/** The stream topology builder */
	protected final TopologyBuilder builder;
	
	/** The streams instance handling the continuous input */
	protected KafkaStreams kafkaStreams;
	
	public static void main(String[] args) {
		final StreamsConfigBuilder conf = new StreamsConfigBuilder();
		conf.setBootstrapServers("localhost:9093", "localhost:9094");
		final Properties p = conf.buildProperties();
		p.put("processor.topics", "tsdb.metrics.accumulator,tsdb.metrics.st");
		final TextMetricStreamRouter router = new TextMetricStreamRouter(p);
		router.setBeanName("Hallo");
		try {
			router.afterPropertiesSet();
			StdInCommandHandler.getInstance().registerCommand("stop", new Runnable(){
				public void run() {
					try {
						router.destroy();
					} catch (Exception ex) {
						ex.printStackTrace(System.err);
					} finally {
						Runtime.getRuntime().halt(1);
					}
				}
			});
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			Runtime.getRuntime().halt(-1);
		}
	}
	
	 
	/**
	 * Creates a new TextMetricStreamRouter
	 * @param config The streaming configuration properties
	 */
	public TextMetricStreamRouter(final Properties config) {
		if(config == null || config.isEmpty()) throw new IllegalArgumentException("The passed config properties was null or empty");		
		this.config = config;
		log.info("Configuring TextMetricStreamRouter...");
		streamsConfig = new StreamsConfig(this.config);
		builder = new TopologyBuilder();
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.springframework.beans.factory.BeanNameAware#setBeanName(java.lang.String)
	 */
	@Override
	public void setBeanName(final String name) {
		this.beanName = name;
		
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() throws Exception {
		log.info("\n\t=================================================\n\tStarting {} ...\n\t=================================================", beanName);
		for(StreamedMetricProcessorSupplier<?,?,?,?> processorSupplier : processorSuppliers) {
			processorSupplier.configure(builder, null);
		}
		kafkaStreams = new KafkaStreams(builder, config);
		kafkaStreams.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
			@Override
			public void uncaughtException(final Thread t, final Throwable e) {
				log.error("Uncaught exception on thread [{}]", t, e);
				
			}
		});
		kafkaStreams.start(); 		
		log.info("\n\t=================================================\n\t{} Started\n\t=================================================", beanName);
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.springframework.beans.factory.DisposableBean#destroy()
	 */
	@Override
	public void destroy() throws Exception {
		log.info("\n\t=================================================\n\tStopping {} ...\n\t=================================================", beanName);
		if(kafkaStreams!=null) kafkaStreams.close();
//		for(StreamedMetricProcessorSupplier<?,?,?,?> p: processorSuppliers ) {
//			p.shutdown();
//		}
//		processorSuppliers.clear();
				
		log.info("\n\t=================================================\n\t{} Stopped\n\t=================================================", beanName);
	}

	public static enum StreamState {
	    CREATED,
	    RUNNING,
	    STOPPED,
	    UNKNOWN;
		
		private static final StreamState[] values = values();
		
		public static StreamState decode(final int code) {
			try {
				return values[code];
			} catch (Exception ex) {
				return StreamState.UNKNOWN;
			}
		}
		
		
	}
	
	@ManagedAttribute(description="The KafkaStreams state")
	public String getStreamState() {
		if(kafkaStreams!=null) {
			return StreamState.decode((Integer)PrivateAccessor.getFieldValue(kafkaStreams, "state")).name();
		}
		return null;
	}

	/**
	 * Returns the processor suppliers
	 * @return the processorSuppliers
	 */
	public List<StreamedMetricProcessorSupplier<?, ?, ?, ?>> getProcessorSuppliers() {
		return processorSuppliers;
	}

	/**
	 * Sets the processor suppliers
	 * @param processorSuppliers the processorSuppliers
	 */
	public void setProcessorSuppliers(final List<StreamedMetricProcessorSupplier<?, ?, ?, ?>> processorSuppliers) {
		this.processorSuppliers.addAll(processorSuppliers);
	}

}
