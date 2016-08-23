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
package com.heliosapm.streams.metrics.router;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.jmx.export.naming.SelfNaming;

import com.heliosapm.streams.metrics.router.config.StreamsConfigBuilder;
import com.heliosapm.utils.jmx.JMXHelper;


/**
 * <p>Title: MetricRouterBuilder</p>
 * <p>Description: Builds, configures and starts the metric router</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.router.MetricRouterBuilder</code></p>
 */
@ManagedResource
public class MetricRouterBuilder implements SelfNaming, ApplicationContextAware, InitializingBean, DisposableBean, UncaughtExceptionHandler {
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** The streams config builder instance */
	protected StreamsConfigBuilder configBuilder = null;

	/** The kstream builder */
	protected KStreamBuilder kstreamBuilder = null;
	/** The kafka streams engine */
	protected KafkaStreams kafkaStreams = null;
	/** The kafka streams engine config */
	protected StreamsConfig streamsConfig = null;
	
	/** The activated stream nodes */
	protected Map<String, MetricStreamNode> nodes = new ConcurrentHashMap<String, MetricStreamNode>();
	/** The application context */
	protected ApplicationContext appCtx = null;
	/** The router's JMX ObjectName */
	protected ObjectName objectName = JMXHelper.objectName("com.heliosapm.streams.metrics.router:service=MetricRouter");
	
	
	
	/**
	 * <p>Starts the MetricRouter</p>
	 * {@inheritDoc}
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() throws Exception {
		log.info(">>>>> Starting MetricRouter.....");
		final Map<String, MetricStreamNode> locatedNodes = appCtx.getBeansOfType(MetricStreamNode.class);
		if(locatedNodes.isEmpty()) {
			log.warn("No MetricStreamNodes found. MetricRouter is dead");
			return;
		}
		kstreamBuilder = new KStreamBuilder();
		for(MetricStreamNode node: locatedNodes.values()) {
			node.configure(kstreamBuilder);
			nodes.put(node.getName(), node);
			log.info("Configured Node: [{}]", node.getName());
		}
		log.info("Configured [{}] MetricStreamNodes", nodes.size());
		streamsConfig = configBuilder.build();
		kafkaStreams = new KafkaStreams(kstreamBuilder, streamsConfig);
		kafkaStreams.setUncaughtExceptionHandler(this);
		kafkaStreams.start();
		log.info("<<<<< MetricRouter started.");		
	}
	
	/**
	 * <p>Stops the MetricRouter</p>
	 * {@inheritDoc}
	 * @see org.springframework.beans.factory.DisposableBean#destroy()
	 */
	@Override
	public void destroy() throws Exception {
		log.info(">>>>> Stopping MetricRouter.....");
		if(kafkaStreams!=null) {
			kafkaStreams.close();
			kafkaStreams.cleanUp();
		}		
		log.info("<<<<< MetricRouter stopped.");				
	}
	
	/**
	 * Returns the node names operating in the MetricRouter
	 * @return the node names operating in the MetricRouter
	 */
	@ManagedAttribute(description="The node names operating in the MetricRouter")
	public Set<String> getNodeNames() {
		return new HashSet<String>(nodes.keySet());
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
	 */
	@Override
	public void setApplicationContext(final ApplicationContext appCtx) throws BeansException {
		this.appCtx = appCtx;
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
	 * Returns the config builder
	 * @return the config builder
	 */
	public StreamsConfigBuilder getConfigBuilder() {
		return configBuilder;
	}

	/**
	 * Sets the configuration builder
	 * @param configBuilder the streams configuration
	 */
	@Required
	public void setConfigBuilder(final StreamsConfigBuilder configBuilder) {
		this.configBuilder = configBuilder;
	}

	@Override
	public void uncaughtException(Thread t, Throwable e) {
		// TODO Auto-generated method stub
		
	}
	
	
	
	
}
