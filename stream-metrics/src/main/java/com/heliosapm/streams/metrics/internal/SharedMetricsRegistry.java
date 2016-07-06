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
package com.heliosapm.streams.metrics.internal;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;

import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer.Context;
import com.heliosapm.utils.jmx.JMXHelper;

/**
 * <p>Title: SharedMetricsRegistry</p>
 * <p>Description: A shared singleton metrics repository</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.internal.SharedMetricsRegistry</code></p>
 */

public class SharedMetricsRegistry extends MetricRegistry implements SharedMetricsRegistryMBean {   // ObjectNameFactory
	/** The single instance */
	private static volatile SharedMetricsRegistry instance = null;
	/** The single instance ctor lock */
	private static final Object lock = new Object();
	
	/** Instance logger */
	private final Logger log = LoggerFactory.getLogger(getClass());
	/** The JMX reporter for the registry */
	private final JmxReporter jmxReporter;
	
	/**
	 * Acquires the SharedMetricsRegistry singleton instance
	 * @return the SharedMetricsRegistry singleton instance
	 */
	public static SharedMetricsRegistry getInstance() {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					instance = new SharedMetricsRegistry();
				}
			}
		}
		return instance;
	}
	
	/**
	 * Creates a new SharedMetricsRegistry
	 */
	private SharedMetricsRegistry() {
		super();
		jmxReporter = JmxReporter.forRegistry(this)
				.registerWith(JMXHelper.getHeliosMBeanServer())
				.inDomain("tsdb.agent.metrics")
//				.createsObjectNamesWith(this)
				.build();
		jmxReporter.start();
		log.info("Created SharedMetricsRegistry");
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.codahale.metrics.MetricRegistry#buildMap()
	 */
	@Override
	protected ConcurrentMap<String, Metric> buildMap() {		
		return new NonBlockingHashMap<String, Metric>();
	}
	
	/**
	 * Finds the named timer, starts a timer context and returns it
	 * @param name The metric name
	 * @return the timer context
	 */
	public Context startTimer(final String name) {
		return timer(name).time();
	}
	
	/**
	 * Creates and registers a new gauge
	 * @param name The name to register the gauge as
	 * @param provider The callable that provides the value of the gauge
	 * @return the gauge value
	 * @param <T> The value type of the gauge
	 */
	public <T> Gauge<T> gauge(final String name, final Callable<T> provider) {
		final Gauge<T> g = new Gauge<T>() {
			@Override
			public T getValue() {	
				try {
					return provider.call();
				} catch (Exception ex) {
					return null;
				}
			}
		};
		this.register(name, g);
		return g;
	}
	

//	@Override
//	public ObjectName createName(final String type, final String domain, final String name) {
//		final Map<String, String> tags = new HashMap<String, String>();
//		tags.put("mtype", type);
//		final StringBuilder b = new StringBuilder();
//		for(String s: StringHelper.splitString(name, '/', true)) {
//			
//		}
//		return null;
//	}


	
}
