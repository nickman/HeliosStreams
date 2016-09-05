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
package com.heliosapm.streams.common.metrics;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;

import javax.management.ObjectName;

import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.cliffc.high_scale_lib.NonBlockingHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.DefaultObjectNameFactory;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ObjectNameFactory;
import com.codahale.metrics.Reporter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.heliosapm.streams.jmx.metrics.DropWizardMetrics;
import com.heliosapm.utils.jmx.JMXHelper;

/**
 * <p>Title: SharedMetricsRegistry</p>
 * <p>Description: A shared singleton metrics repository</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.common.metrics.SharedMetricsRegistry</code></p>
 */

public class SharedMetricsRegistry extends MetricRegistry implements SharedMetricsRegistryMBean, ObjectNameFactory, Reporter {   // ObjectNameFactory
	/** The single instance */
	private static volatile SharedMetricsRegistry instance = null;
	/** The single instance ctor lock */
	private static final Object lock = new Object();
	
	/** Instance logger */
	private final Logger log = LoggerFactory.getLogger(getClass());
	/** The default JMX reporter for the registry if ours fails */
	private final JmxReporter jmxReporter;
	
	/** The default ObjectNameFactory if ours fails */
	protected static final DefaultObjectNameFactory DEFAULT_ON_FACTORY = new DefaultObjectNameFactory();
	
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
				.inDomain("agent.metrics")
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
		this.register("gauge." + name, g);
		return g;
	}
	
	//addMetric(final Metric metric, final String name, final String description)
	
	private static final DropWizardMetrics PLACEHOLDER = new DropWizardMetrics(JMXHelper.objectName("fake:name=fake"), "Placeholder");
	
	protected final NonBlockingHashMap<ObjectName, DropWizardMetrics> objectNameMetrics = new NonBlockingHashMap<ObjectName, DropWizardMetrics>(); 
	
	protected synchronized void installMXBean(final ObjectName objectName, final String name, String description, final Metric metric) {
		if(objectName==null) throw new IllegalArgumentException("The passed ObjectName was null");
		if(name==null || name.trim().isEmpty()) throw new IllegalArgumentException("The passed name was null or empty");
		if(metric==null) throw new IllegalArgumentException("The passed Metric was null");
		
		DropWizardMetrics dwm = objectNameMetrics.putIfAbsent(objectName, PLACEHOLDER);
		if(dwm==null || dwm==PLACEHOLDER) {
			dwm = new DropWizardMetrics(objectName, description);
			objectNameMetrics.replace(objectName, dwm);
			log.info("Creating new DWM for [{}}", objectName);
			try {
				JMXHelper.registerMBean(dwm, objectName);
			} catch (Exception ex) {
				/* No Op ? */
				log.error("Error registering DWM: [{}]", objectName, ex);
			}
		} else {
			log.info("Found existing DWM for [{}}", objectName);
		}
		dwm.addMetric(metric, name, description);
	}
	
	protected void installMXBean(final ObjectName objectName, final String name, final Metric metric) {
		installMXBean(objectName, name, null, metric);
	}
	
	/**
	 * Creates a counter and registers it in an MXBean
	 * @param objectName The JMX ObjectName for the MXBean
	 * @param name The prefix for the MXBean attribute names
	 * @param description An optional description 
	 * @return the created counter
	 */
	public Counter mxCounter(final ObjectName objectName, final String name, String description) {
		final Counter metric = new Counter();
		installMXBean(objectName, name, description, metric);
		return metric;		
	}
	
	/**
	 * Creates a counter and registers it in an MXBean
	 * @param objectName The JMX ObjectName for the MXBean
	 * @param name The prefix for the MXBean attribute names
	 * @return the created counter
	 */
	public Counter mxCounter(final ObjectName objectName, final String name) {
		return mxCounter(objectName, name, null);
	}

	/**
	 * Creates a Timer and registers it in an MXBean
	 * @param objectName The JMX ObjectName for the MXBean
	 * @param name The prefix for the MXBean attribute names
	 * @param description An optional description 
	 * @return the created Timer
	 */
	public Timer mxTimer(final ObjectName objectName, final String name, String description) {
		final Timer metric = new Timer();
		installMXBean(objectName, name, description, metric);
		return metric;		
	}
	
	/**
	 * Creates a Timer and registers it in an MXBean
	 * @param objectName The JMX ObjectName for the MXBean
	 * @param name The prefix for the MXBean attribute names
	 * @return the created Timer
	 */
	public Timer mxTimer(final ObjectName objectName, final String name) {
		return mxTimer(objectName, name, null);
	}
	
	/**
	 * Creates a Histogram and registers it in an MXBean
	 * @param objectName The JMX ObjectName for the MXBean
	 * @param name The prefix for the MXBean attribute names
	 * @param description An optional description 
	 * @return the created Histogram
	 */
	public Histogram mxHistogram(final ObjectName objectName, final String name, String description) {
		final Histogram metric = new Histogram(new ExponentiallyDecayingReservoir());
		installMXBean(objectName, name, description, metric);
		return metric;		
	}
	
	/**
	 * Creates a Histogram and registers it in an MXBean
	 * @param objectName The JMX ObjectName for the MXBean
	 * @param name The prefix for the MXBean attribute names
	 * @return the created Histogram
	 */
	public Histogram mxHistogram(final ObjectName objectName, final String name) {
		return mxHistogram(objectName, name, null);
	}
	
	/**
	 * Creates a Meter and registers it in an MXBean
	 * @param objectName The JMX ObjectName for the MXBean
	 * @param name The prefix for the MXBean attribute names
	 * @param description An optional description 
	 * @return the created Meter
	 */
	public Meter mxMeter(final ObjectName objectName, final String name, String description) {
		final Meter metric = new Meter();
		installMXBean(objectName, name, description, metric);
		return metric;		
	}
	
	/**
	 * Creates a Meter and registers it in an MXBean
	 * @param objectName The JMX ObjectName for the MXBean
	 * @param name The prefix for the MXBean attribute names
	 * @return the created Meter
	 */
	public Meter mxMeter(final ObjectName objectName, final String name) {
		return mxMeter(objectName, name, null);
	}
	
	/**
	 * Creates a Gauge and registers it in an MXBean
	 * @param objectName The JMX ObjectName for the MXBean
	 * @param name The prefix for the MXBean attribute names
	 * @param description An optional description 
	 * @return the created Gauge
	 */
	public <T> Gauge<T> mxGauge(final Callable<T> provider, final ObjectName objectName, final String name, String description) {
		final Gauge<T> metric = new Gauge<T>(){
			@Override
			public T getValue() {
				try {
					return provider.call();
				} catch (Exception ex) {
					throw new RuntimeException(ex);
				}
			}
		};
		installMXBean(objectName, name, description, metric);
		return metric;		
	}
	
	/**
	 * Creates a Gauge and registers it in an MXBean
	 * @param objectName The JMX ObjectName for the MXBean
	 * @param name The prefix for the MXBean attribute names
	 * @return the created Gauge
	 */
	public <T> Gauge<T> Gauge(final Callable<T> provider, final ObjectName objectName, final String name) {
		return mxGauge(provider, objectName, name, null);
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see com.codahale.metrics.MetricRegistry#counter(java.lang.String)
	 */
	@Override
	public Counter counter(final String name) {
		return super.counter("counter." + name);
	}
	
		

	/**
	 * {@inheritDoc}
	 * @see com.codahale.metrics.MetricRegistry#timer(java.lang.String)
	 */
	@Override
	public Timer timer(final String name) {
		return super.timer("timer." + name);
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.codahale.metrics.MetricRegistry#histogram(java.lang.String)
	 */
	@Override
	public Histogram histogram(final String name) {
		return super.histogram("histogram." + name);
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.codahale.metrics.MetricRegistry#meter(java.lang.String)
	 */
	@Override
	public Meter meter(final String name) {
		return super.meter("meter." + name);
	}

	@Override
	public ObjectName createName(String type, String domain, String name) {
		// TODO Auto-generated method stub
		return null;
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
