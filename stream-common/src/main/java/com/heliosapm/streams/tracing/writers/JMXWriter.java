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
package com.heliosapm.streams.tracing.writers;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.cliffc.high_scale_lib.NonBlockingHashMap;

import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.tracing.AbstractMetricWriter;
import com.heliosapm.streams.tracing.TagKeySorter;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.tuples.NVP;

/**
 * <p>Title: JMXWriter</p>
 * <p>Description: Writes metrics to custom mbean metric containers registered in the local MBeanServer</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tracing.writers.JMXWriter</code></p>
 */

public class JMXWriter extends AbstractMetricWriter {
	/** The MBeanServer hosting the metrics */
	protected final MBeanServer server;
	
	/** A map of updating JMXStreamedMetrics keyed by their ObjectName */
	protected final NonBlockingHashMap<String, NVP<ObjectName, JMXStreamedMetric>> metricRegistry = new NonBlockingHashMap<String, NVP<ObjectName, JMXStreamedMetric>>(512); 

	/** A placeholder NVP */
	protected static final NVP<ObjectName, JMXStreamedMetric> PLACEHOLDER = new NVP<ObjectName, JMXStreamedMetric>(null, null);
	
	protected String serverJmxDomain = null;
	protected String jmxmpUri = null;
	
	/**
	 * Creates a new JMXWriter
	 */
	public JMXWriter() {
		super(false, true);
		// TODO:  these need config
		server = JMXHelper.createMBeanServer("helios-metrics", false);
		JMXHelper.fireUpJMXMPServer("0.0.0.0", 1422, server);
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.AbstractMetricWriter#configure(java.util.Properties)
	 */
	@Override
	public void configure(final Properties config) {
		
		super.configure(config);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.MetricWriterMXBean#getCustomState()
	 */
	@Override
	public String getCustomState() {
		return "OK";
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.AbstractMetricWriter#doMetrics(java.util.Collection)
	 */
	@Override
	protected void doMetrics(Collection<StreamedMetric> metrics) {
		if(metrics==null || metrics.isEmpty()) return;
		for(final StreamedMetric sm: metrics) {			
			try {
				getMetric(sm)
					.update(sm);
				sentMetrics.increment();
			} catch (Exception ex) {
				failedMetrics.increment();
			}
		}
	}
	
	
	protected JMXStreamedMetric getMetric(final StreamedMetric sm) {
		NVP<ObjectName, JMXStreamedMetric> nvp = metricRegistry.putIfAbsent(sm.metricKey(), PLACEHOLDER);		
		if(nvp==null || nvp==PLACEHOLDER) {
			log.debug("Creating JMXStreamedMetric for [{}]", sm.metricKey());
			final ObjectName on = smObjectName(sm);
			final JMXStreamedMetric metric = new JMXStreamedMetric(sm);
			nvp = new NVP<ObjectName, JMXStreamedMetric>(on, metric);
			metricRegistry.replace(sm.metricKey(), nvp);
			JMXHelper.registerMBean(server, on, metric);
		}
		return nvp.getValue();
	}
	
	protected ObjectName smObjectName(final StreamedMetric sm) {
		final Map<String, String> tags = new HashMap<String, String>(sm.getTags());
		final String mn = sm.getMetricName();
		final String domain = tags.remove("host");
		tags.put("metric", mn);		
		return JMXHelper.objectName(domain, tags, SORTER);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.AbstractMetricWriter#doMetrics(com.heliosapm.streams.metrics.StreamedMetric[])
	 */
	@Override
	protected void doMetrics(StreamedMetric... metrics) {
		doMetrics(Arrays.asList(metrics));
	}

	/**
	 * {@inheritDoc}
	 * @see com.google.common.util.concurrent.AbstractIdleService#startUp()
	 */
	@Override
	protected void startUp() throws Exception {
		/* No Op */
	}

	/**
	 * {@inheritDoc}
	 * @see com.google.common.util.concurrent.AbstractIdleService#shutDown()
	 */
	@Override
	protected void shutDown() throws Exception {
		/* No Op */
	}

	
	public static class MetricTagKeySorter implements Comparator<String> {

		public int compare(final String o1, final String o2) {
			if(o1.equals(o2)) return 0;
			if(o1.equalsIgnoreCase(o2)) return -1;
			final int d = doCompare(o1, o2);
			if(d!=Integer.MAX_VALUE) return d;
			return o1.toLowerCase().compareTo(o2.toLowerCase());
		}
		
		protected int doCompare(final String var1, final String var2) {
			if("metric".equalsIgnoreCase(var1)) return 1;
			else if("metric".equalsIgnoreCase(var2)) return -1;
	        return "app".equalsIgnoreCase(var1)
	        		? -1: ("app".equalsIgnoreCase(var2)
	        			? 1: ("host".equalsIgnoreCase(var1)
	        		?-1:("host".equalsIgnoreCase(var2)
	        			? 1: 2147483647)));
	    }
	}
	public static final MetricTagKeySorter SORTER = new MetricTagKeySorter(); 
}
