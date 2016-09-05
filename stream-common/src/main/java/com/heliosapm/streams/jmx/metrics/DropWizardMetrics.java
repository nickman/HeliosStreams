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
package com.heliosapm.streams.jmx.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.ImmutableDescriptor;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanConstructorInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanOperationInfo;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.StandardEmitterMBean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;
import com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter;
import com.heliosapm.streams.jmx.metrics.MetricType.SnapshotMember;
import com.heliosapm.utils.collections.FluentMap;
import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.jmx.SharedNotificationExecutor;
import com.heliosapm.utils.tuples.NVP;

/**
 * <p>Title: DropWizardMetrics</p>
 * <p>Description: MBean impl to combine multiple metric types under one name and expose as a registered managed object.</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.jmx.metrics.DropWizardMetrics</code></p>
 */

public class DropWizardMetrics extends StandardEmitterMBean implements DropWizardMetricsMXBean {
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** This mbean's assigned ObjectName */
	protected final ObjectName objectName;
	/** The description for this mbean */
	protected final String description;
	/** We have no op infos yet */
	protected final MBeanOperationInfo[] opInfos = {};
	/** We have no ctor infos yet */
	protected final MBeanConstructorInfo[] ctorInfos = {};
	
	/** Notif serial factory */
	protected final AtomicInteger notifSerial = new AtomicInteger();
	
	/** A map of metric instances keyed by the attribute name */
	protected final ConcurrentHashMap<String, Metric> attrMetrics = new ConcurrentHashMap<String, Metric>();
	/** A map of MBeanAttributeInfos instances keyed by the attribute name */
	protected final Map<String, MBeanAttributeInfo> attrInfos = new ConcurrentHashMap<String, MBeanAttributeInfo>();
	/** A map of AttributeAdapters keyed by the attribute name */
	protected final Map<String, AttributeAdapter> attrAdapters = new ConcurrentHashMap<String, AttributeAdapter>();
	
	/** The type of notification emitted when the MBeanInfo changes */
	public static final String INFO_CHANGE = "jmx.mbean.info.changed";
	
	/** Array of supported notifications */
	protected static final MBeanNotificationInfo[] notifInfos = new MBeanNotificationInfo[]{
			new MBeanNotificationInfo(new String[]{INFO_CHANGE}, Notification.class.getName(), "Emitted when a DropWizardMetric MBean implements new meta-data")
	}; 
	
	 
	
	/**
	 * Creates a new DropWizardMetrics
	 * @param objectName The object name to register this mbean with
	 * @param description A description for the metric set to be exposed by this mbean
	 */
	public DropWizardMetrics(final ObjectName objectName, final String description) {
		super(DropWizardMetricsMXBean.class, true, new NotificationBroadcasterSupport(SharedNotificationExecutor.getInstance(), notifInfos));
		if(objectName==null) throw new IllegalArgumentException("The passed object name was null");
		this.objectName = objectName;
		this.description = (description==null || description.trim().isEmpty()) ? "DropWizard Metric Set" : description.trim();		
	}

	/**
	 * Indicates if the passed metric is managed by this mbean
	 * @param metric the metric to test for 
	 * @return true if the passed metric is managed by this mbean, false otherwise
	 */
	public boolean hasMetric(final Metric metric) {
		return attrMetrics.containsValue(metric);
	}
	
	
	/**
	 * [Re-]Publishes this MBean
	 */
	public void publish() {
		if(JMXHelper.isRegistered(objectName)) {
			try { JMXHelper.unregisterMBean(objectName); } catch (Exception x) {/* No Op */}
		}
		try { JMXHelper.registerMBean(objectName, this); } catch (Exception x) {/* No Op */}
	}
	
	public void addMetricSet(final MetricSet metric) {
		for(Map.Entry<String, Metric> entry : metric.getMetrics().entrySet()) {
			addMetric(entry.getValue(), entry.getKey(), null);
		}
	}
	
	public void addMetric(final Metric metric, final String name, final String description) {
		final MetricType metricType = MetricType.metricType(metric);
		NVP<MBeanAttributeInfo[], AttributeAdapter[]> nvp = MetricType.attrInfos(metricType.aa, name, description, metric);
		MBeanAttributeInfo[] attributeInfos = nvp.getKey();
		AttributeAdapter[] adapters = nvp.getValue();
		for(int i = 0; i < attributeInfos.length; i++) {
			if(!attrInfos.containsKey(attributeInfos[i].getName())) {
				attrInfos.put(attributeInfos[i].getName(), attributeInfos[i]);
				attrMetrics.put(attributeInfos[i].getName(), metric);
				attrAdapters.put(attributeInfos[i].getName(), adapters[i]);
			}
		}
		if(metricType.aa.isSampling()) {
			nvp = MetricType.attrInfos(SnapshotMember.MAX, name, description, metric);
			attributeInfos = nvp.getKey();
			adapters = nvp.getValue();
			for(int i = 0; i < attributeInfos.length; i++) {
				if(!attrInfos.containsKey(attributeInfos[i].getName())) {
					attrInfos.put(attributeInfos[i].getName(), attributeInfos[i]);
					attrMetrics.put(attributeInfos[i].getName(), metric);
					attrAdapters.put(attributeInfos[i].getName(), adapters[i]);
				}
			}			
		}
		final MBeanInfo minfo = getCachedMBeanInfo();
		cacheMBeanInfo(minfo);
		final Notification notif = new Notification(INFO_CHANGE, objectName, notifSerial.incrementAndGet(), System.currentTimeMillis());
		notif.setUserData(minfo);
		sendNotification(notif);		
	}
	
	/**
	 * {@inheritDoc}
	 * @see javax.management.StandardMBean#getAttribute(java.lang.String)
	 */
	@Override
	public Object getAttribute(final String attribute) throws AttributeNotFoundException, MBeanException, ReflectionException {
		if(attribute==null || attribute.trim().isEmpty()) throw new IllegalArgumentException("Attribute Name was null or empty");
		final String attr = attribute.trim();
		final Metric metric = attrMetrics.get(attr);
		final AttributeAdapter aa = attrAdapters.get(attr);
		if(metric==null || aa==null) throw new AttributeNotFoundException("Metric Attribute [" + attr + "] not found");
		return aa.invoke(metric);
	}
	
	/**
	 * {@inheritDoc}
	 * @see javax.management.StandardMBean#getAttributes(java.lang.String[])
	 */
	@Override
	public AttributeList getAttributes(final String[] attributes) {
		final AttributeList attrList = new AttributeList();
		if(attributes==null || attributes.length==0) {
			return attrList;
		}
		for(String attrName : attributes) {
			if(attrName==null || attrName.trim().isEmpty()) continue;
			try {
				final Object obj = getAttribute(attrName.trim());
				attrList.add(new Attribute(attrName.trim(), obj));
			} catch (Exception x) {/* No Op */}
		}
		return super.getAttributes(attributes);
	}
	
	@Override
	protected MBeanInfo getCachedMBeanInfo() {		
		return new MBeanInfo(getClass().getName(), description, attrInfos.values().toArray(new MBeanAttributeInfo[0]), ctorInfos, opInfos, notifInfos, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
			.fput("infoTimeout", 10000L)
		));
	}

	public static void main(String[] args) {
		JMXHelper.fireUpJMXMPServer(2259);
		final DropWizardMetrics dwm = new DropWizardMetrics(JMXHelper.objectName("dw:service=Metrics"), "Metric Test");
		JMXHelper.registerMBean(dwm, dwm.objectName);
		dwm.addMetric(new Meter(), "MessagesPerS", "Kafka Messages Per Second");
		StdInCommandHandler.getInstance()
			.registerCommand("next", new Runnable(){
				public void run() {
					dwm.addMetric(new Timer(), "Elapsed", "Kafka Messages Processing");
				}
			})
			.run();
	}
	
}
