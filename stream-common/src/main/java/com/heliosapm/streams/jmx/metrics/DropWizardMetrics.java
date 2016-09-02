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

import javax.management.ImmutableDescriptor;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanConstructorInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanOperationInfo;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.ObjectName;
import javax.management.StandardEmitterMBean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Metric;
import com.heliosapm.utils.collections.FluentMap;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.jmx.SharedNotificationExecutor;
import com.heliosapm.utils.tuples.NVP;

/**
 * <p>Title: DropWizardMetrics</p>
 * <p>Description: MBean impl to combine multiple metric types under one name and expose as a registered managed object.</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.jmx.metrics.DropWizardMetrics</code></p>
 */

public class DropWizardMetrics extends StandardEmitterMBean {
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** This mbean's assigned ObjectName */
	protected final ObjectName objectName;
	/** A map of the metrics registered in this MBean */
	protected final Map<MetricType, Metric> metricMap = MetricType.metricMap();
	
	/** The type of notification emitted when the MBeanInfo changes */
	public static final String INFO_CHANGE = "jmx.mbean.info.changed";
	
	/** Array of supported notifications */
	protected static final MBeanNotificationInfo[] notifInfos = new MBeanNotificationInfo[]{
			new MBeanNotificationInfo(new String[]{INFO_CHANGE}, Notification.class.getName(), "Emitted when a DropWizardMetric MBean implements new meta-data")
	}; 
	
	/** We have no op infos yet */
	protected final MBeanOperationInfo[] opInfos = {};
	/** We have no ctor infos yet */
	protected final MBeanConstructorInfo[] ctorInfos = {};
	
	/** Notif serial factory */
	protected final AtomicInteger notifSerial = new AtomicInteger();
	
	/** A map of metric instances keyed by the attribute name */
	protected final Map<String, Metric> attrMetrics = new ConcurrentHashMap<String, Metric>();
	/** A map of MBeanAttributeInfos instances keyed by the attribute name */
	protected final Map<String, MBeanAttributeInfo> attrInfos = new ConcurrentHashMap<String, MBeanAttributeInfo>();
	
	
	/**
	 * Creates a new DropWizardMetrics
	 * @param objectName The object name to register this mbean with
	 */
	public DropWizardMetrics(final ObjectName objectName) {
		super(DropWizardMetricsMXBean.class, true, new NotificationBroadcasterSupport(SharedNotificationExecutor.getInstance(), notifInfos));
		this.objectName = objectName;
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
	
	public void add(final Metric metric) {
		final MetricType metricType = MetricType.metricType(metric);
		if(metricMap.containsKey(metricType)) throw new IllegalStateException("This mbean already contains a metric type of [" + metricType + "]");
		metricMap.put(metricType, metric);
		final NVP<Metric, MBeanAttributeInfo[]> metricAttrInfos = metricType.minfo(metric);
		for(MBeanAttributeInfo info: metricAttrInfos.getValue()) {
			attrInfos.put(info.getName(), info);
			attrMetrics.put(info.getName(), metric);			
		}
		final MBeanInfo minfo = getCachedMBeanInfo();
		cacheMBeanInfo(minfo);
		final Notification notif = new Notification(INFO_CHANGE, objectName, notifSerial.incrementAndGet(), System.currentTimeMillis());
		notif.setUserData(minfo);
		sendNotification(notif);
	}
	
	@Override
	protected MBeanInfo getCachedMBeanInfo() {		
		return new MBeanInfo(getClass().getName(), "DropWizard Metric Set", attrInfos.values().toArray(new MBeanAttributeInfo[0]), ctorInfos, opInfos, notifInfos, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
			.fput("infoTimeout", 10000L)
		));
	}

}
