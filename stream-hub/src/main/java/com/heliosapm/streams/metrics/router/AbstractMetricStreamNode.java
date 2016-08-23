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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.springframework.beans.factory.BeanNameAware;
import org.springframework.jmx.export.annotation.ManagedMetric;
import org.springframework.jmx.export.naming.SelfNaming;
import org.springframework.jmx.support.MetricType;

import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.jmx.annotations.ManagedResource;

import jsr166e.LongAdder;

/**
 * <p>Title: AbstractMetricStreamNode</p>
 * <p>Description: An abstract MetricStreamNode for extension. Supplies some spring boilder plate</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.router.AbstractMetricStreamNode</code></p>
 */
@ManagedResource
public abstract  class AbstractMetricStreamNode implements MetricStreamNode, BeanNameAware, SelfNaming {
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


	/**
	 * {@inheritDoc}
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() throws IOException {
		/* No Op */
	}


	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.router.MetricStreamNode#getName()
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
	

}
