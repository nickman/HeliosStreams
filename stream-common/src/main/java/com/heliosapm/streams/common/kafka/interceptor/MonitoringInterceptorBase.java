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
package com.heliosapm.streams.common.kafka.interceptor;

import java.util.Map;
import java.util.concurrent.Callable;

import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;

import com.codahale.metrics.Meter;
import com.heliosapm.streams.common.metrics.SharedMetricsRegistry;
import com.heliosapm.streams.common.naming.AgentName;

/**
 * <p>Title: InterceptorBase</p>
 * <p>Description: The base class for the monitor interceptors</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.common.kafka.interceptor.InterceptorBase</code></p>
 */

public abstract class MonitoringInterceptorBase<K, V> {
	/** The app name we're monitoring in */
	protected final String appName;
	/** The host name the app is running on */
	protected final String hostName;
	/** The service tags for shared metrics */
	protected String serviceTag;
	/** The service tags for shared metrics for producers with no partition */
	protected String noPartitionServiceTag;
	
	/** The group id for the consumer */
	protected String groupId = "";
	/** The client id for the consumer */
	protected String clientId = "";
	
	protected String commitKey = null;
	
	/** The metrics registry */
	protected final SharedMetricsRegistry mr = SharedMetricsRegistry.getInstance();
	
	/** A map of counters keyed by the topic name + partition id */
	protected final NonBlockingHashMap<String, NonBlockingHashMapLong<Meter>> meters = new NonBlockingHashMap<String, NonBlockingHashMapLong<Meter>>(16); 
	
	/** The total meter */
	protected final Meter totalMeter;
	/** True if this is a producer interceptor, false if a consumer */
	protected final boolean producer;
	/** The verb for this interceptor */
	protected final String verb;
	/** The noun for this interceptor */
	protected final String noun;

	/**
	 * Creates a new MonitoringInterceptorBase
	 * @param producer true for a producer, false for a consumer
	 */
	protected MonitoringInterceptorBase(final boolean producer) {
		this.producer = producer;
		verb = producer ? "produced" : "consumed";
		noun = producer ? "Producer" : "Consumer";
		appName = AgentName.getInstance().getAppName();
		hostName = AgentName.getInstance().getHostName();
		totalMeter = SharedMetricsRegistry.getInstance().meter("messages." + verb + ".service=Kafka" + noun + "" + ".host=" + hostName + ".app=" + appName);
	}

	/**
	 * Acquires a meter for the passed topic name and partition id
	 * @param topicName The topic name
	 * @param partitionId The partition id
	 * @return the assigned meter
	 */
	protected Meter meter(final String topicName, final Integer partitionId) {
		final long pId = partitionId==null ? -1L : partitionId;
		return meters.get(topicName, new Callable<NonBlockingHashMapLong<Meter>>() { @Override
		public NonBlockingHashMapLong<Meter> call() { return new NonBlockingHashMapLong<Meter>(16, false); }})
				.get(pId, new Callable<Meter>() { @Override
				public Meter call() {return SharedMetricsRegistry.getInstance().meter(pId==-1L ? String.format(noPartitionServiceTag, topicName, groupId, clientId) : String.format(serviceTag, topicName, partitionId, groupId, clientId)); }} );		
	}
	
	/**
	 * Closes this interceptor 
	 */
	public void close() {
		meters.clear();
	}
	
	/**
	 * Configures this interceptor
	 * @param configs The config properties
	 */
	public void configure(final Map<String, ?> configs) {
		final Object g =  configs.get("group.id");
		final Object c =  configs.get("client.id");
		groupId = g==null ? null : g.toString().trim();
		clientId = g==null ? null : c.toString().trim();
		final StringBuilder serviceBuilder = new StringBuilder("messages." + verb + ".service=Kafka" + noun + "" + ".host=" + hostName + ".app=" + appName + ".topic=%s.partition=%s");
		final StringBuilder noPartServiceBuilder = new StringBuilder("messages." + verb + ".service=Kafka" + noun + "" + ".host=" + hostName + ".app=" + appName + ".topic=%s");
		final StringBuilder commitKeyBuilder = new StringBuilder();
		if(clientId!=null && !clientId.trim().isEmpty()) {
			serviceBuilder.append(".client=%s");
			noPartServiceBuilder.append(".client=%s");
			clientId = clientId.trim();
			commitKeyBuilder.append(clientId);
		} else {
			serviceBuilder.append("%s");
			noPartServiceBuilder.append("%s");
			clientId = "";
		}
		if(groupId!=null && !groupId.trim().isEmpty()) {
			serviceBuilder.append(".group=%s");
			noPartServiceBuilder.append(".group=%s");
			groupId = groupId.trim();
			if(commitKeyBuilder.length()>0) commitKeyBuilder.append("."); 
			commitKeyBuilder.append(groupId);
		} else {
			serviceBuilder.append("%s");
			noPartServiceBuilder.append("%s");
			groupId = "";
		}
		commitKey = commitKeyBuilder.append("@").append(appName).append(".").append(hostName).toString();
		serviceTag = serviceBuilder.toString();		
		noPartitionServiceTag = noPartServiceBuilder.toString();
	}


	
	
}
