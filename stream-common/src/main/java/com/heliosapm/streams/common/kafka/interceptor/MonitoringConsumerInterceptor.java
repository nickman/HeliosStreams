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

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;

import com.codahale.metrics.Meter;
import com.heliosapm.streams.common.metrics.SharedMetricsRegistry;
import com.heliosapm.streams.common.naming.AgentName;

/**
 * <p>Title: MonitoringConsumerInterceptor</p>
 * <p>Description: A monitoring interceptor for kafka consumers</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.common.kafka.interceptor.MonitoringConsumerInterceptor</code></p>
 */

public class MonitoringConsumerInterceptor<K, V> implements ConsumerInterceptor<K, V> {
	/** The app name we're monitoring in */
	protected final String appName;
	/** The host name the app is running on */
	protected final String hostName;
	/** The service tags for shared metrics */
	protected String serviceTag;
	/** The group id for the consumer */
	protected String groupId = "";
	/** The client id for the consumer */
	protected String clientId = "";
	
	protected String commitKey = null;
	
	/** The metrics registry */
	protected final SharedMetricsRegistry mr = SharedMetricsRegistry.getInstance();
	
	/** A map of counters keyed by the topic name + partition id */
	protected final NonBlockingHashMap<String, NonBlockingHashMapLong<Meter>> meters = new NonBlockingHashMap<String, NonBlockingHashMapLong<Meter>>(16); 
	
	protected final Meter totalMeter;

	/**
	 * Creates a new MonitoringConsumerInterceptor
	 */
	public MonitoringConsumerInterceptor() {
		appName = AgentName.appName();
		hostName = AgentName.hostName();
		totalMeter = SharedMetricsRegistry.getInstance().meter("messages.consumed.service=KafkaConsumer" + ".host=" + hostName + ".app=" + appName);
	}
	
	protected Meter meter(final String topicName, final int partitionId) {
		return meters.getOrDefault(topicName, new NonBlockingHashMapLong<Meter>(16, false))
				.getOrDefault(partitionId, SharedMetricsRegistry.getInstance().meter(String.format(serviceTag, topicName, partitionId, groupId, clientId)));		
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.clients.consumer.ConsumerInterceptor#onConsume(org.apache.kafka.clients.consumer.ConsumerRecords)
	 */
	@Override
	public ConsumerRecords<K, V> onConsume(final ConsumerRecords<K, V> records) {
		totalMeter.mark(records.count());
		for(ConsumerRecord<K, V> r : records) {
			meter(r.topic(), r.partition()).mark();
		}
		return records;
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.clients.consumer.ConsumerInterceptor#close()
	 */
	@Override
	public void close() {
		meters.clear();

	}
	

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.clients.consumer.ConsumerInterceptor#onCommit(java.util.Map)
	 */
	@Override
	public void onCommit(final Map<TopicPartition, OffsetAndMetadata> offsets) {
		for(Map.Entry<TopicPartition, OffsetAndMetadata> entry: offsets.entrySet()) {
			final String newMeta = commitKey + " : " + System.currentTimeMillis();
			entry.setValue(new OffsetAndMetadata(entry.getValue().offset(), newMeta));
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.common.Configurable#configure(java.util.Map)
	 */
	@Override
	public void configure(final Map<String, ?> configs) {
		groupId = configs.get("group.id").toString();
		clientId = configs.get("client.id").toString();
		final StringBuilder serviceBuilder = new StringBuilder("messages.consumed.service=KafkaConsumer" + ".host=" + hostName + ".app=" + appName + ".topic=%s.partition=%s");
		final StringBuilder commitKeyBuilder = new StringBuilder();
		if(clientId!=null && !clientId.trim().isEmpty()) {
			serviceBuilder.append(".client=%s");
			clientId = clientId.trim();
			commitKeyBuilder.append(clientId);
		} else {
			serviceBuilder.append("%s");
			clientId = "";
		}
		if(groupId!=null && !groupId.trim().isEmpty()) {
			serviceBuilder.append(".group=%s");
			groupId = groupId.trim();
			if(commitKeyBuilder.length()>0) commitKeyBuilder.append("."); 
			commitKeyBuilder.append(groupId);
		} else {
			serviceBuilder.append("%s");
			groupId = "";
		}
		commitKey = commitKeyBuilder.append("@").append(appName).append(".").append(hostName).toString();
		serviceTag = serviceBuilder.toString();		
	}
	


}
