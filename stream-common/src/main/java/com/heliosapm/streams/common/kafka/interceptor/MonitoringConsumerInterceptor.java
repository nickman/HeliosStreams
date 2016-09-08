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

/**
 * <p>Title: MonitoringConsumerInterceptor</p>
 * <p>Description: A monitoring interceptor for kafka consumers</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.common.kafka.interceptor.MonitoringConsumerInterceptor</code></p>
 * @param <K> The message key type
 * @param <V> The message value type
 */

public class MonitoringConsumerInterceptor<K, V> extends MonitoringInterceptorBase<K, V> implements ConsumerInterceptor<K, V> {
	
	/**
	 * Creates a new MonitoringConsumerInterceptor
	 */
	public MonitoringConsumerInterceptor() {
		super(false);
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
			final int size = r.serializedKeySize() + r.serializedValueSize();
			histogram(r.topic(), r.partition()).update(size);
			counter(r.topic(), r.partition()).inc(size);
		}
		return records;
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
	

}
