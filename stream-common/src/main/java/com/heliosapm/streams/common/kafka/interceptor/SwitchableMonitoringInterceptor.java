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
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * <p>Title: SwitchableMonitoringInterceptor</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.common.kafka.interceptor.SwitchableMonitoringInterceptor</code></p>
 * @param <K> The message key type
 * @param <V> The message value type
 */

public class SwitchableMonitoringInterceptor<K,V>  implements ConsumerInterceptor<K, V>, ProducerInterceptor<K,V> {
	protected MonitoringConsumerInterceptor<K, V> consumer = null;
	protected MonitoringProducerInterceptor<K, V> producer = null;

	/**
	 * Creates a new SwitchableMonitoringInterceptor
	 */
	public SwitchableMonitoringInterceptor() {
	}
	
	@Override
	public void configure(final Map<String, ?> configs) {
		if(configs.containsKey(ProducerConfig.LINGER_MS_CONFIG)) {
			producer = new MonitoringProducerInterceptor<K, V>();
			producer.configure(configs);			
		} else {			
			consumer = new MonitoringConsumerInterceptor<K,V>();
			consumer.configure(configs);
		}		
	}

	@Override
	public ProducerRecord<K, V> onSend(final ProducerRecord<K, V> record) {
		return producer.onSend(record);
	}

	@Override
	public void onAcknowledgement(final RecordMetadata metadata, final Exception exception) {
		producer.onAcknowledgement(metadata, exception);		
	}

	@Override
	public ConsumerRecords<K, V> onConsume(final ConsumerRecords<K, V> records) {
		return consumer.onConsume(records);
	}

	@Override
	public void onCommit(final Map<TopicPartition, OffsetAndMetadata> offsets) {
		consumer.onCommit(offsets);
	}

	@Override
	public void close() {
		if(consumer!=null) try { consumer.close(); } catch (Exception x) {/* No Op */}
		if(producer!=null) try { producer.close(); } catch (Exception x) {/* No Op */}
	}
	
	

}
