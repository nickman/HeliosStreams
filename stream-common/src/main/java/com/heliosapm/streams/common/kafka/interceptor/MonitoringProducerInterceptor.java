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

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * <p>Title: MonitoringProducerInterceptor</p>
 * <p>Description: A monitoring interceptor for kafka producer</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.common.kafka.interceptor.MonitoringProducerInterceptor</code></p>
 */

public class MonitoringProducerInterceptor<K, V> extends MonitoringInterceptorBase<K, V> implements ProducerInterceptor<K, V> {

	/**
	 * Creates a new MonitoringProducerInterceptor
	 */
	public MonitoringProducerInterceptor() {
		super(true);

	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.clients.producer.ProducerInterceptor#onSend(org.apache.kafka.clients.producer.ProducerRecord)
	 */
	@Override
	public ProducerRecord<K, V> onSend(final ProducerRecord<K, V> record) {
		totalMeter.mark();
		meter(record.topic(), record.partition()).mark();
		return record;
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.clients.producer.ProducerInterceptor#onAcknowledgement(org.apache.kafka.clients.producer.RecordMetadata, java.lang.Exception)
	 */
	@Override	
	public void onAcknowledgement(final RecordMetadata metadata, final Exception exception) {
		if(metadata!=null) {
			final int total = metadata.serializedKeySize() + metadata.serializedValueSize();
			totalHistogram.update(total);
			histogram(metadata.topic(), metadata.partition()).update(total);
		}
	}
	


}
