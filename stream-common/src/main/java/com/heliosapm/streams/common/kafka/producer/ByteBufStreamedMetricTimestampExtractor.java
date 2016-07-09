/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.heliosapm.streams.common.kafka.producer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import com.heliosapm.streams.metrics.StreamedMetricValue;

import io.netty.buffer.ByteBuf;

/**
 * <p>Title: ByteBufStreamedMetricTimestampExtractor</p>
 * <p>Description: A {@link TimestampExtractor} to extract the timestamp from a {@link StreamedMetric} serialized into a ByteBuf </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.common.kafka.producer.ByteBufStreamedMetricTimestampExtractor</code></p>
 */

public class ByteBufStreamedMetricTimestampExtractor implements TimestampExtractor {

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.processor.TimestampExtractor#extract(org.apache.kafka.clients.consumer.ConsumerRecord)
	 */
	@Override
	public long extract(ConsumerRecord<Object, Object> record) {
		try {
			return StreamedMetricValue.timestamp((ByteBuf)record.value());
		} catch (Exception ex) {
			return System.currentTimeMillis();
		}
	}

}
