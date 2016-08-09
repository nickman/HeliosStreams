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
package com.heliosapm.streams.metrics;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * <p>Title: StreamedMetricSerde</p>
 * <p>Description: Kafka Serde for streamed metrics</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.StreamedMetricSerde</code></p>
 */

public class StreamedMetricSerde implements Serde<StreamedMetric> {
	/** Sharable Serde instance */
	public static final Serde<StreamedMetric> INSTANCE = new StreamedMetricSerde(); 
	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.common.serialization.Serde#deserializer()
	 */
	@Override
	public Deserializer<StreamedMetric> deserializer() {			
		return new StreamedMetricDeserializer();
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.common.serialization.Serde#serializer()
	 */
	@Override
	public Serializer<StreamedMetric> serializer() {			
		return new StreamedMetricSerializer();
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.common.serialization.Serde#configure(java.util.Map, boolean)
	 */
	@Override
	public void configure(final Map<String, ?> configs, final boolean isKey) {
		/* No Op */
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.common.serialization.Serde#close()
	 */
	@Override
	public void close() {
		/* No Op */			
	}

}
