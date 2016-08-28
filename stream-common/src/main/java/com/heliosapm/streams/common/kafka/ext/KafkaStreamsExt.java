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
package com.heliosapm.streams.common.kafka.ext;

import java.util.Properties;

import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

/**
 * <p>Title: KafkaStreamsExt</p>
 * <p>Description: An extended {@link KafkaStreams} to support native java streaming in and out of the Kafka-Streams engine</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.common.kafka.ext.KafkaStreamsExt</code></p>
 */

public class KafkaStreamsExt extends KafkaStreams {


	/**
	 * Creates a new KafkaStreamsExt
	 * @param builder The stream topology builder
	 * @param config The streams configuration
	 * @param clientSupplier the kafka consumer and producer client provider
	 */
	public KafkaStreamsExt(final TopologyBuilder builder, final StreamsConfig config, final KafkaClientSupplier clientSupplier) {
		super(builder, config, clientSupplier);
	}

	
	

}
