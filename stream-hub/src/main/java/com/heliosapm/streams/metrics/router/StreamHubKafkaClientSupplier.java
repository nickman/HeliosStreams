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
package com.heliosapm.streams.metrics.router;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;

/**
 * <p>Title: StreamHubKafkaClientSupplier</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.router.StreamHubKafkaClientSupplier</code></p>
 */

public class StreamHubKafkaClientSupplier extends DefaultKafkaClientSupplier {
	/** The streams config */
	protected final StreamsConfig config;
	/** The client id */
	protected final String clientId;
	
	/**
	 * Creates a new StreamHubKafkaClientSupplier
	 * @param config the streams config
	 * @param clientId The client id
	 */
	public StreamHubKafkaClientSupplier(final StreamsConfig config, final String clientId) {
		this.config = config;
		this.clientId = clientId;
	}
	
	
    /**
     * Creates a producer using the common config and the specified serializers
     * @param keySerde The key serde
     * @param valueSerde The value serde
     * @return the producer
     * @param <K> type key type
     * @param <V> the value type
     */
    public <K,V> Producer<K, V> getProducer(final Serde<K> keySerde, final Serde<V> valueSerde) {
        return new KafkaProducer<K,V>(config.getProducerConfigs(clientId), keySerde.serializer(), valueSerde.serializer());
    }

	
	

}
