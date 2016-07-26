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
package com.heliosapm.streams.metrics.aggregation;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * <p>Title: StreamedMetricAggregationSerde</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.aggregation.StreamedMetricAggregationSerde</code></p>
 */

public class StreamedMetricAggregationSerde implements Serde<StreamedMetricAggregation> {
	/** A sharable SMASerializer instance */
	public static final Serializer<StreamedMetricAggregation> SER = new SMASerializer();
	/** A sharable SMADeserializer instance */
	public static final Deserializer<StreamedMetricAggregation> DESER = new SMADeserializer();
	
	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.common.serialization.Serde#serializer()
	 */
	@Override
	public Serializer<StreamedMetricAggregation> serializer() {
		return SER;
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.common.serialization.Serde#deserializer()
	 */
	@Override
	public Deserializer<StreamedMetricAggregation> deserializer() {
		return DESER;
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


	/**
	 * <p>Title: SMASerializer</p>
	 * <p>Description: Kafka serializer for {@link StreamedMetricAggregation} instances</p> 
	 * <p>Company: Helios Development Group LLC</p>
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>com.heliosapm.streams.metrics.aggregation.StreamedMetricAggregationSerde.SMASerializer</code></p>
	 */
	public static class SMASerializer implements Serializer<StreamedMetricAggregation> {

		/**
		 * {@inheritDoc}
		 * @see org.apache.kafka.common.serialization.Serializer#serialize(java.lang.String, java.lang.Object)
		 */
		@Override
		public byte[] serialize(final String topic, final StreamedMetricAggregation data) {
			return data.toByteArray();
		}

		/**
		 * {@inheritDoc}
		 * @see org.apache.kafka.common.serialization.Serializer#configure(java.util.Map, boolean)
		 */
		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {
			/* No Op */
		}


		/**
		 * {@inheritDoc}
		 * @see org.apache.kafka.common.serialization.Serializer#close()
		 */
		@Override
		public void close() {
			/* No Op */			
		}
		
	}
	
	/**
	 * <p>Title: SMADeserializer</p>
	 * <p>Description: Kafka deserializer for {@link StreamedMetricAggregation} instances</p> 
	 * <p>Company: Helios Development Group LLC</p>
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>com.heliosapm.streams.metrics.aggregation.StreamedMetricAggregationSerde.SMADeserializer</code></p>
	 */
	public static class SMADeserializer implements Deserializer<StreamedMetricAggregation> {
		
		/**
		 * {@inheritDoc}
		 * @see org.apache.kafka.common.serialization.Deserializer#deserialize(java.lang.String, byte[])
		 */
		@Override
		public StreamedMetricAggregation deserialize(final String topic, final byte[] data) {
			return StreamedMetricAggregation.fromBytes(data);
		}

		/**
		 * {@inheritDoc}
		 * @see org.apache.kafka.common.serialization.Deserializer#configure(java.util.Map, boolean)
		 */
		@Override
		public void configure(final Map<String, ?> configs, final boolean isKey) {
			/* No Op */
		}


		/**
		 * {@inheritDoc}
		 * @see org.apache.kafka.common.serialization.Deserializer#close()
		 */
		@Override
		public void close() {
			/* No Op */			
		}
		
	}
	
}
