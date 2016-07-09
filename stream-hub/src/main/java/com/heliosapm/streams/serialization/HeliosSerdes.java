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
package com.heliosapm.streams.serialization;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.StreamedMetricDeserializer;
import com.heliosapm.streams.metrics.StreamedMetricSerializer;
import com.heliosapm.streams.metrics.StreamedMetricValue;
import com.heliosapm.streams.metrics.StreamedMetricValueDeserializer;
import com.heliosapm.streams.metrics.StreamedMetricValueSerializer;
import com.heliosapm.streams.metrics.processors.TimestampedMetricKey;
import com.heliosapm.streams.metrics.processors.TimestampedMetricKey.TimestampedMetricKeyDeserializer;
import com.heliosapm.streams.metrics.processors.TimestampedMetricKey.TimestampedMetricKeySerializer;

/**
 * <p>Title: HeliosSerdes</p>
 * <p>Description: Serde definitions</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.serialization.HeliosSerdes</code></p>
 */

public class HeliosSerdes extends Serdes {

	/** The {@link StreamedMetric} serializer */
	public static final Serializer<StreamedMetric> STREAMED_METRIC_SER = new StreamedMetricSerializer();
	/** The {@link StreamedMetric} deserializer */
	public static final Deserializer<StreamedMetric> STREAMED_METRIC_DESER = new StreamedMetricDeserializer();
	/** The {@link StreamedMetricValue} serializer */
	public static final Serializer<StreamedMetricValue> STREAMED_METRIC_VALUE_SER = new StreamedMetricValueSerializer();
	/** The {@link StreamedMetricValue} deserializer */
	public static final Deserializer<StreamedMetricValue> STREAMED_METRIC_VALUE_DESER = new StreamedMetricValueDeserializer();
	/** The {@link TimestampedMetricKey} serializer */
	public static final Serializer<TimestampedMetricKey> TIMESTAMPED_METRIC_SER = new TimestampedMetricKeySerializer(); 	
	/** The {@link TimestampedMetricKey} deserializer */
	public static final Deserializer<TimestampedMetricKey> TIMESTAMPED_METRIC_DESER = new TimestampedMetricKeyDeserializer(); 
	

	/** The {@link StreamedMetric} deserializer from a String value */
	public static final Deserializer<StreamedMetric> STREAMED_METRIC_DESER_FROM_STRING = new StatelessDeserializer<StreamedMetric>() {
		final Deserializer<String> stringDeser = String().deserializer();
		@Override
		public StreamedMetric deserialize(final java.lang.String topic, final byte[] data) {
			return StreamedMetric.fromString(stringDeser.deserialize(topic, data));
		}
	};
	
	/** The {@link StreamedMetric} serializer to a String value */
	public static final Serializer<StreamedMetric> STREAMED_METRIC_SER_TO_STRING = new StatelessSerializer<StreamedMetric>() {
		final Serializer<String> stringSer = String().serializer();
		@Override
		public byte[] serialize(final String topic, final StreamedMetric data) {			
			return stringSer.serialize(topic, data.toString());
		}		
	};
	
	
	
	/** The {@link StreamedMetric} Serde */
	public static final Serde<StreamedMetric> STREAMED_METRIC_SERDE = new StatelessSerde<StreamedMetric>(STREAMED_METRIC_SER, STREAMED_METRIC_DESER);
	/** The {@link StreamedMetricValue} Serde */
	public static final Serde<StreamedMetricValue> STREAMED_METRIC_VALUE_SERDE = new StatelessSerde<StreamedMetricValue>(STREAMED_METRIC_VALUE_SER, STREAMED_METRIC_VALUE_DESER);
	/** The {@link TimestampedMetricKey} Serde */
	public static final Serde<TimestampedMetricKey> TIMESTAMPED_METRIC_SERDE = new StatelessSerde<TimestampedMetricKey>(TIMESTAMPED_METRIC_SER, TIMESTAMPED_METRIC_DESER);
	/** The {@link StreamedMetric} Serde with a String as an intermediary */
	public static final Serde<StreamedMetric> STREAMED_METRIC_SERDE_THROUGH_STRING = new StatelessSerde<StreamedMetric>(STREAMED_METRIC_SER_TO_STRING, STREAMED_METRIC_DESER_FROM_STRING);
	
	
	/** The {@link String} Serde */
	public static final Serde<String> STRING_SERDE = String();
	/** The {@link Long} Serde */
	public static final Serde<Long> LONG_SERDE = Long();
	/** The {@link Integer} Serde */
	public static final Serde<Integer> INTEGER_SERDE = Integer();
	/** The {@link Double} Serde */
	public static final Serde<Double> DOUBLE_SERDE = Double();
	
	
	
	private HeliosSerdes() {}

}
