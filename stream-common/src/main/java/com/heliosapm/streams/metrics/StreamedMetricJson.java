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
package com.heliosapm.streams.metrics;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.heliosapm.streams.json.JSONOps;

/**
 * <p>Title: StreamedMetricJson</p>
 * <p>Description: Encapsulation of streamed metrics json serializer and deserializers</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.StreamedMetricJson</code></p>
 */

public class StreamedMetricJson {
	/** Sharable instance of the {@link StreamedMetric} JSON serializer */
	public static final StreamedMetricJsonSerializer SM_SERIALIZER = new StreamedMetricJsonSerializer();
	/** Sharable instance of the {@link StreamedMetricValue} JSON serializer */
	public static final StreamedMetricValueJsonSerializer SMV_SERIALIZER = new StreamedMetricValueJsonSerializer();
	/** Sharable instance of the {@link StreamedMetric} JSON dserializer */
	public static final StreamedMetricJsonDeserializer SM_DESERIALIZER = new StreamedMetricJsonDeserializer();
	/** Sharable instance of the {@link StreamedMetricValue} JSON dserializer */
	public static final StreamedMetricValueJsonDeserializer SMV_DESERIALIZER = new StreamedMetricValueJsonDeserializer();
	
	private static TypeReference<HashMap<String, String>> TR_TAGS = new TypeReference<HashMap<String, String>>() {};

	
	/**
	 * <p>Title: StreamedMetricJsonSerializer</p>
	 * <p>Description: A JSON serializer for StreamedMetrics</p> 
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>com.heliosapm.streams.metrics.StreamedMetricJson.StreamedMetricJsonSerializer</code></p>
	 */
	public static class StreamedMetricJsonSerializer extends JsonSerializer<StreamedMetric> {
		/**
		 * {@inheritDoc}
		 * @see com.fasterxml.jackson.databind.JsonSerializer#serialize(java.lang.Object, com.fasterxml.jackson.core.JsonGenerator, com.fasterxml.jackson.databind.SerializerProvider)
		 */
		@Override
		public void serialize(final StreamedMetric value, final JsonGenerator j, final SerializerProvider serializers) throws IOException, JsonProcessingException {
			j.writeStartObject();
			final ValueType v =  value.getValueType();
			if(v!=null) {
				j.writeStringField("v", v.name);
			}
			j.writeStringField("metric", value.getMetricName());
			j.writeNumberField("timestamp", value.getTimestamp());
			if(value.isValued()) {
				final StreamedMetricValue smv = value.forValue();
				if(smv.isDoubleValue()) {
					j.writeNumberField("value", smv.getDoubleValue());
				} else {
					j.writeNumberField("value", smv.getLongValue());
				}
			}
			j.writeFieldName("tags");
			j.writeStartObject();
			for(final Map.Entry<String, String> tag : value.getTags().entrySet()) {
				j.writeStringField(tag.getKey(), tag.getValue());
			}
			j.writeEndObject();
			j.writeEndObject();
		}
	}
	
	/**
	 * <p>Title: StreamedMetricJsonDeserializer</p>
	 * <p>Description: A JSON deserializer for StreamedMetrics</p> 
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>com.heliosapm.streams.metrics.StreamedMetricJson.StreamedMetricJsonDeserializer</code></p>
	 */
	public static class StreamedMetricJsonDeserializer extends JsonDeserializer<StreamedMetric> { 
		/**
		 * {@inheritDoc}
		 * @see com.fasterxml.jackson.databind.JsonDeserializer#deserialize(com.fasterxml.jackson.core.JsonParser, com.fasterxml.jackson.databind.DeserializationContext)
		 */
		@Override
		public StreamedMetric deserialize(final JsonParser p, final DeserializationContext ctxt) throws IOException, JsonProcessingException {
			final JsonNode node = p.getCodec().readTree(p);
			final Map<String, String> tags = JSONOps.parseToObject(node.get("tags"), TR_TAGS);
			StreamedMetric sm = new StreamedMetric(node.get("timestamp").asLong(), node.get("metric").textValue(), tags);
			if(node.has("v")) {
				ValueType v = ValueType.valueOf(node.get("v").textValue());
				sm.setValueType(v);
			}
			if(node.has("value")) {
				final JsonNode value = node.get("value");
				if(value.canConvertToLong()) {
					sm = new StreamedMetricValue(sm, value.asLong());
				} else {
					sm = new StreamedMetricValue(sm, value.asDouble());
				}
			}
			return sm;
		}
	}
	/**
	 * <p>Title: StreamedMetricValueJsonSerializer</p>
	 * <p>Description: A JSON serializer for StreamedMetricValues</p> 
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>com.heliosapm.streams.metrics.StreamedMetricJson.StreamedMetricValueJsonSerializer</code></p>
	 */
	public static class StreamedMetricValueJsonSerializer extends JsonSerializer<StreamedMetricValue> {
		/**
		 * {@inheritDoc}
		 * @see com.fasterxml.jackson.databind.JsonSerializer#serialize(java.lang.Object, com.fasterxml.jackson.core.JsonGenerator, com.fasterxml.jackson.databind.SerializerProvider)
		 */
		@Override
		public void serialize(final StreamedMetricValue value, final JsonGenerator j, final SerializerProvider serializers) throws IOException, JsonProcessingException {
			SM_SERIALIZER.serialize(value, j, serializers);			
		}
	}

	/**
	 * <p>Title: StreamedMetricValueJsonDeserializer</p>
	 * <p>Description: A JSON deserializer for StreamedMetricValues</p> 
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>com.heliosapm.streams.metrics.StreamedMetricValueJson.StreamedMetricValueJsonDeserializer</code></p>
	 */
	public static class StreamedMetricValueJsonDeserializer extends JsonDeserializer<StreamedMetricValue> { 
		/**
		 * {@inheritDoc}
		 * @see com.fasterxml.jackson.databind.JsonDeserializer#deserialize(com.fasterxml.jackson.core.JsonParser, com.fasterxml.jackson.databind.DeserializationContext)
		 */
		@Override
		public StreamedMetricValue deserialize(final JsonParser p, final DeserializationContext ctxt) throws IOException, JsonProcessingException {
			return SM_DESERIALIZER.deserialize(p, ctxt).forValue(1L);
		}		
	}
	
	
	private StreamedMetricJson() {}

}
