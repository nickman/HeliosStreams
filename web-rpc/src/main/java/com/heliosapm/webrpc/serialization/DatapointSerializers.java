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
package com.heliosapm.webrpc.serialization;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import net.opentsdb.utils.JSON;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * <p>Title: DatapointSerializers</p>
 * <p>Description: JSON serializers for {@link Datapoint} instances</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.webrpc.serialization.DatapointSerializers</code></p>
 */

public class DatapointSerializers  {
	/** Sharable const instance */
	public static final DatapointSerializer DATAPOINT_SERIALIZER = new DatapointSerializer();
	/** Sharable const instance */
	public static final DatapointArraySerializer DATAPOINT_ARRAY_SERIALIZER = new DatapointArraySerializer();
	/** Sharable const instance */
	public static final DatapointCollectionSerializer DATAPOINT_COLLECTION_SERIALIZER = new DatapointCollectionSerializer();
	
	static {
		final SimpleModule sm = new SimpleModule();
		sm.addSerializer(DATAPOINT_ARRAY_SERIALIZER);
		sm.addSerializer(DATAPOINT_COLLECTION_SERIALIZER);
		JSON.getMapper().registerModule(sm);
	}
	
	/**
	 * <p>Title: DatapointSerializer</p>
	 * <p>Description: JSON serializer for {@link Datapoint} instances</p> 
	 * <p>Company: Helios Development Group LLC</p>
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>org.helios.tsdb.plugins.meta.DatapointSerializers.DatapointSerializer</code></p>
	 */
	public static class DatapointSerializer extends JsonSerializer<Datapoint> {
		@Override
		public void serialize(final Datapoint value, final JsonGenerator jgen, final SerializerProvider provider) throws IOException, JsonProcessingException {
			jgen.writeStartObject();
			jgen.writeStringField("fqn", value.fqn);
			jgen.writeStringField("tsuid", value.tsuid);
			jgen.writeStringField("metric", value.metric);
			jgen.writeObjectFieldStart("tags");
			for(Map.Entry<String, String> e: value.tags.entrySet()) {
				jgen.writeStringField(e.getKey(), e.getValue());
			}
			jgen.writeEndObject();
			value.values.serializeToJson(jgen);
			jgen.writeEndObject();		
		}
	}
	
	/**
	 * <p>Title: DatapointArraySerializer</p>
	 * <p>Description:JSON serializer for {@link Datapoint} arrays</p> 
	 * <p>Company: Helios Development Group LLC</p>
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>org.helios.tsdb.plugins.meta.DatapointSerializers.DatapointArraySerializer</code></p>
	 */
	public static class DatapointArraySerializer extends JsonSerializer<Datapoint[]> {
		@Override
		public void serialize(final Datapoint[] values, final JsonGenerator jgen, final SerializerProvider provider) throws IOException, JsonProcessingException {
			jgen.writeStartArray();
			for(Datapoint d: values) {
				DATAPOINT_SERIALIZER.serialize(d, jgen, provider);
			}
			jgen.writeEndArray();
		}
	}
	
	/**
	 * <p>Title: DatapointCollectionSerializer</p>
	 * <p>Description:JSON serializer for {@link Datapoint} collections</p> 
	 * <p>Company: Helios Development Group LLC</p>
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>org.helios.tsdb.plugins.meta.DatapointSerializers.DatapointCollectionSerializer</code></p>
	 */
	public static class DatapointCollectionSerializer extends JsonSerializer<Collection<Datapoint>> {
		@Override
		public void serialize(final Collection<Datapoint> values, final JsonGenerator jgen, final SerializerProvider provider) throws IOException, JsonProcessingException {
			jgen.writeStartArray();
			for(Datapoint d: values) {
				DATAPOINT_SERIALIZER.serialize(d, jgen, provider);
			}
			jgen.writeEndArray();
		}
	}

	
}
