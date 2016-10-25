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
package com.heliosapm.streams.metrichub.serialization;

import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import net.opentsdb.utils.JSON;

import org.jboss.netty.buffer.ChannelBuffer;

import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * <p>Title: TSDBTypeSerializer</p>
 * <p>Description: Enumerates the serialization methods for TSDB objects</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrichub.serialization.TSDBTypeSerializer</code></p>
 */

public enum TSDBTypeSerializer {
	/** The default serialization */
	FULL,
	/** A slimmed down json payload */
	DEFAULT,
	/** Name only serializer */
	NAME,
	/** D3.js optimized serializer */
	D3;
	

	private final Map<Class<?>, ObjectMapper> serializers = new ConcurrentHashMap<Class<?>, ObjectMapper>();
	private static final Map<TSDBTypeSerializer, Set<JsonSerializer<?>>> byTypeSerializers;
	
	
	static {
		byTypeSerializers = new EnumMap<TSDBTypeSerializer, Set<JsonSerializer<?>>>(TSDBTypeSerializer.class);
		Serializers.register();
		for(TSDBTypeSerializer t: TSDBTypeSerializer.values()) {
			t.rebuildMapper();
		}
	}
	
	private volatile ObjectMapper allTypesMapper = null;
	
	
	private void rebuildMapper() {
		ObjectMapper om = new ObjectMapper();
		Set<JsonSerializer<?>> set = byTypeSerializers.get(this);
		SimpleModule sm = new SimpleModule("All Serializers for [" + name() + "]");
		sm.addSerializer(ChannelBuffer.class, new JSONChannelBufferSerializer());
		if(set!=null && !set.isEmpty()) {			
			for(JsonSerializer<?> js: set) {
				sm.addSerializer(js);
			}
			om.registerModule(sm);
		}	
		allTypesMapper = om;
	}
	
	/**
	 * Registers a serializer for this enum member
	 * @param type The type the serializer is for
	 * @param serializer The serializer
	 */
	public synchronized <T> void registerSerializer(final Class<T> type, final JsonSerializer<T> serializer) {
		serializers.put(type, 
				new ObjectMapper().registerModule(
						new SimpleModule("[" + name() + " Serializer]:" + type.getName())
						.addSerializer(type, serializer)
				)
		);
		Set<JsonSerializer<?>> set = byTypeSerializers.get(this);
		if(set==null) {
			synchronized(byTypeSerializers) {
				set = byTypeSerializers.get(this);
				if(set==null) {
					set = new HashSet<JsonSerializer<?>>();
					byTypeSerializers.put(this, set);
				}
			}
		}
		if(set.add(serializer)) {
			rebuildMapper();
		}
	}
	
	/**
	 * Returns a mapper enabled for all OpenTSDB types for this enum member
	 * @return an ObjectMapper
	 */
	public ObjectMapper getMapper() {
		return allTypesMapper;
	}
	
	/**
	 * Returns the ObjectMapper to serialize objects of the passed type
	 * @param clazz The type to get an ObjectMapper for
	 * @return The registered ObjectMapper or the default if one was not found.
	 */
	public <T> ObjectMapper getSerializer(Class<T> clazz) {
		ObjectMapper mapper = serializers.get(clazz);
		return mapper != null ? mapper : JSON.getMapper();
	}
}
