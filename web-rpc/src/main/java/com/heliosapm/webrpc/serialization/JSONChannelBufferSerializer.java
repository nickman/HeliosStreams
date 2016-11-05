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
import java.nio.charset.Charset;

import io.netty.buffer.ByteBuf;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

/**
 * <p>Title: JSONChannelBufferSerializer</p>
 * <p>Description: JSON serializer for ChannelBuffers  </p>
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><b><code>org.helios.tsdb.plugins.remoting.json.JSONChannelBufferSerializer</code></b>
 */

public class JSONChannelBufferSerializer extends JsonSerializer<Object> {
	
	/** UTF-8 Charset */
	public static final Charset UTF8_CHARSET = Charset.forName("UTF-8");


	/**
	 * {@inheritDoc}
	 * @see com.fasterxml.jackson.databind.JsonSerializer#serialize(java.lang.Object, com.fasterxml.jackson.core.JsonGenerator, com.fasterxml.jackson.databind.SerializerProvider)
	 * FIXME: Need a way to stream the data in the ChannelBuffer as converting to a byte[] or String will scorch heap usage.
	 */
	@Override
	public void serialize(Object value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
		if(value instanceof ByteBuf) {
			ByteBuf buff = (ByteBuf)value;
			//System.err.println(buff.toString(UTF8_CHARSET));			
			jgen.writeString(buff.toString(UTF8_CHARSET));
			buff.clear();
		} else {			
			provider.defaultSerializeValue(value, jgen);
		}
	}
	
//	protected final ObjectMapper getObjectMapper(final Object value) {
//		if(value instanceof ArrayNode) {
//			ArrayNode an = (ArrayNode)value;
//			if(an.size()==3 && (an.get(2) instanceof POJONode)) {
//				POJONode pojo = (POJONode)an.get(2);
//				Object pojoContent = pojo.getPojo(); 
//				if(pojoContent instanceof TSDBTypeSerializer) {
//					return ((TSDBTypeSerializer)pojoContent).getMapper();
//				}
//			}
//		}
//		return JSON.getMapper();
//	}

}
