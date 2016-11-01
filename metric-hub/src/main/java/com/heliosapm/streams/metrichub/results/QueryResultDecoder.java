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
package com.heliosapm.streams.metrichub.results;

import java.util.Collections;
import java.util.List;

import com.heliosapm.streams.json.JSONOps;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.FullHttpResponse;

/**
 * <p>Title: QueryResultDecoder</p>
 * <p>Description: Decoder to convert ByteBufs containing JSON to instances of {@link QueryResult}</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrichub.results.QueryResultDecoder</code></p>
 */
@Sharable
public class QueryResultDecoder extends MessageToMessageDecoder<FullHttpResponse> {

	@Override
	protected void decode(final ChannelHandlerContext ctx, final FullHttpResponse msg, final List<Object> out) throws Exception {
		final QueryResult[] qrs = JSONOps.parseToObject(msg.content(), QueryResult[].class);
		for(QueryResult q: qrs) {
			System.err.println(q);
		}
		Collections.addAll(out, qrs);		
	}
	
	static {
		JSONOps.registerDeserializer(QueryResult.class, QueryResultDeserializer.INSTANCE);
		JSONOps.registerDeserializer(QueryResult[].class, QueryResultArrayDeserializer.INSTANCE);
	}
	

}
