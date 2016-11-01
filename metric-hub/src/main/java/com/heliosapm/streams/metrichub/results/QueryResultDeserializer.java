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

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.heliosapm.streams.json.JSONOps;
import com.heliosapm.streams.metrichub.RequestBuilder;
import com.heliosapm.streams.tracing.TagKeySorter.TagMap;

/**
 * <p>Title: QueryResultDeserializer</p>
 * <p>Description: Jackson json deserializer for {@link QueryResult}s.</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrichub.results.QueryResultDeserializer</code></p>
 */

public class QueryResultDeserializer extends JsonDeserializer<QueryResult> {
	/** Shareable instance */
	public static final QueryResultDeserializer INSTANCE = new QueryResultDeserializer();


	/**
	 * {@inheritDoc}
	 * @see com.fasterxml.jackson.databind.JsonDeserializer#deserialize(com.fasterxml.jackson.core.JsonParser, com.fasterxml.jackson.databind.DeserializationContext)
	 */
	@Override
	public QueryResult deserialize(final JsonParser p, final DeserializationContext ctxt) throws IOException, JsonProcessingException {
		final JsonNode node = p.getCodec().readTree(p);
		return from(node);
	}
	
	public QueryResult from(final JsonNode node) {
		if(!node.has("metric")) return null;
		final String metric = node.get("metric").textValue();
		final TagMap tags = JSONOps.parseToObject(node.get("tags"), QueryResult.TREE_MAP_TYPE_REF);
		final String[] aggregatedTags = JSONOps.parseToObject(node.get("aggregateTags"), String[].class);
		final JsonNode dpsNode = node.get("dps");
		final Set<long[]> dps = Collections.synchronizedSet(new HashSet<long[]>(dpsNode.size()));
		
		final Iterable<Entry<String, JsonNode>> iterable = () -> dpsNode.fields();				
		StreamSupport.stream(iterable.spliterator(), true).forEach(dp -> 
			dps.add(new long[]{RequestBuilder.toMsTime(dp.getKey()), dp.getValue().asLong()})
		);
		return new QueryResult(metric, tags, aggregatedTags, dps);		
	}

}
