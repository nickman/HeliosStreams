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

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.heliosapm.streams.metrichub.DataContext;
import com.heliosapm.streams.tracing.TagKeySorter.TagMap;

/**
 * <p>Title: QueryResult</p>
 * <p>Description: Represents the query results for one distinct metric in an <b>/api/query</b> query to OpenTSDB</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrichub.results.QueryResult</code></p>
 */

public class QueryResult {
	/** The metric name */
	protected final String metricName;
	/** The metric tags */
	protected final Map<String, String> tags;
	/** Aggregated tags */
	protected final String[] aggregatedTags;
	/** The data points */
	protected final SortedSet<long[]> dps;
	
	/** Type reference for a tag key sorted map */
	public static final TypeReference<TagMap> TREE_MAP_TYPE_REF = new TypeReference<TagMap>(){};
	/** Jackson de/serializer initialized, configured and shared */
	private static final ObjectMapper jsonMapper = new ObjectMapper();
	
	/** Empty string array const */
	public static final String[] EMPTY_STR_ARR = {};
	/** Empty dps set const */
	public static final SortedSet<long[]> EMPTY_DPS_SET = Collections.unmodifiableSortedSet(new TreeSet<long[]>());
	
	static {
		// allows parsing NAN and such without throwing an exception. This is
		// important
		// for incoming data points with multiple points per put so that we can
		// toss only the bad ones but keep the good
		jsonMapper.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
	}
	
	/**
	 * <p>Title: DPSComparator</p>
	 * <p>Description: DPS array comparator</p> 
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>com.heliosapm.streams.metrichub.results.QueryResult.DPSComparator</code></p>
	 */
	private static class DPSComparator implements Comparator<long[]> {
		@Override
		public int compare(long[] o1, long[] o2) {			
			return o1[0] < o2[0] ? -1 : o1[0] > o2[0] ? 1 : 0;  
		}
	}
	
	/** DPS comparator */
	public static final Comparator<long[]> DPS_COMPARATOR = new DPSComparator();
	
	/**
	 * Creates a new QueryResult
	 */
	QueryResult(final JsonNode node) {
		metricName = node.get("metric").textValue();
		tags = Collections.unmodifiableMap(jsonMapper.convertValue(node.get("tags"), TREE_MAP_TYPE_REF));
		if(node.has("aggregatedTags")) {
			aggregatedTags = jsonMapper.convertValue(node.get("aggregatedTags"), String[].class);
		} else {
			aggregatedTags = EMPTY_STR_ARR;
		}
		if(node.has("dps")) {
			final JsonNode dpsNodes = node.get("dps");
			if(dpsNodes.size()==0) {
				dps = EMPTY_DPS_SET;
			} else {
				dps = new TreeSet<long[]>(DPS_COMPARATOR);
				final Iterable<Entry<String, JsonNode>> iterable = () -> dpsNodes.fields();				
				StreamSupport.stream(iterable.spliterator(), true).forEach(dp -> 
					dps.add(new long[]{DataContext.toMsTime(dp.getKey()), dp.getValue().asLong()})
				);
				dps.size();
			}
		} else {
			dps = EMPTY_DPS_SET;
		}		
	}
	
	public String toString() {
		final StringBuilder b = new StringBuilder("QResult [\n\tm:")
			.append(metricName).append(tags)
			.append("\n\tAggTags:").append(Arrays.toString(aggregatedTags))
			.append("\n\tDPS:");
		for(long[] dp: dps) {
			b.append("\n\t\t").append(new Date(dp[0])).append(" : ").append(dp[1]);
		}
		b.append("\n]");
		return b.toString();
	}
	
	public static void main(String[] args) {
		try {
			final String jsonResponse = "[    {        \"metric\": \"tsd.hbase.puts\",        \"tags\": {            \"host\": \"tsdb-1.mysite.com\"        },        \"aggregatedTags\": [],        \"dps\": {            \"1365966001\": 3758788892,            \"1365966061\": 3758804070,            \"1365974281\": 3778141673        }    },    {        \"metric\": \"tsd.hbase.puts\",        \"tags\": {            \"host\": \"tsdb-2.mysite.com\"        },        \"aggregatedTags\": [],        \"dps\": {            \"1365966001\": 3902179270,            \"1365966062\": 3902197769,            \"1365974281\": 3922266478        }    }]";
			final List<QueryResult> results = new CopyOnWriteArrayList<QueryResult>();
			StreamSupport.stream(jsonMapper.readTree(jsonResponse).spliterator(), true).forEach(node ->
					results.add(new QueryResult(node))
			);
			for(QueryResult q: results) {
				System.out.println(q);
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
		}
	}

}
