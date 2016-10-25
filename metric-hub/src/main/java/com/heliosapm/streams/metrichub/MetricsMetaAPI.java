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
package com.heliosapm.streams.metrichub;

import java.util.List;
import java.util.Map;

import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import reactor.core.composable.Promise;
import reactor.core.composable.Stream;

/**
 * <p>Title: MetricsMetaAPI</p>
 * <p>Description: Defines a proposed OpenTSDB metrics meta-data access API</p> 
 * <p>Any parameters where the name of a tag is being specified automatically support <b><code>*</code></b> multi-character wildcards
 * and <b><code>|</code></b> specifying a logical <b><code>OR</code></b> on the seperated strings, or both.
 * <p>A <b><code>TSMeta expression</code></b> is a whole or partial representation of a TSMeta fully qualified name. The expression broadly adopts the
 * same pattern as the JMX {@link javax.management.ObjectName} where the ObjectName's domain is the metric name and the ObjectName's key properties 
 * are the tags. e.g. <ul>
 * 	<li>An exact pattern match: <b><code>sys.cpu:dc=dc1,host=WebServer5,cpu=1,type=combined</code></b></li> 
 *  <li>A partial pattern match: <b><code>sys.cpu:dc=dc1,host=WebServer5,cpu=1,type=*</code></b></li>
 *  <li>The same partial pattern match: <b><code>sys.cpu:dc=dc1,host=WebServer5,cpu=1,*</code></b></li>
 *  <li>A wildcard pattern match: <b><code>sys.cpu:dc=dc1,host=WebServer*,cpu=1,*</code></b></li>
 *  <li>A piped pattern match: <b><code>sys.cpu:dc=dc1,host=AppServer1|WebServer1|DBServer1,cpu=1,type=combined</code></b></li>
 *  <li>A wildcarded and piped pattern match: <b><code>sys.cpu:dc=dc1,host=WebServer*|AppServer*|DBServer*,cpu=1,*</code></b></li>
 * </ul>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrichub.MetricsMetaAPI</code></p>
 */

public interface MetricsMetaAPI {
	/**
	 * Finds {@link UIDMeta}s of the specified type that match the passed name pattern
	 * @param queryContext The query context for this call
	 * @param type The type of UIDMetas to search for
	 * @param name The name or name pattern
	 * @return a set of matching {@link UIDMeta}s
	 */
	public Stream<List<UIDMeta>> find(QueryContext queryContext, UniqueIdType type, String name); 

	/**
	 * Returns the tag keys associated with the passed metric name.
	 * Wildcards will be honoured on metric names and tag keys.
	 * @param queryContext The query options for this call
	 * @param metric The metric name to match
	 * @param tagKeys The tag keys to match
	 * @return A stream of lists of matching UIDMetas
	 */
	public Stream<List<UIDMeta>> getTagKeys(QueryContext queryContext, String metric, String...tagKeys);
	
	/**
	 * <p>Returns the tag values associated with the passed metric name and tag keys.</p>
	 * <p>The combined metric name and tag keys may not resolve to any directly associated tag values 
	 * due to missing intermediary tag keys, or they may resolve partially to some tag values.
	 * In other words, the resolution of a metric name and tag keys may produce tree leafs, tree nodes,
	 * a combination of both, or zero of either.</p>
	 * Wildcards will be honoured on metric names and tag keys.
	 * @param queryContext The query options for this call
	 * @param metric The metric name to match
	 * @param tagPairs The tag pairs to match
	 * @param tagKey 
	 * @return A stream of lists of matching UIDMetas
	 */
	public Stream<List<UIDMeta>> getTagValues(QueryContext queryContext, String metric, Map<String, String> tagPairs, String tagKey);
	
	/**
	 * Returns the associated metric names (metric UIDs) for the passed tag keys.
	 * Wildcards will be honoured on tag keys.
	 * @param queryContext The query options for this call
	 * @param tagKeys The tag keys to match
	 * @return A stream of lists of matching UIDMetas
	 */
	public Stream<List<UIDMeta>> getMetricNames(QueryContext queryContext, String...tagKeys);
	
	/**
	 * Returns the associated metric names (metric UIDs) for the passed tag pairs.
	 * Wildcards will be honoured on metric names and tag keys.
	 * @param queryContext The query options for this call
	 * @param tags The tag pairs to match
	 * @return A stream of lists of matching UIDMetas
	 */
	public Stream<List<UIDMeta>> getMetricNames(QueryContext queryContext, Map<String, String> tags);
	
	/**
	 * Returns the TSMetas matching the passed metric name and tags
	 * @param queryContext The query context for this call
	 * @param metricName The metric name to match
	 * @param tags The tag pairs to match
	 * @return A stream of lists of matching TSMetas 
	 */
	public Stream<List<TSMeta>> getTSMetas(QueryContext queryContext, String metricName, Map<String, String> tags);
	
	/**
	 * Evaluates the passed TSMeta expression and returns the matches.
	 * Wildcards will be honoured on metric names, tag keys and tag values.
	 * @param expression The TSMeta expression to evaluate
	 * @param queryContext The query options for this call
	 * @return A stream of lists of matching TSMetas
	 */
	public Stream<List<TSMeta>> evaluate(QueryContext queryContext, String expression);
	
	/**
	 * Determines if the passed TSMeta expression matches the provided TSMeta UID
	 * @param expression The expression to test
	 * @param tsuid The TSMeta UID bytes
	 * @return the deferred result
	 */
	public Promise<Boolean> match(String expression, byte[] tsuid);
	
	/**
	 * Determines if the passed TSMeta expression matches the provided TSMeta UID
	 * @param expression The expression to test
	 * @param tsuid The TSMeta UID
	 * @return the deferred result
	 */
	public Promise<Boolean> match(String expression, String tsuid);
	
	/**
	 * Indicates how many items the passed expressions have in common when evaluated
	 * @param expressionOne A TSMeta expression
	 * @param expressionTwo Another TSMeta expression
	 * @return the deferred result
	 */
	public long overlap(String expressionOne, String expressionTwo);
	
	/**
	 * Returns the annotations associated to TSMetas that match the passed TSMeta expression within the specified time range.
	 * @param queryContext The query context for this call
	 * @param expression A TSMeta expression to match the TSMetas that the annotations should be associated to
	 * @param startTimeEndTime Can be empty, a start time or a start time and end time range. Times are long UTC timestamps.
	 * @return a stream of lists of matching annotations
	 */
	public Stream<List<Annotation>> getAnnotations(QueryContext queryContext, String expression, long... startTimeEndTime);
	
	/**
	 * Returns the global annotations within the specified time range.
	 * @param queryContext The query context for this call
	 * @param startTimeEndTime Can be empty, a start time or a start time and end time range. Times are long UTC timestamps.
	 * @return a stream of lists of matching global annotations
	 */
	public Stream<List<Annotation>> getGlobalAnnotations(QueryContext queryContext, long... startTimeEndTime);

}
