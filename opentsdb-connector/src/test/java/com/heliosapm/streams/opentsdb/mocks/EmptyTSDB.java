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
package com.heliosapm.streams.opentsdb.mocks;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.opentsdb.core.Query;
import net.opentsdb.core.WritableDataPoints;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.search.SearchQuery;
import net.opentsdb.stats.Histogram;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.Config;

import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseException;

import com.stumbleupon.async.Deferred;


/**
 * <p>Title: EmptyTSDB</p>
 * <p>Description: Test support class with concrete methods to supply templates for transformed {@link net.opentsdb.core.TSDB} instances.</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.opentsdb.mocks.EmptyTSDB</code></p>
 */

public class EmptyTSDB implements ITSDB {

	/**
	 * Creates a new EmptyTSDB
	 */
	public EmptyTSDB() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#initializePlugins(boolean)
	 */
	@Override
	public void initializePlugins(boolean init_rpcs) {
		/* No Op */

	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#getClient()
	 */
	@Override
	public HBaseClient getClient() {
		
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#getConfig()
	 */
	@Override
	public Config getConfig() {
		
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#getUidName(net.opentsdb.uid.UniqueId.UniqueIdType, byte[])
	 */
	@Override
	public Deferred<String> getUidName(UniqueIdType type, byte[] uid) {
		
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#getUID(net.opentsdb.uid.UniqueId.UniqueIdType, java.lang.String)
	 */
	@Override
	public byte[] getUID(UniqueIdType type, String name) {
		
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#checkNecessaryTablesExist()
	 */
	@Override
	public Deferred<ArrayList<Object>> checkNecessaryTablesExist() {
		
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#uidCacheHits()
	 */
	@Override
	public int uidCacheHits() {
		
		return 0;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#uidCacheMisses()
	 */
	@Override
	public int uidCacheMisses() {
		
		return 0;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#uidCacheSize()
	 */
	@Override
	public int uidCacheSize() {
		
		return 0;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#collectStats(net.opentsdb.stats.StatsCollector)
	 */
	@Override
	public void collectStats(StatsCollector collector) {
		/* No Op */

	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#getPutLatencyHistogram()
	 */
	@Override
	public Histogram getPutLatencyHistogram() {
		
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#getScanLatencyHistogram()
	 */
	@Override
	public Histogram getScanLatencyHistogram() {
		
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#newQuery()
	 */
	@Override
	public Query newQuery() {
		
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#newDataPoints()
	 */
	@Override
	public WritableDataPoints newDataPoints() {
		
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#addPoint(java.lang.String, long, long, java.util.Map)
	 */
	@Override
	public Deferred<Object> addPoint(String metric, long timestamp, long value,
			Map<String, String> tags) {
		
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#addPoint(java.lang.String, long, double, java.util.Map)
	 */
	@Override
	public Deferred<Object> addPoint(String metric, long timestamp,
			double value, Map<String, String> tags) {
		
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#addPoint(java.lang.String, long, float, java.util.Map)
	 */
	@Override
	public Deferred<Object> addPoint(String metric, long timestamp,
			float value, Map<String, String> tags) {
		
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#flush()
	 */
	@Override
	public Deferred<Object> flush() throws HBaseException {
		
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#shutdown()
	 */
	@Override
	public Deferred<Object> shutdown() {
		
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#suggestMetrics(java.lang.String)
	 */
	@Override
	public List<String> suggestMetrics(String search) {
		
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#suggestMetrics(java.lang.String, int)
	 */
	@Override
	public List<String> suggestMetrics(String search, int max_results) {
		
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#suggestTagNames(java.lang.String)
	 */
	@Override
	public List<String> suggestTagNames(String search) {
		
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#suggestTagNames(java.lang.String, int)
	 */
	@Override
	public List<String> suggestTagNames(String search, int max_results) {
		
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#suggestTagValues(java.lang.String)
	 */
	@Override
	public List<String> suggestTagValues(String search) {
		
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#suggestTagValues(java.lang.String, int)
	 */
	@Override
	public List<String> suggestTagValues(String search, int max_results) {
		
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#dropCaches()
	 */
	@Override
	public void dropCaches() {
		/* No Op */

	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#assignUid(java.lang.String, java.lang.String)
	 */
	@Override
	public byte[] assignUid(String type, String name) {
		
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#uidTable()
	 */
	@Override
	public byte[] uidTable() {
		
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#dataTable()
	 */
	@Override
	public byte[] dataTable() {
		
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#treeTable()
	 */
	@Override
	public byte[] treeTable() {
		
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#metaTable()
	 */
	@Override
	public byte[] metaTable() {
		
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#indexTSMeta(net.opentsdb.meta.TSMeta)
	 */
	@Override
	public void indexTSMeta(TSMeta meta) {
		/* No Op */

	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#deleteTSMeta(java.lang.String)
	 */
	@Override
	public void deleteTSMeta(String tsuid) {
		/* No Op */

	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#indexUIDMeta(net.opentsdb.meta.UIDMeta)
	 */
	@Override
	public void indexUIDMeta(UIDMeta meta) {
		/* No Op */

	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#deleteUIDMeta(net.opentsdb.meta.UIDMeta)
	 */
	@Override
	public void deleteUIDMeta(UIDMeta meta) {
		/* No Op */

	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#indexAnnotation(net.opentsdb.meta.Annotation)
	 */
	@Override
	public void indexAnnotation(Annotation note) {
		/* No Op */

	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#deleteAnnotation(net.opentsdb.meta.Annotation)
	 */
	@Override
	public void deleteAnnotation(Annotation note) {
		/* No Op */

	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#processTSMetaThroughTrees(net.opentsdb.meta.TSMeta)
	 */
	@Override
	public Deferred<Boolean> processTSMetaThroughTrees(TSMeta meta) {
		
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.ITSDB#executeSearch(net.opentsdb.search.SearchQuery)
	 */
	@Override
	public Deferred<SearchQuery> executeSearch(SearchQuery query) {
		
		return null;
	}

}
