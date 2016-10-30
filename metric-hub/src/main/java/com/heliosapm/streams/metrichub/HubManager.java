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
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.heliosapm.streams.metrichub.impl.MetricsMetaAPIImpl;
import com.heliosapm.streams.sqlbinder.SQLWorker;

import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import reactor.core.composable.Promise;
import reactor.core.composable.Stream;

/**
 * <p>Title: HubManager</p>
 * <p>Description: Container singleton for all the services needed for distributed queries</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrichub.HubManager</code></p>
 */

public class HubManager implements MetricsMetaAPI {
	/** The singleton instance */
	private static volatile HubManager instance = null;
	/** The singleton instance ctor guard */
	private static final Object lock = new Object();
	
	/** The metrics metadata lookup service */
	private final MetricsMetaAPIImpl metricMetaService;
	/** The TSDBEndpoint to get updates lists of available endpoints */
	private final TSDBEndpoint tsdbEndpoint;
	
	/** Instance logger */
	private final Logger log = LogManager.getLogger(getClass());

	
	/**
	 * Initializes the hub manager
	 * @param properties The configuration properties
	 * @return the HubManager singleton instance
	 */
	static HubManager init(final Properties properties) {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					instance = new HubManager(properties);
				}
			}
		}
		return instance;
	}
	
	/**
	 * Acquires the hub manager
	 * @return the HubManager singleton instance
	 */
	public static HubManager getInstance() {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					throw new IllegalStateException("The HubManager has not been initialized yet. Programmer Error.");
				}
			}
		}
		return instance;
	}

	
	private HubManager(final Properties properties) {
		metricMetaService = new MetricsMetaAPIImpl(properties);
		tsdbEndpoint = TSDBEndpoint.getEndpoint(metricMetaService.getSqlWorker());
	}

	/**
	 * Returns the data source
	 * @return the data source
	 */
	public DataSource getDataSource() {
		return metricMetaService.getDataSource();
	}

	/**
	 * Returns the sql worker
	 * @return the sql worker
	 */
	public SQLWorker getSqlWorker() {
		return metricMetaService.getSqlWorker();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#find(com.heliosapm.streams.metrichub.QueryContext, net.opentsdb.uid.UniqueId.UniqueIdType, java.lang.String)
	 */
	@Override
	public Stream<List<UIDMeta>> find(final QueryContext queryContext, final UniqueIdType type, final String name) {
		return metricMetaService.find(queryContext, type, name);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#getTagKeys(com.heliosapm.streams.metrichub.QueryContext, java.lang.String, java.lang.String[])
	 */
	@Override
	public Stream<List<UIDMeta>> getTagKeys(final QueryContext queryContext, final String metric, final String... tagKeys) {
		return metricMetaService.getTagKeys(queryContext, metric, tagKeys);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#getTagValues(com.heliosapm.streams.metrichub.QueryContext, java.lang.String, java.util.Map, java.lang.String)
	 */
	@Override
	public Stream<List<UIDMeta>> getTagValues(final QueryContext queryContext, final String metric, final Map<String, String> tagPairs, final String tagKey) {
		return metricMetaService.getTagValues(queryContext, metric, tagPairs, tagKey);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#getMetricNames(com.heliosapm.streams.metrichub.QueryContext, java.lang.String[])
	 */
	@Override
	public Stream<List<UIDMeta>> getMetricNames(final QueryContext queryContext, final String... tagKeys) {
		return metricMetaService.getMetricNames(queryContext, tagKeys);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#getMetricNames(com.heliosapm.streams.metrichub.QueryContext, java.util.Map)
	 */
	@Override
	public Stream<List<UIDMeta>> getMetricNames(final QueryContext queryContext, final Map<String, String> tags) {
		return metricMetaService.getMetricNames(queryContext, tags);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#getTSMetas(com.heliosapm.streams.metrichub.QueryContext, java.lang.String, java.util.Map)
	 */
	@Override
	public Stream<List<TSMeta>> getTSMetas(final QueryContext queryContext, final String metricName, final Map<String, String> tags) {
		return metricMetaService.getTSMetas(queryContext, metricName, tags);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#evaluate(com.heliosapm.streams.metrichub.QueryContext, java.lang.String)
	 */
	@Override
	public Stream<List<TSMeta>> evaluate(final QueryContext queryContext, final String expression) {
		return metricMetaService.evaluate(queryContext, expression);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#match(java.lang.String, byte[])
	 */
	@Override
	public Promise<Boolean> match(final String expression, final byte[] tsuid) {
		return metricMetaService.match(expression, tsuid);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#match(java.lang.String, java.lang.String)
	 */
	@Override
	public Promise<Boolean> match(final String expression, final String tsuid) {
		return metricMetaService.match(expression, tsuid);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#overlap(java.lang.String, java.lang.String)
	 */
	@Override
	public long overlap(final String expressionOne, final String expressionTwo) {
		return metricMetaService.overlap(expressionOne, expressionTwo);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#getAnnotations(com.heliosapm.streams.metrichub.QueryContext, java.lang.String, long[])
	 */
	@Override
	public Stream<List<Annotation>> getAnnotations(final QueryContext queryContext, final String expression, final long... startTimeEndTime) {
		return metricMetaService.getAnnotations(queryContext, expression, startTimeEndTime);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#getGlobalAnnotations(com.heliosapm.streams.metrichub.QueryContext, long[])
	 */
	@Override
	public Stream<List<Annotation>> getGlobalAnnotations(final QueryContext queryContext, final long... startTimeEndTime) {
		return metricMetaService.getGlobalAnnotations(queryContext, startTimeEndTime);
	}
	
	
	
	
}
