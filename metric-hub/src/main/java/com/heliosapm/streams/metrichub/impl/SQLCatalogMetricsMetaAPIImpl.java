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
package com.heliosapm.streams.metrichub.impl;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.Environment;

import com.heliosapm.streams.metrichub.MetricsMetaAPI;
import com.heliosapm.streams.metrichub.QueryContext;
import com.heliosapm.streams.sqlbinder.SQLWorker;
import com.heliosapm.streams.sqlbinder.TagPredicateCache;

import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import reactor.core.composable.Promise;
import reactor.core.composable.Stream;

/**
 * <p>Title: SQLCatalogMetricsMetaAPIImpl</p>
 * <p>Description: The {@link MetricsMetaAPI} implementation</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrichub.impl.SQLCatalogMetricsMetaAPIImpl</code></p>
 */

public class SQLCatalogMetricsMetaAPIImpl implements MetricsMetaAPI, UncaughtExceptionHandler{
	/** Instance logger */
	protected Logger log = LogManager.getLogger(getClass());
	/** The reactor environment */
	protected final Environment env;
	/** The SQLWorker to manage JDBC Ops */
	protected final SQLWorker sqlWorker;
	/** The TSDB instance */
	protected final TSDB tsdb;
	/** The meta-reader for returning pojos */
	protected final MetaReader metaReader;
	
	/** The tag predicate cache */
	protected final TagPredicateCache tagPredicateCache;
	
	/** The maximum TSUID in Hex String format */
	public static final String MAX_TSUID;
	/** The maximum UID in Hex String format */
	public static final String MAX_UID;
	
	/** The dynamic binding SQL block for tag keys */
	public static final String TAGK_SQL_BLOCK = "K.NAME %s ?"; 
	/** The dynamic binding SQL block for tag values */
	public static final String TAGV_SQL_BLOCK = "V.NAME %s ?"; 
	/** The dynamic binding SQL block for metric names */
	public static final String METRIC_SQL_BLOCK = "M.NAME %s ?"; 
	
	/** Empty UIDMeta set const */
	private static final Set<UIDMeta> EMPTY_UIDMETA_SET =  Collections.unmodifiableSet(new LinkedHashSet<UIDMeta>(0));
	/** Empty TSMeta set const */
	private static final Set<TSMeta> EMPTY_TSMETA_SET =  Collections.unmodifiableSet(new LinkedHashSet<TSMeta>(0));
	/** Empty Annotation set const */
	private static final Set<Annotation> EMPTY_ANNOTATION_SET =  Collections.unmodifiableSet(new LinkedHashSet<Annotation>(0));

	/** The reactor dispatcher name */
	public static final String DISPATCHER_NAME = "MetricsMetaAPIDispatcher";
	
	/** Empty string array const */
	public static final String[] EMPTY_STR_ARR = {};
	/** Pipe parser pattern */
	public static final Pattern SPLIT_PIPES = Pattern.compile("\\|");
	
	/** The UID Retrieval SQL template.
	 * Tokens are:
	 * 1. The target UID type
	 * 2. The correlation UID type
	 * 3. The bind symbols for #2
	 * 4. The start at XUID expression
	 *  */
	public static final String GET_KEY_TAGS_SQL = 
			"SELECT DISTINCT X.* FROM TSD_%s X, TSD_TSMETA F, TSD_FQN_TAGPAIR T, TSD_TAGPAIR P, TSD_%s K " + 
			"WHERE K.XUID = F.METRIC_UID " + 
			"AND F.FQNID = T.FQNID " + 
			"AND T.XUID = P.XUID " + 
			"AND P.TAGK = X.XUID " + 
			"AND (%s) " +   				//  K.NAME = ? et. al.
			"AND X.NAME NOT IN (%s) " +
			"AND %s " + 					// XUID_START_SQL goes here
			"ORDER BY X.XUID DESC " + 
			"LIMIT ?"; 
		
		/** The initial start range if no starting index is supplied. Token is the target table alias  */
		public static final String INITIAL_XUID_START_SQL = " X.XUID <= 'FFFFFF'";
		/** The initial start range if a starting index is supplied. Token is the target table alias */
		public static final String XUID_START_SQL = " X.XUID < ? " ;


		/** The Metric Name Retrieval SQL template when tag keys are provided */
		public static final String GET_METRIC_NAMES_WITH_KEYS_SQL =
				"SELECT DISTINCT X.* FROM TSD_METRIC X, TSD_TSMETA F, TSD_FQN_TAGPAIR T, TSD_TAGPAIR P, TSD_TAGK K  " +
				"WHERE X.XUID = F.METRIC_UID  " +
				"AND F.FQNID = T.FQNID  " +
				"AND T.XUID = P.XUID " +
				"AND P.TAGK = K.XUID " +
				"AND (%s) ";			// K.NAME = ? 	  --- > TAGK_SQL_BLOCK		

		/** The Metric Name Retrieval SQL template when no tag keys are provided */
		public static final String GET_METRIC_NAMES_SQL =
				"SELECT X.* FROM TSD_METRIC X WHERE %s ORDER BY X.XUID DESC LIMIT ?"; 
		
	
		/** The Metric Name Retrieval SQL template when tag pairs are provided */
		public static final String GET_METRIC_NAMES_WITH_TAGS_SQL =
				"SELECT DISTINCT X.* FROM TSD_METRIC X, TSD_TSMETA F, TSD_FQN_TAGPAIR T, TSD_TAGPAIR P, TSD_TAGK K, TSD_TAGV V " +
				"WHERE X.XUID = F.METRIC_UID  " +
				"AND F.FQNID = T.FQNID  " +
				"AND T.XUID = P.XUID " +
				"AND P.TAGK = K.XUID " +
				"AND P.TAGV = V.XUID " +
				"AND (%s) " +					// K.NAME % ? 	  --- > TAGK_SQL_BLOCK		 			
				"AND (%s) ";					// V.NAME % ? 	  --- > TAGV_SQL_BLOCK

		/** The FQNID Retrieval SQL template for matching annotations */
		public static final String GET_TSMETAS_SQL =
				"SELECT X.* FROM TSD_TSMETA X, TSD_METRIC M, TSD_FQN_TAGPAIR T, TSD_TAGPAIR P, TSD_TAGK K, TSD_TAGV V " +
				"WHERE M.XUID = X.METRIC_UID  " +
				"AND X.FQNID = T.FQNID  " +
				"AND T.XUID = P.XUID " +
				"AND P.TAGK = K.XUID " +
				"AND (%s) " + 						// M.NAME = ?    --> METRIC_SQL_BLOCK
				"AND P.TAGV = V.XUID " +
				"AND (%s) " +					// K.NAME % ? 	  --- > TAGK_SQL_BLOCK		 			
				"AND (%s) ";				// V.NAME % ? 	  --- > TAGV_SQL_BLOCK
		
		
		/** The Metric Name Retrieval SQL template when tag pairs are provided */
		public static final String GET_ANN_TSMETAS_SQL =
				"SELECT DISTINCT X.FQNID FROM TSD_TSMETA X, TSD_METRIC M, TSD_FQN_TAGPAIR T, TSD_TAGPAIR P, TSD_TAGK K, TSD_TAGV V " +
				"WHERE M.XUID = X.METRIC_UID  " +
				"AND X.FQNID = T.FQNID  " +
				"AND T.XUID = P.XUID " +
				"AND P.TAGK = K.XUID " +
				"AND (%s) " + 						// M.NAME = ?    --> METRIC_SQL_BLOCK
				"AND P.TAGV = V.XUID " +
				"AND (%s) " +					// K.NAME % ? 	  --- > TAGK_SQL_BLOCK		 			
				"AND (%s) ";				// V.NAME % ? 	  --- > TAGV_SQL_BLOCK

		/** The TSMeta Retrieval SQL template when no tags or metric name are provided and overflow is true */
		public static final String GET_TSMETAS_NO_TAGS_NO_METRIC_NAME_SQL =
				"SELECT X.* FROM TSD_TSMETA X WHERE %s ORDER BY X.TSUID DESC LIMIT ?"; 

		/** The TSMeta Retrieval SQL template when no tags are provided and overflow is true */
		public static final String GET_TSMETAS_NO_TAGS_NAME_SQL =
				"SELECT X.* FROM TSD_TSMETA X, TSD_METRIC M WHERE M.XUID = X.METRIC_UID AND %s AND M.NAME = ? ORDER BY X.TSUID DESC LIMIT ?";
		
		/** The TSMeta Retrieval SQL template when no tags or metric name are provided but a TSUID is */
		public static final String GET_TSMETAS_NO_TAGS_NO_METRIC_NAME_TSUID_SQL =
				"SELECT X.* FROM TSD_TSMETA X WHERE %s AND X.TSUID = ? ORDER BY X.TSUID DESC LIMIT ?"; 

		/** The TSMeta Retrieval SQL template when no tags are provided but a TSUID is */
		public static final String GET_TSMETAS_NO_TAGS_NAME_TSUID_SQL =
				"SELECT X.* FROM TSD_TSMETA X, TSD_METRIC M WHERE M.XUID = X.METRIC_UID AND %s AND M.NAME = ?  AND X.TSUID = ? ORDER BY X.TSUID DESC LIMIT ?"; 
		
	
	
	static {
		char[] fs = new char[4 + (2*4*Const.MAX_NUM_TAGS())];
		Arrays.fill(fs, 'F');
		MAX_TSUID = new String(fs);		
	}

	/**
	 * Creates a new SQLCatalogMetricsMetaAPIImpl
	 */
	public SQLCatalogMetricsMetaAPIImpl() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Thread.UncaughtExceptionHandler#uncaughtException(java.lang.Thread, java.lang.Throwable)
	 */
	@Override
	public void uncaughtException(Thread t, Throwable e) {
		// TODO Auto-generated method stub
		
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#find(com.heliosapm.streams.metrichub.QueryContext, net.opentsdb.uid.UniqueId.UniqueIdType, java.lang.String)
	 */
	@Override
	public Stream<List<UIDMeta>> find(QueryContext queryContext, UniqueIdType type, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#getTagKeys(com.heliosapm.streams.metrichub.QueryContext, java.lang.String, java.lang.String[])
	 */
	@Override
	public Stream<List<UIDMeta>> getTagKeys(QueryContext queryContext, String metric, String... tagKeys) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#getTagValues(com.heliosapm.streams.metrichub.QueryContext, java.lang.String, java.util.Map, java.lang.String)
	 */
	@Override
	public Stream<List<UIDMeta>> getTagValues(QueryContext queryContext, String metric, Map<String, String> tagPairs,
			String tagKey) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#getMetricNames(com.heliosapm.streams.metrichub.QueryContext, java.lang.String[])
	 */
	@Override
	public Stream<List<UIDMeta>> getMetricNames(QueryContext queryContext, String... tagKeys) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#getMetricNames(com.heliosapm.streams.metrichub.QueryContext, java.util.Map)
	 */
	@Override
	public Stream<List<UIDMeta>> getMetricNames(QueryContext queryContext, Map<String, String> tags) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#getTSMetas(com.heliosapm.streams.metrichub.QueryContext, java.lang.String, java.util.Map)
	 */
	@Override
	public Stream<List<TSMeta>> getTSMetas(QueryContext queryContext, String metricName, Map<String, String> tags) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#evaluate(com.heliosapm.streams.metrichub.QueryContext, java.lang.String)
	 */
	@Override
	public Stream<List<TSMeta>> evaluate(QueryContext queryContext, String expression) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#match(java.lang.String, byte[])
	 */
	@Override
	public Promise<Boolean> match(String expression, byte[] tsuid) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#match(java.lang.String, java.lang.String)
	 */
	@Override
	public Promise<Boolean> match(String expression, String tsuid) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#overlap(java.lang.String, java.lang.String)
	 */
	@Override
	public long overlap(String expressionOne, String expressionTwo) {
		// TODO Auto-generated method stub
		return 0;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#getAnnotations(com.heliosapm.streams.metrichub.QueryContext, java.lang.String, long[])
	 */
	@Override
	public Stream<List<Annotation>> getAnnotations(QueryContext queryContext, String expression,
			long... startTimeEndTime) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#getGlobalAnnotations(com.heliosapm.streams.metrichub.QueryContext, long[])
	 */
	@Override
	public Stream<List<Annotation>> getGlobalAnnotations(QueryContext queryContext, long... startTimeEndTime) {
		// TODO Auto-generated method stub
		return null;
	}

}
