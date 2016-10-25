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
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.heliosapm.streams.metrichub.MetaReader;
import com.heliosapm.streams.metrichub.MetricsMetaAPI;
import com.heliosapm.streams.metrichub.QueryContext;
import com.heliosapm.streams.metrichub.metareader.DefaultMetaReader;
import com.heliosapm.streams.sqlbinder.SQLWorker;
import com.heliosapm.streams.sqlbinder.TagPredicateCache;
import com.heliosapm.streams.sqlbinder.datasource.SQLCompilerDataSource;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.jmx.ManagedForkJoinPool;

import jsr166y.ForkJoinPool;
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
	/** The execution thread pool */
	protected final ForkJoinPool fjPool;
	/** The data source */
	protected final SQLCompilerDataSource dataSource;
	/** The SQLWorker to manage JDBC Ops */
	protected final SQLWorker sqlWorker;
	/** The meta-reader for returning pojos */
	protected final MetaReader metaReader;
	
	/** The tag predicate cache */
	protected final TagPredicateCache tagPredicateCache;
	
	/** The maximum TSUID in Hex String format */
	public static final String MAX_TSUID;
	/** The maximum UID in Hex String format */
	public static final String MAX_UID = "FFFFFF";   // FIXME: This is now variable. Need to get widths from TSDB
	
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
		final int maxUidChars = MAX_UID.toCharArray().length;
		char[] fs = new char[(maxUidChars * 8) + maxUidChars];
		Arrays.fill(fs, 'F');
		MAX_TSUID = new String(fs);				
	}
	
	/**
	 * Creates a new SQLCatalogMetricsMetaAPIImpl
	 * @param properties The configuration properties
	 */
	public SQLCatalogMetricsMetaAPIImpl(final Properties properties) {
		dataSource = SQLCompilerDataSource.getInstance(properties);
		sqlWorker = dataSource.getSQLWorker();
		tagPredicateCache = new TagPredicateCache(sqlWorker);
		fjPool = new ManagedForkJoinPool(getClass().getSimpleName(), Runtime.getRuntime().availableProcessors(), true, JMXHelper.objectName(getClass()));
		metaReader = new DefaultMetaReader(sqlWorker);
	}
	

	/**
	 * {@inheritDoc}
	 * @see java.lang.Thread.UncaughtExceptionHandler#uncaughtException(java.lang.Thread, java.lang.Throwable)
	 */
	@Override
	public void uncaughtException(Thread t, Throwable e) {
		// TODO Auto-generated method stub
		
	}

//	public static void log(Object fmt, Object...args) {
//		System.out.println(String.format(fmt.toString(), args));
//	}
//	public static void loge(Object fmt, Object...args) {
//		System.err.println(String.format(fmt.toString(), args));
//		if(args.length > 0 && args[args.length-1] instanceof Throwable) {
//			System.err.println("Stack Trace Follows....");
//			((Throwable)args[args.length-1]).printStackTrace(System.err);
//		}
//	}
	
	/** Bind variable token pattern */
	private static final Pattern Q_PATTERN = Pattern.compile("\\?");
	
//	/**
//	 * Fills in some parameterized SQL with literals
//	 * @param sql The base SQL to modify
//	 * @param binds The bind values to fill in
//	 * @return the rewritten SQL
//	 */
//	public static String fillInSQL(String sql, final List<Object> binds) {
//		final int bindCnt = binds.size();
//		Matcher m = Q_PATTERN.matcher(sql);
//		for(int i = 0; i < bindCnt; i++) {
//			Object bind = binds.get(i);
//			if(bind instanceof CharSequence) {
//				sql = m.replaceFirst("'" + bind.toString() + "'");
//			} else if (bind.getClass().isArray()) {
//				sql = m.replaceFirst(renderArray(bind));
//			} else {
//				sql = m.replaceFirst(bind.toString());
//			}		
//			m = Q_PATTERN.matcher(sql);
//		}
//		return sql;
//	}
	
	/**
	 * Renders an array of objects
	 * @param array The array
	 * @return the rendered string
	 */
	public static String renderArray(final Object array) {
		if(array==null) return "";
		final int length = java.lang.reflect.Array.getLength(array);
		if(length < 1) return "";
		final boolean quote = CharSequence.class.isAssignableFrom(array.getClass().getComponentType());
		final StringBuilder b = new StringBuilder();
		for(int i = 0; i < length; i++) {
			if(quote) b.append("'");
			b.append(java.lang.reflect.Array.get(array, i));
			if(quote) b.append("'");
			b.append(", ");
		}
		return b.deleteCharAt(b.length()-1).deleteCharAt(b.length()-1).toString();
		
	}

}
