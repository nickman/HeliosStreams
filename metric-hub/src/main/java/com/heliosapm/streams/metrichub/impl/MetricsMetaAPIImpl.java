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

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.management.ObjectName;
import javax.sql.DataSource;

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
import com.heliosapm.utils.time.SystemClock;
import com.heliosapm.utils.time.SystemClock.ElapsedTime;
import com.heliosapm.utils.url.URLHelper;
import com.lmax.disruptor.LiteBlockingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

import jsr166y.ForkJoinPool;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import reactor.core.composable.Deferred;
import reactor.core.composable.Promise;
import reactor.core.composable.Stream;
import reactor.core.composable.spec.Promises;
import reactor.core.composable.spec.Streams;
import reactor.event.dispatch.Dispatcher;
import reactor.event.dispatch.WorkQueueDispatcher;
import reactor.function.Consumer;

/**
 * <p>Title: MetricsMetaAPIImpl</p>
 * <p>Description: The {@link MetricsMetaAPI} implementation</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrichub.impl.MetricsMetaAPIImpl</code></p>
 */

public class MetricsMetaAPIImpl implements MetricsMetaAPI, UncaughtExceptionHandler, Consumer<Throwable>{
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
	/** The dispatcher used inside the reactor */
	protected final WorkQueueDispatcher dispatcher;
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
	public static final Set<UIDMeta> EMPTY_UIDMETA_SET =  Collections.unmodifiableSet(new LinkedHashSet<UIDMeta>(0));
	/** Empty TSMeta set const */
	public static final Set<TSMeta> EMPTY_TSMETA_SET =  Collections.unmodifiableSet(new LinkedHashSet<TSMeta>(0));
	/** Empty Annotation set const */
	public static final Set<Annotation> EMPTY_ANNOTATION_SET =  Collections.unmodifiableSet(new LinkedHashSet<Annotation>(0));

	/** The reactor dispatcher name */
	public static final String DISPATCHER_NAME = "MetricsMetaAPIDispatcher";
	
	/** An empty tags map const */
	public static final Map<String, String> EMPTY_TAGS = Collections.unmodifiableMap(new HashMap<String, String>(0));
	
	
	/** Empty string array const */
	public static final String[] EMPTY_STR_ARR = {};
	/** Pipe parser pattern */
	public static final Pattern SPLIT_PIPES = Pattern.compile("\\|");
	
	/** Bind variable token pattern */
	public static final Pattern Q_PATTERN = Pattern.compile("\\?");
	
	
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
		
		/** SQL base for retrieving tag values for given keys */
		public static final String GET_TAG_VALUES_SQL =
				"SELECT " +
				"DISTINCT X.* " +
				"FROM TSD_TAGV X, TSD_TSMETA F, TSD_FQN_TAGPAIR T, TSD_TAGPAIR P, TSD_METRIC M, TSD_TAGK K " +
				"WHERE M.XUID = F.METRIC_UID " +
				"AND F.FQNID = T.FQNID " +
				"AND T.XUID = P.XUID " +
				"AND P.TAGV = X.XUID " +
				"AND P.TAGK = K.XUID " +
				"AND (%s) " +   // M.NAME = 'sys.cpu'  --> METRIC_SQL_BLOCK
				"AND (%s) ";	// K.NAME = 'cpu'   -->  TAGK_SQL_BLOCK

//		public static final String INITIAL_XUID_START_SQL = " X.XUID <= 'FFFFFF'";
//		public static final String XUID_START_SQL = " X.XUID < ? " ;
		
		/** SQL base for filtering tags  */
		public static final String GET_TAG_FILTER_SQL = 
			      "SELECT 1 " +
			      "FROM TSD_FQN_TAGPAIR TA, TSD_TAGPAIR PA, TSD_TAGK KA, TSD_TAGV VA " +
			      "WHERE TA.FQNID = F.FQNID " +
			      "AND TA.XUID = PA.XUID " +
			      "AND PA.TAGK = KA.XUID " +
			      "AND PA.TAGV = VA.XUID " +
			      "AND ( " +
				  " %s " + 		// ((KA.NAME = 'dc') AND (VA.NAME = 'dc4'))
			      " )";
				
		

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
		
		/** The initial start range if no starting index is supplied. Token is the target table alias  */
		public static final String INITIAL_TSUID_START_SQL;
		/** The initial start range if a starting index is supplied. Token is the target table alias */
		public static final String TSUID_START_SQL = " X.TSUID <= ? " ;
		
		
	static {
		final int maxUidChars = MAX_UID.toCharArray().length;
		char[] fs = new char[(maxUidChars * 8) + maxUidChars];
		Arrays.fill(fs, 'F');
		MAX_TSUID = new String(fs);				
		INITIAL_TSUID_START_SQL = " X.TSUID <= '" + MAX_TSUID + "'";
	}
	
	public static void main(String[] args) {
		log("DB Only MetaInstance");
		final String config = "./src/test/resources/conf/application.properties";
//		FileInputStream fis = null;
		try {
			final AtomicBoolean print = new AtomicBoolean(false);
			final Properties p = URLHelper.readProperties(URLHelper.toURL(config));
			final MetricsMetaAPIImpl api = new MetricsMetaAPIImpl(p);
			log("MetricsMetaAPI Initialized");
			final QueryContext q = new QueryContext()
					.setTimeout(-1L)
					.setContinuous(true)
					.setPageSize(500)
					.setMaxSize(1701);
			final Thread t = Thread.currentThread();
			final Consumer<List<TSMeta>> consumer = new Consumer<List<TSMeta>>() {
				@Override
				public void accept(final List<TSMeta> metas) {
					for(TSMeta meta: metas) {
						if(print.get()) log("[%s] TSMeta: [%s]", q.getCummulative(), meta.getDisplayName());
					}
					if(!q.shouldContinue()) {
						t.interrupt();
					}
				}
			};
			final Consumer<Throwable> errorHandler = new Consumer<Throwable>(){
				@Override
				public void accept(final Throwable te) {
					loge("Stream ejected an error");
					te.printStackTrace(System.err);
					t.interrupt();
				}
			}; 
			//mq.topic.subs.queue.onqtime-recent:{app=icemqmgr, host=pdk-pt-cefmq-01, q=ifeu.to.mwy.liffe.lqueue, subscription=ifeu.to.mwy.liffe.sub, topic=ifeu.to.mwy.liffe.topic}
			//final String QUERY = "mq.topic.subs.queue.*:app=*, host=pdk-pt-cefmq-0*|pdk-pt-cebmq-0*,subscription=*,topic=ifeu*,*";
			//final String QUERY = "linux.mem.*:host=pdk-pt-cltsdb-*";			
//			final String QUERY = "sys.cpu:host=mad-server,cpu=0,dc=dcX,type=*";
			final String QUERY = "sys.cpu:host=mad-server,cpu=0,dc=*";			
//			final String QUERY = "*:*";
			final ElapsedTime et1 = SystemClock.startClock();
			Stream<List<TSMeta>> metaStream = api.evaluate(q, QUERY);
			metaStream.consume(consumer).when(Throwable.class, errorHandler);
			try { Thread.currentThread().join(); } catch (Exception ex) {
				log("Done. Count: %s, Elapsed: %s, Q:\n%s", q.getCummulative(), et1.elapsedStrMs(), q);
				if(Thread.interrupted()) Thread.interrupted();
			}			
			//print.set(true);
			final ElapsedTime et2 = SystemClock.startClock();
			api.evaluate(q.reset(), QUERY)
				.consume(consumer).when(Throwable.class, errorHandler);
			try { Thread.currentThread().join(); } catch (Exception ex) {
				log("Done. Count: %s, Elapsed: %s, Q:\n%s", q.getCummulative(), et2.elapsedStrMs(), q.reportElapsed());
				if(Thread.interrupted()) Thread.interrupted();
			}			
		} catch (Exception ex) {
			loge("Start Failed", ex);
		} finally {
//			if(fis!=null) try { fis.close(); } catch (Exception x) {/* No Op */}
		}
	}
	
	public static void log(Object fmt, Object...args) {
		System.out.println(String.format(fmt.toString(), args));
	}
	public static void loge(Object fmt, Object...args) {
		System.err.println(String.format(fmt.toString(), args));
		if(args.length > 0 && args[args.length-1] instanceof Throwable) {
			System.err.println("Stack Trace Follows....");
			((Throwable)args[args.length-1]).printStackTrace(System.err);
		}
	}

	/**
	 * Creates a new MetricsMetaAPIImpl
	 * @param properties The configuration properties
	 */
	public MetricsMetaAPIImpl(final Properties properties) {
		dataSource = SQLCompilerDataSource.getInstance(properties);
		sqlWorker = dataSource.getSQLWorker();
		tagPredicateCache = new TagPredicateCache(sqlWorker);
		fjPool = new ManagedForkJoinPool(getClass().getSimpleName(), Runtime.getRuntime().availableProcessors(), true, JMXHelper.objectName(getClass()));
		metaReader = new DefaultMetaReader(sqlWorker);
		dispatcher = new WorkQueueDispatcher("MetricsMetaDispatcher", Runtime.getRuntime().availableProcessors(), 1024, this, ProducerType.MULTI, new LiteBlockingWaitStrategy());
		log.info("Dispatcher Alive: {}", dispatcher.alive());
	}
	
	public Dispatcher getDispatcher() {
		return dispatcher;
	}
	
	/**
	 * {@inheritDoc}
	 * @see reactor.function.Consumer#accept(java.lang.Object)
	 */
	@Override
	public void accept(final Throwable t) {
		log.error("WorkQueueDispatcher Uncaught Exception", t);
		
	}
	
	/**
	 * Shuts down this service
	 * @throws IOException never thrown
	 */
	public void close() throws IOException {
		log.info(">>>>> Closing MetricsMetaAPIImpl...");
		if(fjPool!=null) {			
			try {
				fjPool.shutdown();
				if(!fjPool.awaitTermination(5, TimeUnit.SECONDS)) {
					log.info("ForkJoinPool shutdown cleanly");
				}
			} catch (InterruptedException iex) {
				log.warn("Thread interrupted while waiting for ForkJoinPool to shutdown. Forcing termination.");
				fjPool.shutdownNow();
			}
		}
		if(dispatcher!=null) {			
			if(!dispatcher.awaitAndShutdown(5, TimeUnit.SECONDS)) {
				log.info("Dispatcher shutdown cleanly");
			} else {
				dispatcher.halt();
				log.info("Dispatcher still running after grade period. It was halted.");
			}
		}
		log.info("<<<<< MetricsMetaAPIImpl Closed.");
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#getTagKeys(com.heliosapm.streams.metrichub.QueryContext, java.lang.String, java.lang.String[])
	 */
	@Override
	public Stream<List<UIDMeta>> getTagKeys(final QueryContext queryContext, final String metric, final String...tagKeys) {
		return getUIDsFor(null, queryContext, UniqueId.UniqueIdType.TAGK, UniqueId.UniqueIdType.METRIC, metric, tagKeys);
	}
	
	
	/**
	 * Executes a single correlated UID query, where the target is the type we want to query and the filter is the correlation data.
	 * @param priorDeferred An optional deferred result from a prior continuous call. If null, this is assumed
	 * to be the first call and a new deferred will be created.
	 * @param queryContext The query options 
	 * @param targetType The type we're looking up
	 * @param filterType The type we're using to filter
	 * @param filterName The primary filter name driver
	 * @param excludes Values of the target name that we don't want
	 * @return A deferred result to a set of matching UIDMetas of the type defined in the target argument
	 */
	protected Stream<List<UIDMeta>> getUIDsFor(final Deferred<UIDMeta, Stream<UIDMeta>> priorDeferred, final QueryContext queryContext, final UniqueId.UniqueIdType targetType, final UniqueId.UniqueIdType filterType,  final String filterName, final String...excludes) {
		final Deferred<UIDMeta, Stream<UIDMeta>> def = getDeferred(priorDeferred, queryContext);
		final Stream<List<UIDMeta>> stream = def.compose().collect();
		final String _filterName = (filterName==null || filterName.trim().isEmpty()) ? "*" : filterName.trim();
		fjPool.execute(new Runnable() {
			@SuppressWarnings("boxing")
			public void run() {
				final List<Object> binds = new ArrayList<Object>();
				String sql = null;
				final String predicate = expandPredicate(_filterName, TAGK_SQL_BLOCK, binds);
				try {
					
//					binds.add(filterName);
					StringBuilder keyBinds = new StringBuilder();
					for(String key: excludes) {
						keyBinds.append("?, ");
						binds.add(key);
					}			
					if(keyBinds.length()>0) {
						keyBinds.deleteCharAt(keyBinds.length()-1); 
						keyBinds.deleteCharAt(keyBinds.length()-1);
					}
//					 * 1. The target UID type
//					 * 2. The correlation UID type
//					 * 3. The bind symbols for #2
//					 * 4. The start at XUID expression
					
					if(queryContext.getNextIndex()==null) {
						sql = String.format(GET_KEY_TAGS_SQL, targetType, filterType, predicate, keyBinds, INITIAL_XUID_START_SQL); 
					} else {
						sql = String.format(GET_KEY_TAGS_SQL, targetType, filterType, predicate, keyBinds, XUID_START_SQL);
						binds.add(queryContext.getNextIndex());
					}
					binds.add(queryContext.getNextMaxLimit() + 1);					
					if(log.isDebugEnabled()) log.debug("Executing SQL [{}]", fillInSQL(sql, binds));
					final ResultSet rset = sqlWorker.executeQuery(sql, true, binds.toArray(new Object[0]));
					final IndexProvidingIterator<UIDMeta> iter = metaReader.iterateUIDMetas(rset, targetType);
					try {
						while(processStream(iter, def, queryContext)) {/* No Op */} 
					} finally {
						try { rset.close(); } catch (Exception x) {/* No Op */}
					}
				} catch (Exception ex) {
					log.error("Failed to execute getTagKeysFor.\nSQL was [{}]", sql, ex);
					def.accept(new Exception("Failed to execute getTagValues", ex));
				}
			}
		});
		return stream;
	} 
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#getMetricNames(com.heliosapm.streams.metrichub.QueryContext, java.lang.String[])
	 */
	@Override
	public Stream<List<UIDMeta>> getMetricNames(final QueryContext queryContext, final String... tagKeys) {
		return getMetricNames(null, queryContext, tagKeys);
	}

	
	/**
	 * Returns the associated metric names (metric UIDs) for the passed tag keys.
	 * Wildcards will be honoured on metric names and tag keys.
	 * @param priorDeferred An optional deferred result from a prior continuous call. If null, this is assumed
	 * to be the first call and a new deferred will be created.
	 * @param queryContext The query context 
	 * @param tagKeys an array of tag keys to exclude
	 * @return the deferred result
	 */
	protected Stream<List<UIDMeta>> getMetricNames(final reactor.core.composable.Deferred<UIDMeta, Stream<UIDMeta>> priorDeferred, final QueryContext queryContext, final String... tagKeys) {
		final reactor.core.composable.Deferred<UIDMeta, Stream<UIDMeta>> def = getDeferred(priorDeferred, queryContext);
		final Stream<List<UIDMeta>> stream = def.compose().collect();
		
		fjPool.execute(new Runnable() {
			@SuppressWarnings("boxing")
			public void run() {				
				final List<Object> binds = new ArrayList<Object>();
				
				String sql = null;
				try {
					if(tagKeys==null || tagKeys.length==0) {										
						if(queryContext.getNextIndex()==null) {
							sql = String.format(GET_METRIC_NAMES_SQL,INITIAL_XUID_START_SQL); 
						} else {
							sql = String.format(GET_METRIC_NAMES_SQL, XUID_START_SQL);
							binds.add(queryContext.getNextIndex());
						}
					} else {
						StringBuilder keySql = new StringBuilder("SELECT * FROM ( ").append(String.format(GET_METRIC_NAMES_WITH_KEYS_SQL, expandPredicate(tagKeys[0], TAGK_SQL_BLOCK, binds))); 
//						binds.add(tagKeys[0]);
						int tagCount = tagKeys.length;
						for(int i = 1; i < tagCount; i++) {
							keySql.append(" INTERSECT ").append(String.format(GET_METRIC_NAMES_WITH_KEYS_SQL, expandPredicate(tagKeys[i], TAGK_SQL_BLOCK, binds)));
//							binds.add(tagKeys[i]);
						}
						keySql.append(") X ");
						if(queryContext.getNextIndex()==null) {
							keySql.append(" WHERE ").append(INITIAL_XUID_START_SQL);
						} else {
							keySql.append(" WHERE ").append(XUID_START_SQL);
							binds.add(queryContext.getNextIndex());
						}
						keySql.append(" ORDER BY X.XUID DESC LIMIT ? ");
						sql = keySql.toString();
					}
					binds.add(queryContext.getNextMaxLimit() + 1);
					if(log.isDebugEnabled()) log.debug("Executing SQL [{}]", fillInSQL(sql, binds));
					final ResultSet rset = sqlWorker.executeQuery(sql, true, binds.toArray(new Object[0]));
					final IndexProvidingIterator<UIDMeta> iter = metaReader.iterateUIDMetas(rset, UniqueIdType.METRIC);
					try {
						while(processStream(iter, def, queryContext)) {/* No Op */} 
					} finally {
						try { rset.close(); } catch (Exception x) {/* No Op */}
					}
				} catch (Exception ex) {
					log.error("Failed to execute getMetricNamesFor.\nSQL was [{}]", sql, ex);
					def.accept(new Exception("Failed to execute getTagValues", ex));				
				}
			}
		});
		return stream;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#getMetricNames(com.heliosapm.streams.metrichub.QueryContext, java.util.Map)
	 */
	@Override
	public Stream<List<UIDMeta>> getMetricNames(final QueryContext queryContext, final Map<String, String> tags) {
		return getMetricNames(null, queryContext, tags);
	}
	
	
	/**
	 * Returns the associated metric names (metric UIDs) for the passed tag pairs.
	 * Wildcards will be honoured on metric names and tag keys.
	 * @param priorDeferred An optional deferred result from a prior continuous call. If null, this is assumed
	 * to be the first call and a new deferred will be created.
	 * @param queryContext The query context 
	 * @param tags The TSMeta tags to match
	 * @return the deferred result
	 */
	protected Stream<List<UIDMeta>> getMetricNames(final reactor.core.composable.Deferred<UIDMeta, Stream<UIDMeta>> priorDeferred, final QueryContext queryContext, final Map<String, String> tags) {
		final reactor.core.composable.Deferred<UIDMeta, Stream<UIDMeta>> def = getDeferred(priorDeferred, queryContext);
		final Stream<List<UIDMeta>> stream = def.compose().collect();
		
		final Map<String, String> _tags = (tags==null) ? EMPTY_TAGS : tags;
		fjPool.execute(new Runnable() {
			@SuppressWarnings("boxing")
			public void run() {				
				final List<Object> binds = new ArrayList<Object>();
				final List<String> likeOrEquals = new ArrayList<String>();
				String sql = null;
				try {
					if(_tags==null || _tags.isEmpty()) {										
						if(queryContext.getNextIndex()==null) {
							sql = String.format(GET_METRIC_NAMES_SQL,INITIAL_XUID_START_SQL); 
						} else {
							sql = String.format(GET_METRIC_NAMES_SQL, XUID_START_SQL);
							binds.add(queryContext.getNextIndex());
						}
					} else {
//						String predicate = expandPredicate(tagKeys[0], TAGK_SQL_BLOCK, binds)
						Iterator<Map.Entry<String, String>> iter = _tags.entrySet().iterator();
						Map.Entry<String, String> entry = iter.next();
								
						StringBuilder keySql = new StringBuilder("SELECT * FROM ( ").append(String.format(GET_METRIC_NAMES_WITH_TAGS_SQL, 
								expandPredicate(entry.getKey(), TAGK_SQL_BLOCK, binds),
								expandPredicate(entry.getValue(), TAGV_SQL_BLOCK, binds)								
						));
						
						//processBindsAndTokens(entry, binds, likeOrEquals);
						while(iter.hasNext()) {
//							processBindsAndTokens(iter.next(), binds, likeOrEquals);
							entry = iter.next();
							keySql.append(" INTERSECT ").append(String.format(GET_METRIC_NAMES_WITH_TAGS_SQL,
									expandPredicate(entry.getKey(), TAGK_SQL_BLOCK, binds),
									expandPredicate(entry.getValue(), TAGV_SQL_BLOCK, binds)																	
							));
						}
						keySql.append(") X ");
						if(queryContext.getNextIndex()==null) {
							keySql.append(" WHERE ").append(INITIAL_XUID_START_SQL);
						} else {
							keySql.append(" WHERE ").append(XUID_START_SQL);
							binds.add(queryContext.getNextIndex());
						}
						keySql.append(" ORDER BY X.XUID DESC LIMIT ? ");
						sql = String.format(keySql.toString(), likeOrEquals.toArray());
					} 
					binds.add(queryContext.getNextMaxLimit() + 1);
					if(log.isDebugEnabled()) log.debug("Executing SQL [{}]", fillInSQL(sql, binds));
					final ResultSet rset = sqlWorker.executeQuery(sql, true, binds.toArray(new Object[0]));
					final IndexProvidingIterator<UIDMeta> iter = metaReader.iterateUIDMetas(rset, UniqueIdType.METRIC);
					try {
						while(processStream(iter, def, queryContext)) {/* No Op */} 
					} finally {
						try { rset.close(); } catch (Exception x) {/* No Op */}
					}					
				} catch (Exception ex) {
					log.error("Failed to execute getMetricNamesFor (with tags).\nSQL was [{}]", sql, ex);
					def.accept(new Exception("Failed to execute getTagValues", ex));				
				}
			}
		});
		return stream;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#getTSMetas(com.heliosapm.streams.metrichub.QueryContext, java.lang.String, java.util.Map)
	 */
	@Override
	public Stream<List<TSMeta>> getTSMetas(final QueryContext queryContext, final String metricName, final Map<String, String> tags) {
		return getTSMetas(null, queryContext.startExpiry(), metricName, tags, false, null);
	}
	
	/**
	 * Executes a TSMetas query
	 * @param priorDeferred An optional deferred result from a prior continuous call. If null, this is assumed
	 * to be the first call and a new deferred will be created.
	 * @param queryContext The query context
	 * @param metricName The optional TSMeta metric name or expression. Will be substituted with <b><code>*</code></b> if null or empty
	 * @param tags The optional TSMeta tags. Will be substituted with an empty map if null.
	 * @param propertyListPattern Indicates if the expression is a {@link ObjectName#isPropertyListPattern()}, i.e. if it ends with a <b><code>,*</code></b>.
	 * @param tsuid The optional TSMeta UID used for matching patterns. Ignored if null.
	 * @return the deferred result
	 */
	protected Stream<List<TSMeta>> getTSMetas(final Deferred<TSMeta, Stream<TSMeta>> priorDeferred, final QueryContext queryContext, final String metricName, final Map<String, String> tags, final boolean propertyListPattern, final String tsuid) {
		final Deferred<TSMeta, Stream<TSMeta>> def = getDeferred(priorDeferred, queryContext);
		final Stream<List<TSMeta>> stream = def.compose().collect();		
		final String _metricName = (metricName==null || metricName.trim().isEmpty()) ? "*" : metricName.trim();
		final Map<String, String> _tags = (tags==null) ? EMPTY_TAGS : tags;
		
		fjPool.execute(new Runnable() {
			@SuppressWarnings({ "boxing" })
			public void run() {								
				final List<Object> binds = new ArrayList<Object>();
				final StringBuilder sqlBuffer = new StringBuilder();
				try {
					generateTSMetaSQL(sqlBuffer, binds, queryContext, _metricName, _tags, (propertyListPattern ? "UNION ALL" : "INTERSECT"), tsuid);		
					final int expectedRows = queryContext.getNextMaxLimit(); 
//					final int expectedRows = queryContext.getPageSize();
					binds.add(expectedRows);
					queryContext.addCtx("SQLPrepared", System.currentTimeMillis());
					if(log.isDebugEnabled()) log.debug("Executing SQL [{}]", fillInSQL(sqlBuffer.toString(), binds));					
					final ResultSet rset = sqlWorker.executeQuery(sqlBuffer.toString(), expectedRows, false, binds.toArray(new Object[0]));
					rset.setFetchSize(expectedRows);
					queryContext.addCtx("SQLExecuted", System.currentTimeMillis());
//					final List<TSMeta> tsMetas = metaReader.readTSMetas(rset, true);
					final IndexProvidingIterator<TSMeta> tsMetas = metaReader.iterateTSMetas(rset, false);  // true makes it quite slow
					queryContext.addCtx("SQLRSetIter", System.currentTimeMillis());
					try {
						while(processStream(tsMetas, def, queryContext)) {
							queryContext.startExpiry();
						} 
					} finally {
						try { rset.close(); } catch (Exception x) {/* No Op */}
					}
					if(queryContext.isContinuous() && queryContext.shouldContinue()) {
						fjPool.execute(new Runnable(){
							public void run() {
								getTSMetas(def, queryContext, metricName, tags, propertyListPattern, tsuid);
							}
						});
					}
				} catch (Exception ex) {
					log.error("Failed to execute getTSMetas (with tags).\nSQL was [{}]", sqlBuffer, ex);
					def.accept(new Exception("Failed to execute getTSMetas", ex));
				}
			}
		});
		return stream;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#match(java.lang.String, byte[])
	 */
	@Override
	public Promise<Boolean> match(final String expression, final byte[] tsuid) {
		if(tsuid==null || tsuid.length==0) {
			throw new IllegalArgumentException("The passed tsuid was null or empty");
		}
		
		return match(expression, UniqueId.uidToString(tsuid));		
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#match(java.lang.String, java.lang.String)
	 */
	@Override
	public Promise<Boolean> match(final String expression, final String tsuid) {
		
		if(expression==null || expression.trim().isEmpty()) {
			throw new IllegalArgumentException("The passed expression was null or empty");
		}
		if(tsuid==null || tsuid.trim().isEmpty()) {
			throw new IllegalArgumentException("The passed tsuid was null or empty");
		}
		final Deferred<Boolean, Promise<Boolean>> def = Promises.<Boolean>defer().dispatcher(this.dispatcher).get();
		final Promise<Boolean> promise = def.compose();		
		dispatcher.execute(new Runnable(){
			public void run() {
				final String expr = expression.trim();
				try {			
					final ObjectName on = JMXHelper.objectName(expr);
					getTSMetas(null, new QueryContext().setPageSize(1).setMaxSize(1).startExpiry(), on.getDomain(), on.getKeyPropertyList(), on.isPropertyListPattern(), tsuid)
						.consume(new Consumer<List<TSMeta>>() {
							@Override
							public void accept(List<TSMeta> t) {
								def.accept(!(t==null || t.isEmpty()));
								TSMeta tsmeta = t.get(0);
								log.debug("Matched Expression \n\tPattern: [{}] \n\tIncoming: [{}]", expr, buildObjectName(tsmeta.getMetric().getName(), tsmeta.getTags()));
							}
						}).when(Throwable.class, new Consumer<Throwable>() {
							public void accept(Throwable t) {								
								def.accept(new RuntimeException("Failed to process match on expression [" + expr + "] and TSUID [" + tsuid + "]", t));
							}
						});
				} catch (Exception ex) {
					def.accept(new RuntimeException("Failed to process match on expression [" + expr + "] and TSUID [" + tsuid + "]", ex));
				}
				
			}
		});
		return promise;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#overlap(java.lang.String, java.lang.String)
	 */
	@Override
	public long overlap(final String expressionOne, final String expressionTwo) {
		if(expressionOne==null || expressionOne.trim().isEmpty()) {
			throw new IllegalArgumentException("The passed expressionOne was null or empty");
		}
		if(expressionTwo==null || expressionTwo.trim().isEmpty()) {
			throw new IllegalArgumentException("The passed expressionTwo was null or empty");
		}
//		final long start = System.currentTimeMillis();
		final List<Object> binds = new ArrayList<Object>();
		final StringBuilder sqlBuffer = new StringBuilder("SELECT COUNT(*) FROM ( ");
		final ObjectName on1 = JMXHelper.objectName(expressionOne);
		final ObjectName on2 = JMXHelper.objectName(expressionTwo);
		prepareGetTSMetasSQL(on1.getDomain(), on1.getKeyPropertyList(), binds, sqlBuffer, GET_TSMETAS_SQL, "INTERSECT");
		sqlBuffer.append("\n\tEXCEPT\n");
		prepareGetTSMetasSQL(on2.getDomain(), on2.getKeyPropertyList(), binds, sqlBuffer, GET_TSMETAS_SQL, "INTERSECT");
		sqlBuffer.append(") X ");
		if(log.isDebugEnabled()) log.debug("Executing SQL [{}]", fillInSQL(sqlBuffer.toString(), binds));		
		final long result = sqlWorker.sqlForLong(sqlBuffer.toString(), binds.toArray(new Object[0]));
//		final long elapsed = System.currentTimeMillis()-start;
		//log.info("Computed overlap for expressions:\n\tExpression One: [{}]\n\tExpression Two: [{}]\n\tElapsed: [{}] ms\n\tResult: [{}]", expressionOne, expressionTwo, elapsed, result);
		return result;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#getTagValues(com.heliosapm.streams.metrichub.QueryContext, java.lang.String, java.util.Map, java.lang.String)
	 */
	@Override
	public Stream<List<UIDMeta>> getTagValues(final QueryContext queryContext, final String metricName, final Map<String, String> tags, final String tagKey) {
		return getTagValues(null, queryContext, metricName, tags, tagKey);
	}
	
	/**
	 * @param priorDeferred An optional deferred result from a prior continuous call. If null, this is assumed
	 * to be the first call and a new deferred will be created.
	 * @param queryContext The query context 
	 * @param metricName The optional metric name to match against
	 * @param tags The TSMeta tags to match against
	 * @param tagKey The tag key to get the values of
	 * @return the deferred result
	 */
	protected Stream<List<UIDMeta>> getTagValues(final reactor.core.composable.Deferred<UIDMeta, Stream<UIDMeta>> priorDeferred, final QueryContext queryContext, final String metricName, final Map<String, String> tags, final String tagKey) {
		final reactor.core.composable.Deferred<UIDMeta, Stream<UIDMeta>> def = getDeferred(priorDeferred, queryContext);
		final Stream<List<UIDMeta>> stream = def.compose().collect();

		final String _metricName = (metricName==null || metricName.trim().isEmpty()) ? "*" : metricName.trim();
		final String _tagKey = (tagKey==null || tagKey.trim().isEmpty()) ? "*" : tagKey.trim();
		final Map<String, String> _tags = (tags==null) ? EMPTY_TAGS : tags;
		
		fjPool.execute(new Runnable() {
			@SuppressWarnings("boxing")
			public void run() {				
				final List<Object> binds = new ArrayList<Object>();
				final StringBuilder sqlBuffer = new StringBuilder(String.format(GET_TAG_VALUES_SQL,
						expandPredicate(_metricName, METRIC_SQL_BLOCK, binds),
						expandPredicate(_tagKey, TAGK_SQL_BLOCK, binds)
				));
				if(!_tags.isEmpty()) {
					sqlBuffer.append(" AND EXISTS ( ");
					boolean first = true;
					for(Map.Entry<String, String> pair: _tags.entrySet()) {
						if(!first) sqlBuffer.append(" \nINTERSECT\n ");
						StringBuilder b = new StringBuilder("( ( ");
						b.append(expandPredicate(pair.getKey(), " KA.NAME %s ? ", binds));
						b.append(" ) AND ( ");
						b.append(expandPredicate(pair.getValue(), " VA.NAME %s ? ", binds));
						b.append(" ) ) ");
						sqlBuffer.append(String.format(GET_TAG_FILTER_SQL, b.toString()));
						first = false;
						
					}
					sqlBuffer.append(" ) ");
				}
				sqlBuffer.append(" AND ");
				if(queryContext.getNextIndex()!=null && !queryContext.getNextIndex().toString().trim().isEmpty()) {
					sqlBuffer.append(XUID_START_SQL);
					binds.add(queryContext.getNextIndex().toString().trim());
				} else {
					sqlBuffer.append(INITIAL_XUID_START_SQL);
				}
				sqlBuffer.append(" ORDER BY X.XUID DESC LIMIT ? ");
				binds.add(queryContext.getNextMaxLimit() + 1);
				try {
					final ResultSet rset = sqlWorker.executeQuery(sqlBuffer.toString(), true, binds.toArray(new Object[0]));
					final IndexProvidingIterator<UIDMeta> iter = metaReader.iterateUIDMetas(rset, UniqueIdType.TAGV);
					try {
						while(processStream(iter, def, queryContext)) {/* No Op */} 
					} finally {
						try { rset.close(); } catch (Exception x) {/* No Op */}
					}
				} catch (Exception ex) {
					log.error("Failed to execute getTagValues (with tags).\nSQL was [{}]", sqlBuffer, ex);
					def.accept(new Exception("Failed to execute getTagValues", ex));
				}
			}
		});
		return stream;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#evaluate(com.heliosapm.streams.metrichub.QueryContext, java.lang.String)
	 */
	@Override
	public Stream<List<TSMeta>> evaluate(final QueryContext queryContext, final String expression) {
		if(expression==null || expression.trim().isEmpty()) {
			throw new IllegalArgumentException("The passed expression was null or empty");
		}
		final String expr = expression.trim();
		try {			
			final ObjectName on = JMXHelper.objectName(expr);
			return getTSMetas(null, queryContext.startExpiry(), on.getDomain(), on.getKeyPropertyList(), on.isPropertyListPattern(), null);
		} catch (Exception ex) {
			throw new RuntimeException("Failed to process expression [" + expr + "]", ex);
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#getAnnotations(com.heliosapm.streams.metrichub.QueryContext, java.lang.String, long[])
	 */
	@Override
	public Stream<List<Annotation>> getAnnotations(final QueryContext queryContext, final String expression, final long... startTimeEndTime) {
		return null;  // TODO
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#getGlobalAnnotations(com.heliosapm.streams.metrichub.QueryContext, long[])
	 */
	@Override
	public Stream<List<Annotation>> getGlobalAnnotations(QueryContext queryOptions, long... startTimeEndTime) {
		return null; // TODO
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#find(com.heliosapm.streams.metrichub.QueryContext, net.opentsdb.uid.UniqueId.UniqueIdType, java.lang.String)
	 */
	@Override
	public Stream<List<UIDMeta>> find(final QueryContext queryContext, final UniqueIdType type, final String name) {
		return find(null, queryContext, type, name);
	}


	
	/**
	 * Finds and streams a set of UIDMetas matching the passed name pattern
	 * @param priorDeferred An optional deferred result from a prior continuous call. If null, this is assumed
	 * to be the first call and a new deferred will be created.
	 * @param queryContext The query context
	 * @param type The type of UIDMeta to search
	 * @param name The name pattern to match
	 * @return a deferred stream of lists of matching UIDMetas
	 */
	public Stream<List<UIDMeta>> find(final Deferred<UIDMeta, Stream<UIDMeta>> priorDeferred, final QueryContext queryContext, final UniqueIdType type, final String name) {
		final Deferred<UIDMeta, Stream<UIDMeta>> def = getDeferred(priorDeferred, queryContext);
		final Stream<List<UIDMeta>> stream = def.compose().collect();
		
		if(name==null || name.trim().isEmpty()) { 
			def.accept(new IllegalArgumentException("The passed name was null or empty"));
			return stream;
		}
		if(type==null) { 
			def.accept(new IllegalArgumentException("The passed type was null"));
			return stream;
		}
		fjPool.execute(new Runnable() {
			public void run() {
				final List<Object> binds = new ArrayList<Object>();
				StringBuilder sqlBuffer = new StringBuilder("SELECT * FROM TSD_")
					.append(type.name()).append(" X WHERE (")
					.append(expandPredicate(name.trim(), "X.NAME %s ?", binds))
					.append(") AND ");
				if(queryContext.getNextIndex()!=null && !queryContext.getNextIndex().toString().trim().isEmpty()) {
					sqlBuffer.append(XUID_START_SQL);
					binds.add(queryContext.getNextIndex().toString().trim());
				} else {
					sqlBuffer.append(INITIAL_XUID_START_SQL);
				}
				sqlBuffer.append(" ORDER BY X.XUID DESC LIMIT ? ");
				binds.add(queryContext.getNextMaxLimit() + 1);
				if(log.isDebugEnabled()) log.debug("Executing SQL [{}]", fillInSQL(sqlBuffer.toString(), binds));
				try {
					
					final ResultSet rset = sqlWorker.executeQuery(sqlBuffer.toString(), true, binds.toArray(new Object[0]));
					final IndexProvidingIterator<UIDMeta> iter = metaReader.iterateUIDMetas(rset, type);
					try {
						while(processStream(iter, def, queryContext)) {/* No Op */} 
					} finally {
						try { rset.close(); } catch (Exception x) {/* No Op */}
					}
				} catch (Exception ex) {
					log.error("Failed to execute find.\nSQL was [{}]", sqlBuffer, ex);
					def.accept(new Exception("Failed to execute find", ex));
				}				
			}
		});
		return stream;			
	}
	
	
	

	/**
	 * {@inheritDoc}
	 * @see java.lang.Thread.UncaughtExceptionHandler#uncaughtException(java.lang.Thread, java.lang.Throwable)
	 */
	@Override
	public void uncaughtException(final Thread t, final Throwable e) {
		log.error("Caught exception on thread [{}]", t, e);
	}

	
	/**
	 * Builds a stringy from the passed metric name and UID tags
	 * @param metric The metric name
	 * @param tags The UID tags
	 * @return a stringy
	 */
	static final CharSequence buildObjectName(final String metric, final List<UIDMeta> tags) {
		if(tags==null || tags.isEmpty()) throw new IllegalArgumentException("The passed tags map was null or empty");
		String mname = metric==null || metric.isEmpty() ? "*" : metric;
		StringBuilder b = stringBuilder.get().append(mname).append(":");
		boolean k = true;		
		for(final UIDMeta meta: tags) {
			b.append(meta.getName());
			if(k) {
				b.append("=");
			} else {
				b.append(",");
			}
			k = !k;
		}
		b.deleteCharAt(b.length()-1);
		return b.toString();		
	}

	
	static final CharSequence buildObjectName(final String metric, final Map<String, String> tags) {
		if(tags==null || tags.isEmpty()) throw new IllegalArgumentException("The passed tags map was null or empty");
		String mname = metric==null || metric.isEmpty() ? "*" : metric;
		StringBuilder b = stringBuilder.get().append(mname).append(":");
		for(final Map.Entry<String, String> e: tags.entrySet()) {
			b.append(e.getKey()).append("=").append(e.getValue()).append(",");
		}
		b.deleteCharAt(b.length()-1);
		return b.toString();
	}
	
	/** Fast string builder support */
	static final ThreadLocal<StringBuilder> stringBuilder = new ThreadLocal<StringBuilder>() {
		@Override
		protected StringBuilder initialValue() {
			return new StringBuilder(128);
		}
		@Override
		public StringBuilder get() {
			StringBuilder b = super.get();
			b.setLength(0);
			return b;
		}
	};

	
	
	/**
	 * Fills in some parameterized SQL with literals.
	 * This is not used for executing SQL but when debug is enabled,
	 * full parameter value SQL will be logged.
	 * @param sql The base SQL to modify
	 * @param binds The bind values to fill in
	 * @return the rewritten SQL
	 */
	public static String fillInSQL(String sql, final List<Object> binds) {
		final int bindCnt = binds.size();
		Matcher m = Q_PATTERN.matcher(sql);
		for(int i = 0; i < bindCnt; i++) {
			Object bind = binds.get(i);
			if(bind instanceof CharSequence) {
				sql = m.replaceFirst("'" + bind.toString() + "'");
			} else if (bind.getClass().isArray()) {
				sql = m.replaceFirst(renderArray(bind));
			} else {
				sql = m.replaceFirst(bind.toString());
			}		
			m = Q_PATTERN.matcher(sql);
		}
		return sql;
	}
	
	/**
	 * Processes the results into the stream
	 * @param iter The iterator of the results to stream out
	 * @param def The deferred the results are accepted into
	 * @param queryContext The current query context
	 * @return true to continue processing, false for done
	 */
	protected  <T> boolean processStream(final IndexProvidingIterator<T> iter , final Deferred<T, Stream<T>> def, final QueryContext queryContext) {
		int rowsRead = 0;
		final int pageRows = queryContext.getPageSize();		
		if(queryContext.isExpired()) {
			def.accept(new TimeoutException("Request Timed Out During Processing after [" + queryContext.getTimeout() + "] ms."));
			return false;
		}
		boolean exh = false;
		while(rowsRead < pageRows) {
			if(iter.hasNext()) {
				final T t = iter.next();
				if(t!=null) {
					def.accept(iter.next());
					rowsRead++;
				}
			} else {
				exh = true;
				break;
			}
		}
		
		if(!exh) {
			queryContext.setExhausted(false).setNextIndex(iter.getIndex()).incrementCummulative(rowsRead);
		} else {
			queryContext.setExhausted(true).setNextIndex(null).incrementCummulative(rowsRead);
		}		
		queryContext.addCtx("StreamFlushed", System.currentTimeMillis());
		log.debug("Deferred Flushing [{}] T rows", rowsRead);
		def.flush();
		final boolean cont = queryContext.shouldContinue();
		return cont;
//		if(cont && queryContext.isContinuous()) {
//			fjPool.submit(new Callable<Boolean>(){
//				@Override
//				public Boolean call() throws Exception {						
//					return processStream(iter , def, queryContext);
//				}
//			});
//			return false;
//		} else {
//			return cont;
//		}
		
	}
	
	/**
	 * Processes the results into the stream
	 * @param items The results to stream out
	 * @param def The deferred the results are accepted into
	 * @param queryContext The current query context
	 * @return true to continue processing, false for done
	 */
	protected  boolean processStream(List<TSMeta> items , final Deferred<TSMeta, Stream<TSMeta>> def, final QueryContext queryContext) {
		int rowsRead = 0;
		final int pageRows = queryContext.getPageSize();		
		if(queryContext.isExpired()) {
			def.accept(new TimeoutException("Request Timed Out During Processing after [" + queryContext.getTimeout() + "] ms."));
			return false;
		}
		boolean exh = false;
		Iterator<TSMeta> iter = items.iterator();
		while(rowsRead < pageRows) {
			if(iter.hasNext()) {
				def.accept(iter.next());
				rowsRead++;
			} else {
				exh = true;
				break;
			}
		}
		
		if(!exh) {
			if(iter.hasNext()) {
				queryContext.setExhausted(false).setNextIndex(iter.next().getTSUID()).incrementCummulative(rowsRead);
			} else {
				queryContext.setExhausted(true).setNextIndex(null).incrementCummulative(rowsRead);
			}
		} else {
			queryContext.setExhausted(true).setNextIndex(null).incrementCummulative(rowsRead);
		}		
		queryContext.addCtx("StreamFlushed", System.currentTimeMillis());
		log.debug("Deferred Flushing [{}] rows", rowsRead);
		def.flush();
		return queryContext.shouldContinue();
	}
	
	/**
	 * Generates a TSMeta query
	 * @param sqlBuffer The sql buffer to append generated SQL into
	 * @param binds The query bind values
	 * @param queryOptions The query options
	 * @param metricName The metric name
	 * @param tags The tag pairs
	 * @param mergeOp Specifies a row merge op (usually INTERSECT or UNION ALL)
	 * @param tsuid The optional TSMeta UID to match against
	 */
	protected void generateTSMetaSQL(final StringBuilder sqlBuffer, final List<Object> binds, final QueryContext queryOptions, final String metricName, final Map<String, String> tags, final String mergeOp, final String tsuid) {
		final boolean hasMetricName = (metricName==null || metricName.trim().isEmpty());
		final String tagColumn = queryOptions.isIncludeUIDs() ? ", Y.TAGS " : "";
		if(tags==null || tags.isEmpty()) {	
			if(tsuid!=null && !tsuid.trim().isEmpty()) {
				if(queryOptions.getNextIndex()==null) {							
					sqlBuffer.append(String.format(hasMetricName ? GET_TSMETAS_NO_TAGS_NAME_TSUID_SQL : GET_TSMETAS_NO_TAGS_NO_METRIC_NAME_TSUID_SQL, INITIAL_TSUID_START_SQL)); 
				} else {
					sqlBuffer.append(String.format(hasMetricName ? GET_TSMETAS_NO_TAGS_NAME_TSUID_SQL : GET_TSMETAS_NO_TAGS_NO_METRIC_NAME_TSUID_SQL, TSUID_START_SQL));
					binds.add(queryOptions.getNextIndex());
				}
				binds.add(tsuid);
			} else {
				if(queryOptions.getNextIndex()==null) {							
					sqlBuffer.append(String.format(hasMetricName ? GET_TSMETAS_NO_TAGS_NAME_SQL : GET_TSMETAS_NO_TAGS_NO_METRIC_NAME_SQL, INITIAL_TSUID_START_SQL)); 
				} else {
					sqlBuffer.append(String.format(hasMetricName ? GET_TSMETAS_NO_TAGS_NAME_SQL : GET_TSMETAS_NO_TAGS_NO_METRIC_NAME_SQL, TSUID_START_SQL));
					binds.add(queryOptions.getNextIndex());
				}
			}
			if(hasMetricName) binds.add(metricName);			
		} else {
			StringBuilder keySql = new StringBuilder("SELECT * FROM ( ");
			prepareGetTSMetasSQL(metricName, tags, binds, keySql, GET_TSMETAS_SQL, mergeOp);
			keySql.append(") X ");
			if(queryOptions.getNextIndex()==null) {
				keySql.append(" WHERE ").append(INITIAL_TSUID_START_SQL);
			} else {
				keySql.append(" WHERE ").append(TSUID_START_SQL);
				binds.add(queryOptions.getNextIndex());
			}
			if(tsuid!=null && !tsuid.trim().isEmpty()) {
				keySql.append("\n AND X.TSUID = ? \n");
				binds.add(tsuid);
			}
			
			keySql.append(" ORDER BY X.TSUID DESC LIMIT ? ");
			
			sqlBuffer.append(keySql.toString());
		}	
		
	}
		
	protected void doGetTSMetasSQLTagValues(final boolean firstEntry, final String metricName, Map.Entry<String, String> tag, final List<Object> binds, final StringBuilder sql, final String driverTsMetaSql, String mergeOp) {
		// // expandPredicate(entry.getKey(), TAGK_SQL_BLOCK, binds),
		if(!firstEntry) {
			sql.append("\n ").append(mergeOp).append("  \n");
			
		}
		
		sql.append(String.format(driverTsMetaSql,  
				expandPredicate(metricName, METRIC_SQL_BLOCK, binds),
				expandPredicate(tag.getKey(), TAGK_SQL_BLOCK, binds),
				expandPredicate(tag.getValue(), TAGV_SQL_BLOCK, binds)
		));
	}
	
	protected void prepareGetTSMetasSQL(final String metricName, final Map<String, String> tags, final List<Object> binds, final StringBuilder sql, final String driverTsMetaSql, String mergeOp) {
		Iterator<Map.Entry<String, String>> iter = tags.entrySet().iterator();
		doGetTSMetasSQLTagValues(true, metricName, iter.next(), binds, sql, driverTsMetaSql, mergeOp);
		while(iter.hasNext()) {
			doGetTSMetasSQLTagValues(false, metricName, iter.next(), binds, sql, driverTsMetaSql, mergeOp);
		}
	}
	
	

	
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
	
	/**
	 * Acquires the reactor streaming deferred for the current operation 
	 * @param priorDeferred The prior streaming deferred which is null if this is not a recursive call
	 * @return the reactor streaming deferred
	 */
	private <T> Deferred<T, Stream<T>> getDeferred(final Deferred<T, Stream<T>> priorDeferred, final QueryContext queryContext) {
		if(priorDeferred!=null) return priorDeferred;
		return Streams.<T>defer()
			.dispatcher(this.dispatcher)			
//			.batchSize(queryContext.getPageSize())
			.get();
	}
	
	
	/**
	 * Expands a SQL predicate for wildcards and multis
	 * @param value The value expression
	 * @param predicateBase The constant predicate base format
	 * @param binds The bind variable accumulator
	 * @return the expanded predicate
	 */
	public static final String expandPredicate(final String value, final String predicateBase, final List<Object> binds) {
		String nexpandedValue = value;
		final StringTokenizer st = new StringTokenizer(nexpandedValue.replace(" ", ""), "|", false);
		int segmentCount = st.countTokens();
		if(segmentCount<1) {
			throw new RuntimeException("Failed to parse expression [" + value + "]. Segment count was 0");
		}
		if(segmentCount==1) {
			String val = st.nextToken();
			binds.add(val.replace('*', '%'));
			//String pred = expandNumericRange(predicateBase.replace("%s", val.indexOf('*')==-1 ? "=" : "LIKE"));
			String pred = predicateBase.replace("%s", val.indexOf('*')==-1 ? "=" : "LIKE");
			return pred;
		}
		StringBuilder b = new StringBuilder();
		for(int i = 0; i < segmentCount; i++) {			
			if(i!=0) b.append(" OR ");
			String val = st.nextToken();
			binds.add(val.replace('*', '%'));
			b.append(
//					expandNumericRange(
								predicateBase.replace("%s", val.indexOf('*')==-1 ? "=" : "LIKE")
//					)
			);
		}
		return b.toString();				
	}

	/**
	 * Returns the data source
	 * @return the data source
	 */
	public DataSource getDataSource() {
		return dataSource.getDataSource();
	}

	/**
	 * Returns the SQLWorker
	 * @return the SQLWorker
	 */
	public SQLWorker getSqlWorker() {
		return sqlWorker;
	}

	/**
	 * Returns the service's fork join pool
	 * @return the service's fork join pool
	 */
	public ForkJoinPool getForkJoinPool() {
		return fjPool;
	}
	

}


/*
Query to retrieve TSMetas with tags
===================================
SELECT
Y.TAGS, X.*
FROM
(
   SELECT
   X.*
   FROM TSD_TSMETA X,
   TSD_METRIC M,
   TSD_FQN_TAGPAIR T,
   TSD_TAGPAIR P,
   TSD_TAGK K,
   TSD_TAGV V
   WHERE M.XUID = X.METRIC_UID
   AND X.FQNID = T.FQNID
   AND T.XUID = P.XUID
   AND P.TAGK = K.XUID
   AND (M.NAME LIKE 'mq.topic.subs.queue.%')
   AND P.TAGV = V.XUID
   AND (K.NAME = 'host')
   AND (V.NAME LIKE 'pdk-pt-cefmq-0%' OR V.NAME LIKE 'pdk-pt-cebmq-0%') INTERSECT
   SELECT
   X.*
   FROM TSD_TSMETA X,
   TSD_METRIC M,
   TSD_FQN_TAGPAIR T,
   TSD_TAGPAIR P,
   TSD_TAGK K,
   TSD_TAGV V
   WHERE M.XUID = X.METRIC_UID
   AND X.FQNID = T.FQNID
   AND T.XUID = P.XUID
   AND P.TAGK = K.XUID
   AND (M.NAME LIKE 'mq.topic.subs.queue.%')
   AND P.TAGV = V.XUID
   AND (K.NAME = 'app')
   AND (V.NAME LIKE '%')
)
X, (
	--SELECT json_object_keys(array_to_json(array_agg(P.NAME))) TAGKS, T.FQNID
	--SELECT array_to_json(array_agg(P.XUID), array_agg(P.NAME)) TAGS,  T.FQNID
	SELECT jsonb_agg(jsonb_build_object('kuid', K.XUID, 'vuid', V.XUID, 'kname', K.NAME, 'vname', V.NAME )) TAGS,  T.FQNID	
	FROM TSD_TSMETA X,
	TSD_FQN_TAGPAIR T,
   	TSD_TAGPAIR P,
   	TSD_TAGK K,
   	TSD_TAGV V
	WHERE T.XUID = P.XUID
	AND X.FQNID = T.FQNID
	AND P.TAGK = K.XUID
	AND P.TAGV = V.XUID
	GROUP BY T.FQNID
) Y
WHERE X.TSUID <= 'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF'
AND X.FQNID = Y.FQNID
ORDER BY X.TSUID DESC LIMIT 5001




import java.util.regex.*;

p = Pattern.compile("\\[(\\d+.*?\\d+)\\]");
x = Pattern.compile("(\\d+\\-\\d+|\\d+)");
v = "[1,3-5,7]";
m = p.matcher(v);
int cnt = 0;
println "Matches: ${m.matches()}";
m = x.matcher(m.group(1));
while(m.find()) {
    println m.group(1);
}


return null;

*/