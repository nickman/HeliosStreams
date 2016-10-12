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
package com.heliosapm.streams.tsdb.listener;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.heliosapm.streams.chronicle.TSDBMetricMeta;
import com.heliosapm.streams.common.metrics.SharedMetricsRegistry;
import com.heliosapm.streams.opentsdb.ringbuffer.RBWaitStrategy;
import com.heliosapm.streams.sqlbinder.SQLWorker;
import com.heliosapm.streams.sqlbinder.SQLWorker.ResultSetRowDataHandler;
import com.heliosapm.streams.tracing.TagKeySorter;
import com.heliosapm.utils.collections.Props;
import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.jmx.JMXManagedThreadFactory;
import com.heliosapm.utils.lang.StringHelper;
import com.heliosapm.utils.time.SystemClock;
import com.heliosapm.utils.time.SystemClock.ElapsedTime;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.threads.LongPauser;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.WireType;

/**
 * <p>Title: ListenerMain</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tsdb.listener.ListenerMain</code></p>
 */

public class ListenerMain implements Closeable, Runnable {
	/** The number of processors */
	public static final int CORES = Runtime.getRuntime().availableProcessors();

	/** The config key name for the number of disruptor threads to run in the meta dispatch ringbuffer */
	public static final String CONFIG_DISPATCHRB_THREADS = "listener.dispatch.rb.threads";
	/** The default number of disruptor threads to run in the meta dispatch ringbuffer */
	public static final int DEFAULT_DISPATCHRB_THREADS = CORES;

	/** The config key name for the dispatch ringbuffer size */
	public static final String CONFIG_DISPATCHRB_SIZE = "listener.dispatch.rb.size";
	/** The default dispatch ringbuffer size */
	public static final int DEFAULT_DISPATCHRB_SIZE = 2048;
	
	/** The config key name for the dispatch ringbuffer wait strategy */
	public static final String CONFIG_DISPATCHRB_WAITSTRAT = "listener.dispatch.rb.waitstrat";
	/** The default dispatch ringbuffer wait strategy */
	public static final RBWaitStrategy DEFAULT_DISPATCHRB_WAITSTRAT = RBWaitStrategy.SLEEP;
	/** The config key prefix for the dispatch ringbuffer wait strategy config properties */
	public static final String CONFIG_DISPATCHRB_WAITSTRAT_PROPS = DEFAULT_DISPATCHRB_WAITSTRAT + ".";

	/** The mandatory TSUID cache db file extension */
	public static final String CACHE_DBFILE_EXT = ".db";
	
	/** The config key name for the outbound queue base directory */
	public static final String CONFIG_INQ_DIR = "eventpublisher.inq.dir";
	/** The default outbound queue base directory */
	public static final String DEFAULT_INQ_DIR = new File(System.getProperty("user.home"), ".eventpublisher").getAbsolutePath();

	/** The config key name for the out queue's roll cycle */
	public static final String CONFIG_INQ_ROLLCYCLE = "eventpublisher.inq.rollcycle";
	/** The default out queue roll cycle */
	public static final RollCycles DEFAULT_INQ_ROLLCYCLE = RollCycles.HOURLY;
	
	/** The config key name for the out queue's data format (true for TEXT, false for BINARY) */
	public static final String CONFIG_INQ_TEXT = "eventpublisher.inq.text";
	/** The default out queue data format setting (BINARY) */
	public static final boolean DEFAULT_INQ_TEXT = false;
	
	/** The config key name for the tag key pk lookup cache spec */
	public static final String CONFIG_TAGK_CACHE = "listener.cachespec.tagk";
	/** The default tag key pk lookup cache spec */
	public static final String DEFAULT_TAGK_CACHE = String.format("concurrencyLevel=%s,initialCapacity=%s,maximumSize=%s,recordStats", 
			CORES, 2048, 1048 * 5
	); 
	
	/** The config key name for the tag value pk lookup cache spec */
	public static final String CONFIG_TAGV_CACHE = "listener.cachespec.tagv";
	/** The default tag value pk lookup cache spec */
	public static final String DEFAULT_TAGV_CACHE = String.format("concurrencyLevel=%s,initialCapacity=%s,maximumSize=%s,recordStats", 
			CORES, 2048, 1048 * 5
	);
	
	/** The config key name for the metric name pk lookup cache spec */
	public static final String CONFIG_METRIC_CACHE = "listener.cachespec.metric";
	/** The default metric name pk lookup cache spec */
	public static final String DEFAULT_METRIC_CACHE = String.format("concurrencyLevel=%s,initialCapacity=%s,maximumSize=%s,recordStats", 
			CORES, 2048, 1048 * 5
	); 
	
	
	/** The config key name for the tag pair pk lookup cache spec */
	public static final String CONFIG_TAGPAIR_CACHE = "listener.cachespec.tagpair";
	/** The default tag pair pk lookup cache spec */
	public static final String DEFAULT_TAGPAIR_CACHE = String.format("concurrencyLevel=%s,initialCapacity=%s,maximumSize=%s,recordStats", 
			CORES, 2048, 1048 * 5
	); 

	/** Flag indicating if the OS is windows, meaning rolled files will not immediately be able to be deleted */
	public static final boolean IS_WIN = System.getProperty("os.name").toLowerCase().contains("windows");
	
	/** The SQLWorker providing JDBC services to save meta to the DB */
	protected final SQLWorker sqlWorker;
	/** The Datasource providing connections to save meta to the DB */
	protected final DefaultDataSource ds;
	
	
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** the thread group the threadpool threads belong to */
	final ThreadGroup tg = new ThreadGroup("MessageListGroup");
	/** Threads running flag */
	final AtomicBoolean running = new AtomicBoolean(false);
	/** Indicates if the in queue is using text format (true) or binary (false) */
	protected boolean inQueueTextFormat = false;
	/** Timer to track elapsed times on executing the dispatch handler */
	protected final Timer dispatchHandlerTimer = SharedMetricsRegistry.getInstance().timer("dispatchHandler");
//	/** Endto end Timer to track elapsed times from the rtpublisher callback to the dispatcher */
//	protected final Timer endToEndTimer = SharedMetricsRegistry.getInstance().timer("endToEnd");
	/** A meter of incoming datapoints */
	protected final Meter dataPoints = SharedMetricsRegistry.getInstance().meter("incomingDataPoints");
	/** A meter of incoming datapoints */
	protected final Meter annotations = SharedMetricsRegistry.getInstance().meter("incomingAnnotations");
	
	/** Counter of uncaught exceptions in the dispatch handler */
	protected final Counter dispatchExceptions = SharedMetricsRegistry.getInstance().counter("dispatchExceptions");
	
	/** The run thread that reads from the inbound queue and writes to the ring-buffer */
	protected Thread runThread = null;
	/** Counts the number of of in queue events in each batch */
	protected final LongAdder batchCounter = new LongAdder();
	
	
	// ===============================================================================================
	//		Guava Cache Config
	// ===============================================================================================
	/** The tag key cache */
	protected Cache<String, String> tagKeyCache;
	/** The tag value cache */
	protected Cache<String, String> tagValueCache;
	/** The metric name cache */
	protected Cache<String, String> tagMetricCache;	
	/** The tag pair cache */
	protected Cache<String, String> tagPairCache;
	
	
	// ===============================================================================================
	//		In Queue Config
	// ===============================================================================================
	/** The queue base directory name */
	protected String inQueueDirName = null;
	/** The queue base directory */
	protected File inQueueDir = null;	
	/** The queue roll cycle */
	protected RollCycle inQueueRollCycle = null;
	/** The queue */
	protected ChronicleQueue inQueue = null;
	/** The in queue tailer */
	protected ExcerptTailer tailer = null;
	
	// ===============================================================================================
	//		RingBuffer Config
	// ===============================================================================================
	
	/** The number of threads to allocate to the dispatch ring buffer */
	protected int dispatchRbThreads = -1;
	/** The number of slots in the dispatch ring buffer */
	protected int dispatchRbSize = -1;
	/** The dispatch ring buffer wait strategy */
	protected RBWaitStrategy dispatchRbWaitStrat = null;
	
	/** The dispatch ring buffer thread factory */
	protected ThreadFactory dispatchRbThreadFactory = JMXManagedThreadFactory.newThreadFactory("MetricListenerThread", false); 
	
	/** The dispatch disruptor */
	protected Disruptor<TSDBMetricMeta> dispatchRbDisruptor = null;
	/** The dispatch ring buffer */
	protected RingBuffer<TSDBMetricMeta> dispatchRb = null;
	
	// ===============================================================================================
	//		RingBuffer Handlers
	// ===============================================================================================
	
	/** The dispatch exception handler */
	protected final ExceptionHandler<TSDBMetricMeta> dispatchExceptionHandler = new ExceptionHandler<TSDBMetricMeta>() {
		
		@Override
		public void handleEventException(final Throwable ex, final long sequence, final TSDBMetricMeta event) {
			dispatchExceptions.inc();
			log.error("Dispatch exception on meta {}", event, ex);
		}

		@Override
		public void handleOnStartException(final Throwable ex) {
			dispatchExceptions.inc();
			log.error("Dispatch exception on start", ex);			
		}

		@Override
		public void handleOnShutdownException(final Throwable ex) {
			dispatchExceptions.inc();
			log.error("Dispatch exception on shutdown", ex);			
		}
	};
	
	// FQN_SEQ.NEXTVAL
	
	public static final String TSMETA_INSERT_SQL =
		"KEYS:INSERT INTO TSD_TSMETA " +
		"(METRIC_UID,FQN,TSUID) VALUES " +
		"(?,?,?) ";
	public static final String TSMETA_COUNT_SQL =
			"SELECT COUNT(*) FROM TSD_TSMETA " +
			"WHERE FQN = ? " + 
			"AND TSUID = ?";
	
	public static final String FQN_TAGPAIR_INSERT_SQL =
			"INSERT INTO TSD_FQN_TAGPAIR " +
			"(FQNID,XUID,PORDER,NODE) VALUES " +
			"(?,?,?,?) ";
	
	
//	CREATE TABLE IF NOT EXISTS TSD_FQN_TAGPAIR (
//			FQN_TP_ID BIGINT NOT NULL COMMENT 'Synthetic primary key of an association between an FQN and a Tag Pair',
//			FQNID BIGINT NOT NULL COMMENT 'The ID of the parent FQN',
//			XUID CHAR(12) NOT NULL COMMENT 'The ID of a child tag key/value pair',
//			PORDER TINYINT NOT NULL COMMENT 'The order of the tags in the FQN',
//			NODE CHAR(1) NOT NULL COMMENT 'Indicates if this tagpair is a Branch (B) or a Leaf (L)' CHECK NODE IN ('B', 'L')
//		); COMMENT ON TABLE TSD_FQN_TAGPAIR IS 'Associative table between TSD_TSMETA and TSD_TAGPAIR, or the TSMeta and the Tag keys and values of the UIDMetas therein';

		
	private long lastBatchEnd = 0L; 
	
	/** The dispatch handler */
	protected final EventHandler<TSDBMetricMeta> dispatchHandler = new EventHandler<TSDBMetricMeta>() {
		final AtomicInteger concurrency = new AtomicInteger(0);
		final Gauge<Integer> concurrencyGauge = SharedMetricsRegistry.getInstance().gauge("concurrency", new Callable<Integer>(){
			@Override
			public Integer call() throws Exception {				
				return concurrency.get();
			}
		});
		@Override
		public void onEvent(final TSDBMetricMeta meta, final long sequence, final boolean endOfBatch) throws Exception {
//			log.info("Processing TSDB Metric: {}/{}.", sequence, endOfBatch);
			concurrency.incrementAndGet();
			try {
				final Context ctx = dispatchHandlerTimer.time();
				Connection conn = null;
				PreparedStatement ps = null;
				try {
					if(endOfBatch) {
						final long batchSize = sequence - lastBatchEnd;
						lastBatchEnd = sequence;
						log.info("Processing TSDB Metric: {}/{}. Batch Size: {}", sequence, endOfBatch, batchSize);
					}				
					conn = ds.getConnection();
					final String metricUid = tagMetricCache.get(meta.getMetricName(), uIDLoader("TSD_METRIC", meta.getMetricName(), meta.getMetricUid(), conn));
					final Map<String, String> tagUids = new TreeMap<String, String>(TagKeySorter.INSTANCE);
					
					for(Map.Entry<String, String> tag: meta.getTags().entrySet()) {
						final String tagKey = tag.getKey();
						final String tagValue = tag.getValue();
						final String tagKeyUid = meta.getTagKeyUids().get(tagKey);
						final String tagValueUid = meta.getTagValueUids().get(tagValue);
						final String tagPairUid = tagPairCache.get(tagKey + "=" + tagValue, 
								tagPairLoader(tagKey, tagKeyUid, tagValue, tagValueUid, conn)
							);
	 
						tagUids.put(tagKey, tagPairUid);
					}
					final String fqn = meta.getMetricName() + ":" + meta.getTags();
					final String tsuid = StringHelper.bytesToHex(meta.getTsuid());
					if(sqlWorker.sqlForInt(conn, TSMETA_COUNT_SQL, 0, fqn, tsuid)==0) {
						final Object[][] keys = sqlWorker.execute(conn, TSMETA_INSERT_SQL, metricUid, fqn, tsuid);
						final long tsuidKey = ((Number)keys[0][0]).longValue();
						int porder = 1;
						final int last = tagUids.size();
						ps = null;
						for(final String tagPairUid: tagUids.values()) {
							// FQNID,XUID,PORDER,NODE
							ps = sqlWorker.batch(conn, ps, FQN_TAGPAIR_INSERT_SQL, tsuidKey, tagPairUid, porder, porder==last ? "B" : "L");
						}
						ps.executeBatch();
					}
					
					// insert fqn tagpairs
					conn.commit();
				} finally {
					if(ps!=null) try { ps.close(); } catch (Exception x) {/* No Op */}
					if(conn!=null) try { conn.close(); } catch (Exception x) {/* No Op */}
					meta.reset();
					ctx.stop();
				}
			} finally {
				concurrency.decrementAndGet();
			}
		}
	};
	
	
	@SuppressWarnings("unchecked")
	public ListenerMain(final Properties properties) {
		log.info(">>>>> Starting TSDB Event Listener...");
		
		// ================  Configure Tag Caches		
		tagKeyCache = CacheBuilder.from(statEnableSpec(ConfigurationHelper.getSystemThenEnvProperty(CONFIG_TAGK_CACHE, DEFAULT_TAGK_CACHE, properties))).build();
		tagValueCache = CacheBuilder.from(statEnableSpec(ConfigurationHelper.getSystemThenEnvProperty(CONFIG_TAGV_CACHE, DEFAULT_TAGV_CACHE, properties))).build();
		tagMetricCache = CacheBuilder.from(statEnableSpec(ConfigurationHelper.getSystemThenEnvProperty(CONFIG_METRIC_CACHE, DEFAULT_METRIC_CACHE, properties))).build();
		tagPairCache = CacheBuilder.from(statEnableSpec(ConfigurationHelper.getSystemThenEnvProperty(CONFIG_TAGPAIR_CACHE, DEFAULT_TAGPAIR_CACHE, properties))).build();

		// ================  Configure Outbound Queue
		inQueueTextFormat = ConfigurationHelper.getBooleanSystemThenEnvProperty(CONFIG_INQ_TEXT, DEFAULT_INQ_TEXT, properties);
		inQueueDirName = ConfigurationHelper.getSystemThenEnvProperty(CONFIG_INQ_DIR, DEFAULT_INQ_DIR, properties);
		log.info("InQueue Directory: [{}]", inQueueDirName);
		inQueueDir = new File(inQueueDirName);
		
		
		while(!isQueueFileReady()) {
			log.info("No cq4 in queue files found in [{}]. Waiting....", inQueueDir);
			long loops = 0;
			while(!isQueueFileReady()) {
				loops++;
				if(loops%60==0) {
					log.info("No cq4 in queue files found in [{}]. Waiting....", inQueueDir);
				}
				SystemClock.sleep(1000);
			}
			break;
		}
		
		inQueueRollCycle = ConfigurationHelper.getEnumSystemThenEnvProperty(RollCycles.class, CONFIG_INQ_ROLLCYCLE, DEFAULT_INQ_ROLLCYCLE, properties); 
		inQueue = SingleChronicleQueueBuilder.binary(inQueueDir)
				.rollCycle(inQueueRollCycle)
				.wireType(inQueueTextFormat ? WireType.JSON : WireType.BINARY)
				.build();
		
		dispatchRbThreads = ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_DISPATCHRB_THREADS, DEFAULT_DISPATCHRB_THREADS, properties);
		dispatchRbSize = ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_DISPATCHRB_SIZE, DEFAULT_DISPATCHRB_SIZE, properties); 
		final Properties dispatchRbWaitStratConfig = Props.extract(CONFIG_DISPATCHRB_WAITSTRAT_PROPS, properties, true, false);
		dispatchRbWaitStrat = ConfigurationHelper.getEnumSystemThenEnvProperty(RBWaitStrategy.class, CONFIG_DISPATCHRB_WAITSTRAT, DEFAULT_DISPATCHRB_WAITSTRAT, properties);
		dispatchRbDisruptor = new Disruptor<TSDBMetricMeta>(TSDBMetricMeta.FACTORY, dispatchRbSize, dispatchRbThreadFactory, ProducerType.SINGLE, dispatchRbWaitStrat.waitStrategy(dispatchRbWaitStratConfig));
		dispatchRbDisruptor.setDefaultExceptionHandler(dispatchExceptionHandler);
		dispatchRbDisruptor.handleEventsWith(dispatchHandler);		
		log.info("Started MetricDispatch RingBuffer");
		
		ds = DefaultDataSource.getInstance();
		sqlWorker = SQLWorker.getInstance(ds.getDataSource());
		
		final CountDownLatch keyLoadLatch = asynchLoadUIDCache("tagKeyCache", tagKeyCache, "SELECT XUID, NAME FROM TSD_TAGK");
		final CountDownLatch valueLoadLatch = asynchLoadUIDCache("tagValueCache", tagValueCache, "SELECT XUID, NAME FROM TSD_TAGV");
		asynchLoadUIDCache("tagMetricCache", tagMetricCache, "SELECT XUID, NAME FROM TSD_METRIC");
		asynchLoadUIDCache("tagPairCache", tagPairCache, "SELECT XUID, NAME FROM TSD_TAGPAIR", keyLoadLatch, valueLoadLatch);
		
		
		log.info("<<<<< TSDB Event Listener Ready");
	}
	
	public void start() {
		log.info(">>>>> Starting Listener....");
		runThread = new Thread(this, "RunThread");
		runThread.setDaemon(true);
		running.set(true);
		dispatchRb = dispatchRbDisruptor.start();
		runThread.start();
		
		
		
		
		log.info("<<<<< Listener Started.");
	}
	
	
	private static String statEnableSpec(final String spec) {
		if(spec.indexOf("recordStats")==-1) {
			return spec + "," + "recordStats";
		}
		return spec;
	}

	/**
	 * Indicates if any cq4 files are found in the in queue directory
	 * @return true if found, false otherwise
	 */
	protected boolean isQueueFileReady() {
		return inQueueDir.listFiles(f -> f.getName().endsWith(".cq4")).length > 0;
	}
	
	
	/**
	 * Loads the passed cache from the DB at startup
	 * @param cacheName The cache name
	 * @param cache The cache instance
	 * @param loadingSql The SQL for which the results will be used to populate the cache
	 * @param latches An optional array of latches to wait on before starting the load
	 * @return a completion latch on this load
	 */
	protected CountDownLatch asynchLoadUIDCache(final String cacheName, final Cache<String, String> cache, final String loadingSql, final CountDownLatch...latches) {
		final CountDownLatch latch = new CountDownLatch(1);
		dispatchRbThreadFactory.newThread(new Runnable(){
			public void run() {
				if(latches!=null && latches.length > 0) {
					for(CountDownLatch cdl: latches) {
						try {
							cdl.await();
						} catch (Exception ex) {
							log.error("Failed waiting on latches for cache [{}]", cacheName, ex);
						}
					}
					log.info("Finished waiting on latches. Starting to load cache [{}]", cacheName);
				}
				log.info("Loading Cache [{}]", cacheName);
				final ElapsedTime et = SystemClock.startClock();
				final int[] rowCount = new int[1];
				sqlWorker.executeQuery(loadingSql, new ResultSetRowDataHandler(){
					@Override
					public boolean onRow(final int rowId, final int colCount, final Object...rowData) {
						cache.put((String)rowData[1], (String)rowData[0]);
						rowCount[0] = rowId;
						return true;
					}
				});
				log.info("Loaded {} rows into {} cache: {}", rowCount[0], cacheName, et.printAvg("Rows", rowCount[0]));
				latch.countDown();
			}
			
		}).start();
		return latch;
	}
	
	
	// ===============================================================================================
	//		Guava Cache Loader Callables
	// ===============================================================================================
	/** The tag cache loader  */
	protected Callable<String> uIDLoader(final String tableName, final String name, final String xuid, final Connection conn) {
		final String SELECT_SQL = String.format("SELECT XUID FROM %s WHERE NAME = ?", tableName);
		final String INSERT_SQL = String.format("INSERT INTO %s (XUID, VERSION, NAME, CREATED) VALUES(?,?,?,?)", tableName);		
		return new Callable<String>() {
			@Override
			public String call() throws Exception {
				if(sqlWorker.sqlForString(conn, null, SELECT_SQL, name)==null) {
					sqlWorker.execute(conn, INSERT_SQL, xuid, 1, name, System.currentTimeMillis());
				}
				return xuid;
			}
		};
	}

	
	
	/** The tag pair loader callable */
	protected Callable<String> tagPairLoader(final String tagKey, final String tagKeyUid, final String tagValue, final String tagValueUid, final Connection conn) {
		final String SELECT_SQL = "SELECT XUID FROM TSD_TAGPAIR WHERE TAGK = ? AND TAGV = ?";
		final String INSERT_SQL = "INSERT INTO TSD_TAGPAIR (XUID, TAGK, TAGV, NAME) VALUES(?,?,?,?)";		
		return new Callable<String>() {
			@Override
			public String call() throws Exception {
				final String name = StringHelper.fastConcat(tagKey, "=", tagValue);
				final String xuid = StringHelper.fastConcat(tagKeyUid, tagValueUid);				
				if(sqlWorker.sqlForString(conn, null, SELECT_SQL, tagKeyUid, tagValueUid)==null) {
					tagKeyCache.get(tagKey, uIDLoader("TSD_TAGK", tagKey, tagKeyUid, conn));
					tagValueCache.get(tagValue, uIDLoader("TSD_TAGV", tagValue, tagValueUid, conn));
					sqlWorker.execute(conn, INSERT_SQL, xuid, tagKeyUid, tagValueUid, name);
				}
				return xuid;
			}
		};
	}

	
	

	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			System.setProperty(DefaultDataSource.CONFIG_DS_CLASS, "org.postgresql.ds.PGSimpleDataSource");
//			System.setProperty(DefaultDataSource.CONFIG_DS_CLASS, "org.postgresql.Driver");
			System.setProperty(DefaultDataSource.CONFIG_DS_URL, "jdbc:postgresql://localhost:5432/tsdb");
			System.setProperty(DefaultDataSource.CONFIG_DS_USER, "tsdb");
			System.setProperty(DefaultDataSource.CONFIG_DS_PW, "tsdb");
			System.setProperty(DefaultDataSource.CONFIG_DS_TESTSQL, "SELECT current_timestamp");
			
			
			
			final ListenerMain lm = new ListenerMain(new Properties());
			lm.start();
			final SharedMetricsRegistry smr = SharedMetricsRegistry.getInstance();
//			smr.startConsoleReporter(5L, System.err);
			StdInCommandHandler.getInstance()
				.registerCommand("stop", new Runnable(){
					public void run() {
						lm.stop();
						System.exit(0);
					}
				})
				.registerCommand("conrep", new Runnable(){
					public void run() {
						if(smr.isConsoleReporting()) {
							System.err.println("Stopping ConsoleReporter");
							smr.stopConsoleReporter();
						} else {
							System.err.println("Starting ConsoleReporter");
							smr.startConsoleReporter(5L, System.err);
						}
					}
				})				
			.run();
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(-1);
		}	
	}
		
	
	public void stop() {
		log.info(">>>>> Stopping EventListener....");
		running.set(false);
		if(runThread!=null) {
			runThread.interrupt();
		}
		try { 
			inQueue.close();
			log.info("InboundQueue Closed");
		} catch (Exception ex) {
			log.warn("Error closing InboundQueue: {}", ex);
		}		
		stopDisruptor("Dispatch", dispatchRbDisruptor);		
		log.info("<<<<< EventListener Stopped.");
		
	}
	
	/**
	 * Executes a controlled shutdown of the passed disruptor
	 * @param name The name of the disruptor for logging
	 * @param disruptor The disruptor to stop
	 */
	protected void stopDisruptor(final String name, final Disruptor<?> disruptor) {
		final long start = System.currentTimeMillis();
		try {			
			disruptor.shutdown(15000, TimeUnit.MILLISECONDS);
			final long elapsed = System.currentTimeMillis() - start;
			log.info("{} Disruptor stopped normally in {} ms.", name, elapsed);
		} catch (TimeoutException tex) {
			final long elapsed = System.currentTimeMillis() - start;
			log.warn("{} Disruptor failed to stop normally after {} ms. Halting....", name, elapsed);
			disruptor.halt();
			log.warn("{} Disruptor halted", name);
		}
	}
	

	
	/**
	 * <p>Reads from the in queue and writes the read event to the ring-buffer for processing</p>
	 * {@inheritDoc}
	 * @see java.lang.Runnable#run()
	 */
	public void run() {
		final Pauser pauser = new LongPauser(5, 5, 10, 100, TimeUnit.MILLISECONDS);     
		tailer = inQueue.createTailer();		
		final TSDBMetricMeta meta = TSDBMetricMeta.FACTORY.newInstance();
		while(running.get()) {
			try {
				boolean reset = false;
				tailer = inQueue.createTailer();
				while(running.get() && tailer.readBytes(meta.reset())) {					
					if(meta.isProcessed()) continue;
					meta.index(tailer.index());					
					if(!reset) {
						reset = true;
						pauser.reset();
					}
					long sequence = dispatchRb.next();
					TSDBMetricMeta event = dispatchRb.get(sequence);
					event.load(meta).resolved(meta);
					dispatchRb.publish(sequence);
				}				
				pauser.pause();
			} catch (Exception ex) {
				log.error("RunThread Error", ex);
			}
		}
		log.info("Run Thread Ended");
	}		
	
	


	/**
	 * {@inheritDoc}
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	

}
