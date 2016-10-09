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
import java.sql.ResultSet;
import java.util.Properties;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.heliosapm.streams.chronicle.TSDBMetricMeta;
import com.heliosapm.streams.common.metrics.SharedMetricsRegistry;
import com.heliosapm.streams.opentsdb.ringbuffer.RBWaitStrategy;
import com.heliosapm.streams.sqlbinder.SQLWorker;
import com.heliosapm.streams.sqlbinder.SQLWorker.ResultSetHandler;
import com.heliosapm.utils.collections.Props;
import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.jmx.JMXManagedThreadFactory;
import com.heliosapm.utils.time.SystemClock;
import com.heliosapm.utils.time.SystemClock.ElapsedTime;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
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
	/** Endto end Timer to track elapsed times from the rtpublisher callback to the dispatcher */
	protected final Timer endToEndTimer = SharedMetricsRegistry.getInstance().timer("endToEnd");
	/** A meter of incoming datapoints */
	protected final Meter dataPoints = SharedMetricsRegistry.getInstance().meter("incomingDataPoints");
	/** A meter of incoming datapoints */
	protected final Meter annotations = SharedMetricsRegistry.getInstance().meter("incomingAnnotations");
	
	/** Counter of uncaught exceptions in the dispatch handler */
	protected final Counter dispatchExceptions = SharedMetricsRegistry.getInstance().counter("dispatchExceptions");
	
	// ===============================================================================================
	//		Guava Cache Config
	// ===============================================================================================
	/** The tag key cache */
	protected Cache<String, String> tagKeyCache;
	/** The tag value cache */
	protected Cache<String, String> tagValueCache;
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
	
	/** The dispatch handler */
	protected final EventHandler<TSDBMetricMeta> dispatchHandler = new EventHandler<TSDBMetricMeta>() {
		@Override
		public void onEvent(final TSDBMetricMeta meta, final long sequence, final boolean endOfBatch) throws Exception {
			final Context ctx = dispatchHandlerTimer.time();
			try {
				// TODO
			} finally {
				meta.reset();
				ctx.stop();
			}
		}
	};
	
	
	@SuppressWarnings("unchecked")
	public ListenerMain(final Properties properties) {
		log.info(">>>>> Starting TSDB Event Listener...");
		
		// ================  Configure Tag Caches
		tagKeyCache = CacheBuilder.from(statEnableSpec(ConfigurationHelper.getSystemThenEnvProperty(CONFIG_TAGK_CACHE, DEFAULT_TAGK_CACHE, properties))).build();
		tagValueCache = CacheBuilder.from(statEnableSpec(ConfigurationHelper.getSystemThenEnvProperty(CONFIG_TAGV_CACHE, DEFAULT_TAGV_CACHE, properties))).build();
		tagPairCache = CacheBuilder.from(statEnableSpec(ConfigurationHelper.getSystemThenEnvProperty(CONFIG_TAGPAIR_CACHE, DEFAULT_TAGPAIR_CACHE, properties))).build();

		// ================  Configure Outbound Queue
		inQueueTextFormat = ConfigurationHelper.getBooleanSystemThenEnvProperty(CONFIG_INQ_TEXT, DEFAULT_INQ_TEXT, properties);
		inQueueDirName = ConfigurationHelper.getSystemThenEnvProperty(CONFIG_INQ_DIR, DEFAULT_INQ_DIR, properties);		
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
		dispatchRb = dispatchRbDisruptor.start();
		log.info("Started MetricDispatch RingBuffer");
		
		sqlWorker = DefaultDataSource.getInstance().getSQLWorker();
		
		
		log.info("<<<<< TSDB Event Listener Ready");
	}
	
	private static String statEnableSpec(final String spec) {
		if(spec.toLowerCase().indexOf("recordStats")==-1) {
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
	
	
	protected void asynchLoadCache(final String cacheName, final Cache<String, String> cache, final String loadingSql) {
		dispatchRbThreadFactory.newThread(new Runnable(){
			public void run() {
				final ElapsedTime et = SystemClock.startClock();
				sqlWorker.executeQuery(loadingSql, new ResultSetHandler(){
					@Override
					public boolean onRow(final int rowId, final ResultSet rset) {
						// TODO Auto-generated method stub
						return false;
					}
				});
			}
		}).start();
	}

	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
	}
	
	public void stop() {
	}

	
	public void run() {
		
	}
	
	
	public void start() {
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
