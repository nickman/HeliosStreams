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
package com.heliosapm.streams.opentsdb;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.heliosapm.streams.chronicle.MessageType;
import com.heliosapm.streams.chronicle.TSDBMetricMeta;
import com.heliosapm.streams.opentsdb.plugin.PluginMetricManager;
import com.heliosapm.streams.opentsdb.ringbuffer.RBWaitStrategy;
import com.heliosapm.utils.collections.Props;
import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.jmx.JMXManagedThreadFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.openhft.chronicle.bytes.BytesRingBufferStats;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.set.ChronicleSet;
import net.openhft.chronicle.set.ChronicleSetBuilder;
import net.openhft.chronicle.wire.WireType;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.tsd.RTPublisher;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.Config;

/**
 * <p>Title: TSDBChronicleEventPublisher</p>
 * <p>Description: Publishes events to a chronicle out queue and accepts TSUID resolution requests
 * via an in chronicle queue.</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.opentsdb.TSDBChronicleEventPublisher</code></p>
 */

public class TSDBChronicleEventPublisher extends RTPublisher implements TSDBChronicleEventPublisherMBean, StoreFileListener, Consumer<BytesRingBufferStats> {
	
	/** The number of processors */
	public static final int CORES = Runtime.getRuntime().availableProcessors();
	
	/** The config key name for the number of disruptor threads to run in the cache lookup ringbuffer */
	public static final String CONFIG_CACHERB_THREADS = "eventpublisher.cache.rb.threads";
	/** The default number of disruptor threads to run in the cache lookup ringbuffer */
	public static final int DEFAULT_CACHERB_THREADS = CORES;

	/** The config key name for the number of disruptor threads to run in the meta dispatch ringbuffer */
	public static final String CONFIG_DISPATCHRB_THREADS = "eventpublisher.dispatch.rb.threads";
	/** The default number of disruptor threads to run in the meta dispatch ringbuffer */
	public static final int DEFAULT_DISPATCHRB_THREADS = CORES;

	/** The config key name for the cache lookup ringbuffer size */
	public static final String CONFIG_CACHERB_SIZE = "eventpublisher.cache.rb.size";
	/** The default cache lookup ringbuffer size */
	public static final int DEFAULT_CACHERB_SIZE = 2048;

	/** The config key name for the dispatch ringbuffer size */
	public static final String CONFIG_DISPATCHRB_SIZE = "eventpublisher.dispatch.rb.size";
	/** The default dispatch ringbuffer size */
	public static final int DEFAULT_DISPATCHRB_SIZE = 2048;
	
	/** The config key name for the cache lookup ringbuffer wait strategy */
	public static final String CONFIG_CACHERB_WAITSTRAT = "eventpublisher.cache.rb.waitstrat";
	/** The default cache lookup ringbuffer wait strategy */
	public static final RBWaitStrategy DEFAULT_CACHERB_WAITSTRAT = RBWaitStrategy.SLEEP;
	/** The config key prefix for the cache lookup ringbuffer wait strategy config properties */
	public static final String CONFIG_CACHERB_WAITSTRAT_PROPS = DEFAULT_CACHERB_WAITSTRAT + ".";
	
	/** The config key name for the dispatch ringbuffer wait strategy */
	public static final String CONFIG_DISPATCHRB_WAITSTRAT = "eventpublisher.dispatch.rb.waitstrat";
	/** The default dispatch ringbuffer wait strategy */
	public static final RBWaitStrategy DEFAULT_DISPATCHRB_WAITSTRAT = RBWaitStrategy.SLEEP;
	/** The config key prefix for the dispatch ringbuffer wait strategy config properties */
	public static final String CONFIG_DISPATCHRB_WAITSTRAT_PROPS = DEFAULT_DISPATCHRB_WAITSTRAT + ".";

	
	/** The config key name for the lookup cache persistent file */
	public static final String CONFIG_CACHE_FILE = "eventpublisher.cache.file";
	/** The default lookup cache persistent file */
	public static final String DEFAULT_CACHE_FILE = new File(new File(System.getProperty("user.home"), ".eventpublisher"), "lookupCache.db").getAbsolutePath();

	/** The config key name for the lookup cache average key size in bytes */
	public static final String CONFIG_CACHE_AVGKEYSIZE = "eventpublisher.cache.keysize";
	/** The default lookup cache average key size in bytes */
	public static final int DEFAULT_CACHE_AVGKEYSIZE = 275;
	
	/** The config key name for the lookup cache maximum number of entries */
	public static final String CONFIG_CACHE_MAXKEYS = "eventpublisher.cache.maxsize";
	/** The default lookup cache maximum number of entries */
	public static final long DEFAULT_CACHE_MAXKEYS = 100000;
	
	/** The config key name for the outbound queue base directory */
	public static final String CONFIG_OUTQ_DIR = "eventpublisher.outq.dir";
	/** The default outbound queue base directory */
	public static final String DEFAULT_OUTQ_DIR = new File(System.getProperty("user.home"), ".eventpublisher").getAbsolutePath();

	/** The config key name for the out queue's block size */
	public static final String CONFIG_OUTQ_BLOCKSIZE = "eventpublisher.outq.blocksize";
	/** The default out queue block size */
	public static final int DEFAULT_OUTQ_BLOCKSIZE = 1296 * 1024;

	/** The config key name for the out queue's roll cycle */
	public static final String CONFIG_OUTQ_ROLLCYCLE = "eventpublisher.outq.rollcycle";
	/** The default out queue roll cycle */
	public static final RollCycles DEFAULT_OUTQ_ROLLCYCLE = RollCycles.HOURLY;
	
	/** The config key name for the out queue's data format (true for TEXT, false for BINARY) */
	public static final String CONFIG_OUTQ_TEXT = "eventpublisher.outq.text";
	/** The default out queue data format setting (BINARY) */
	public static final boolean DEFAULT_OUTQ_TEXT = false;
	

	/** Flag indicating if the OS is windows, meaning rolled files will not immediately be able to be deleted */
	public static final boolean IS_WIN = System.getProperty("os.name").toLowerCase().contains("windows");
	
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** The metric manager for this plugin */
	protected final PluginMetricManager metricManager = new PluginMetricManager(getClass().getSimpleName());	
	/** The parent TSDB instance */
	protected TSDB tsdb = null;
	/** Timer to track elapsed times on uid resolutions called when the cacheDB does not contain an incoming tsuid */
	protected final Timer resolveUidsTimer = metricManager.timer("resolveUids");
	/** Timer to track elapsed times on TSMeta retrievals */
	protected final Timer getTSMetaTimer = metricManager.timer("getTSMeta");
	
	/** Indicates if the out queue is using text format (true) or binary (false) */
	protected boolean outQueueTextFormat = false;
	
	/** Timer to track elapsed times on executing the cache lookup handler */
	protected final Timer cacheLookupHandlerTimer = metricManager.timer("cacheLookupHandler");
	/** Timer to track elapsed times on executing the dispatch handler */
	protected final Timer dispatchHandlerTimer = metricManager.timer("dispatchHandler");
	/** Endto end Timer to track elapsed times from the rtpublisher callback to the dispatcher */
	protected final Timer endToEndTimer = metricManager.timer("endToEnd");
	
	/** Counter of uncaught exceptions in the cacheLookUp handler */
	protected final Counter cacheLookupExceptions = metricManager.counter("cacheLookupExceptions");
	/** Counter of uncaught exceptions in the dispatch handler */
	protected final Counter dispatchExceptions = metricManager.counter("dispatchExceptions");
	
	
	/** The number of bytes in a metric uid */
	protected short metrics_width = -1;
	
	// ===============================================================================================
	//		Out Queue Config
	// ===============================================================================================
	/** The queue base directory name */
	protected String outQueueDirName = null;
	/** The queue base directory */
	protected File outQueueDir = null;	
	/** The queue block size */
	protected int outQueueBlockSize = -1;
	/** The queue roll cycle */
	protected RollCycle outQueueRollCycle = null;
	/** The queue */
	protected ChronicleQueue outQueue = null;
	/** A set of undeleted rolled queue files if we're on windows, otherwise null */
	protected final NonBlockingHashMap<String, File> pendingDeletes = IS_WIN ? new NonBlockingHashMap<String, File>() : null;	
	/** A counter of rolled queue files */
	protected final Counter rolledFiles = metricManager.counter("rolledFiles");
	/** A counter of deleted rolled queue files */
	protected final Counter deletedRolledFiles = metricManager.counter("deletedRolledFiles");
	/** A counter of rolled queue files pending deletion */
	protected final Counter pendingRolledFiles = metricManager.counter("pendingRolledFiles");
	
	/** Flag to switch off deletion thread on shutdown */
	protected final AtomicBoolean keepRunning = new AtomicBoolean(IS_WIN);
	/** Background rolled file deletion thread if we're on windows, otherwise null */
	protected final Thread rolledFileDeletionThread = IS_WIN ? new Thread() {
		@Override
		public void run() {
			while(keepRunning.get()) {
				try { 
					Thread.currentThread().join(60000); 
					if(!pendingDeletes.isEmpty()) {
						final Map<String, File> tmp = new HashMap<String, File>(pendingDeletes);
						for(Map.Entry<String, File> entry: tmp.entrySet()) {
							final File f = entry.getValue();
							if(!f.exists()) {
								pendingDeletes.remove(entry.getKey());
								continue;
							}
							final long size = f.length();
							if(entry.getValue().delete()) {
								deletedRolledFiles.inc();
								pendingRolledFiles.dec();
								log.info("Deleted pending roll file [{}], size [{}} bytes", entry.getKey(), size);
								pendingDeletes.remove(entry.getKey());
							}
						}
					}
				} catch (Exception ex) {
					if(!keepRunning.get()) break;
					if(Thread.interrupted()) Thread.interrupted();
				}
			}
			log.info("rolledFileDeletionThread stopped");
		}
	} : null;
	
	// ===============================================================================================
	//		TSUID Cache Lookup Config
	// ===============================================================================================
	/** The TSUID lookup cache file name */
	protected String tsuidCacheDbFileName = null;
	/** The TSUID lookup cache file */
	protected File tsuidCacheDbFile = null;
	/** The TSUID lookup cache average key size */
	protected int avgKeySize = -1;	
	/** The TSUID lookup cache max entries */
	protected long maxEntries = -1;
	/** The TSUID lookup cache instance */
	protected ChronicleSet<byte[]> cacheDb = null;
	
	
	// ===============================================================================================
	//		RingBuffer Config
	// ===============================================================================================
	protected int cacheRbThreads = -1;
	protected int dispatchRbThreads = -1;
	protected int cacheRbSize = -1;
	protected int dispatchRbSize = -1;
	protected RBWaitStrategy cacheRbWaitStrat = null;
	protected RBWaitStrategy dispatchRbWaitStrat = null;
	
	/** The cache lookup ring buffer thread factory */
	protected final ThreadFactory cacheRbThreadFactory = JMXManagedThreadFactory.newThreadFactory("CacheLookupThread", false);
	/** The dispatch ring buffer thread factory */
	protected ThreadFactory dispatchRbThreadFactory = JMXManagedThreadFactory.newThreadFactory("MetricDispatchThread", false); 
	
	/** The cache lookup disruptor */
	protected Disruptor<TSDBMetricMeta> cacheRbDisruptor = null;
	/** The dispatch disruptor */
	protected Disruptor<TSDBMetricMeta> dispatchRbDisruptor = null;
	/** The cache lookup ring buffer */
	protected RingBuffer<TSDBMetricMeta> cacheRb = null;
	/** The dispatch ring buffer */
	protected RingBuffer<TSDBMetricMeta> dispatchRb = null;
	
	/** The cache lookup exception handler */
	protected final ExceptionHandler<TSDBMetricMeta> cacheLookupExceptionHandler = new ExceptionHandler<TSDBMetricMeta>() {
		
		@Override
		public void handleEventException(final Throwable ex, final long sequence, final TSDBMetricMeta event) {
			cacheLookupExceptions.inc();
			log.error("CacheLookup exception on meta {}", event, ex);
		}

		@Override
		public void handleOnStartException(final Throwable ex) {
			cacheLookupExceptions.inc();
			log.error("CacheLookup exception on start", ex);			
		}

		@Override
		public void handleOnShutdownException(final Throwable ex) {
			cacheLookupExceptions.inc();
			log.error("CacheLookup exception on shutdown", ex);			
		}
	};
	
	
	public static void main(String[] args) {
		try {
			final Config cfg = new Config(true);
			final TSDB tsdb = new TSDB(cfg);
			final TSDBChronicleEventPublisher pub = new TSDBChronicleEventPublisher();
			pub.initialize(tsdb);
			StdInCommandHandler.getInstance().registerCommand("stop", new Runnable(){
				public void run() {
					pub.shutdown();
					System.exit(0);
				}
			}).run();
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(-1);
		}
	}
	
	
//	/** The cache lookup handler */
//	protected final EventHandler<TSDBMetricMeta> cacheLookupHandler = new EventHandler<TSDBMetricMeta>() {
//		@Override
//		public void onEvent(final TSDBMetricMeta meta, final long sequence, final boolean endOfBatch) throws Exception {
//			final Context ctx = cacheLookupHandlerTimer.time();
//			try {
//				if(!cacheDb.contains(meta.getTsuid())) {
//					// We have to clone here since the incoming meta gets recycled back to the cacheRb.
//					// Alternatively, the resolveUID could be synchronous. 
//					final TSDBMetricMeta metaClone = meta.clone();
////					FIXME: temporarilly calling this just so we can compare elapsed times
////					FIXED: getTSMetaAsync is approx 20-30 slower than resolveUIDsAsync 
////					getTSMetaAsync(meta);
//					resolveUIDsAsync(metaClone).addCallback(new Callback<Void, EnumMap<UniqueIdType,Map<String,byte[]>>>() {
//						@Override
//						public Void call(final EnumMap<UniqueIdType, Map<String, byte[]>> map) throws Exception {
//							metaClone.resolved(
//									map.get(UniqueIdType.METRIC).values().iterator().next(), 
//									map.get(UniqueIdType.TAGK), 
//									map.get(UniqueIdType.TAGV)
//							);
//							final long seq = dispatchRb.next();
//							final TSDBMetricMeta meta = dispatchRb.get(seq);
//							meta.load(metaClone).resolved(metaClone);
//							dispatchRb.publish(seq);										
//							return null;
//						}
//					});
//				}
//			} finally {
//				meta.reset();
//				ctx.close();
//			}
//		}
//	};
	
	protected ChronicleMap<byte[], TSDBMetricMeta> metricMetas = null;
	public void setTestLookup(final ChronicleMap<byte[], TSDBMetricMeta> metricMetas) {
		this.metricMetas = metricMetas;
	}
	
	public Deferred<EnumMap<UniqueIdType, Map<String, byte[]>>> testLookup(final TSDBMetricMeta meta) {
		final EnumMap<UniqueIdType, Map<String, byte[]>> map = new EnumMap<UniqueIdType, Map<String, byte[]>>(UniqueIdType.class);
		final TSDBMetricMeta m = metricMetas.get(meta.getTsuid());
		map.put(UniqueIdType.METRIC, Collections.singletonMap(m.getMetricName(), m.getMetricUid()));
		map.put(UniqueIdType.TAGK, m.getTagKeyUids());
		map.put(UniqueIdType.TAGV, m.getTagValueUids());
		return Deferred.fromResult(map);
	}
	
	/** The cache lookup handler */
	protected final EventHandler<TSDBMetricMeta> cacheLookupHandler = new EventHandler<TSDBMetricMeta>() {
		@Override
		public void onEvent(final TSDBMetricMeta meta, final long sequence, final boolean endOfBatch) throws Exception {
			final Context ctx = cacheLookupHandlerTimer.time();
			try {
				if(!cacheDb.contains(meta.getTsuid())) {
					// We have to clone here since the incoming meta gets recycled back to the cacheRb.
					// Alternatively, the resolveUID could be synchronous. 
					final TSDBMetricMeta metaClone = meta.clone();
//					FIXME: temporarilly calling this just so we can compare elapsed times
//					FIXED: getTSMetaAsync is approx 20-30 slower than resolveUIDsAsync 
//					getTSMetaAsync(meta);
					testLookup(metaClone).addCallback(new Callback<Void, EnumMap<UniqueIdType,Map<String,byte[]>>>() {
						@Override
						public Void call(final EnumMap<UniqueIdType, Map<String, byte[]>> map) throws Exception {
							metaClone.resolved(
									map.get(UniqueIdType.METRIC).values().iterator().next(), 
									map.get(UniqueIdType.TAGK), 
									map.get(UniqueIdType.TAGV)
							);
							final long seq = dispatchRb.next();
							final TSDBMetricMeta meta = dispatchRb.get(seq);
							meta.load(metaClone).resolved(metaClone);
							dispatchRb.publish(seq);										
							return null;
						}
					});
				}
			} finally {
				meta.reset();
				ctx.close();
			}
		}
	};

	
	
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
				if(outQueueTextFormat) {
					outQueue.acquireAppender().writeDocument(w -> w.write(MessageType.METRICMETA.shortName).marshallable(meta));
				} else {
					outQueue.acquireAppender().writeBytes(meta);
				}
				cacheDb.add(meta.getTsuid());
				meta.recordTimer(endToEndTimer);
			} finally {
				meta.reset();
				ctx.stop();
			}
		}
	};
	
	

	/**
	 * Creates a new TSDBChronicleEventPublisher
	 */
	public TSDBChronicleEventPublisher() {
		log.info("Created TSDBChronicleEventPublisher instance");
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#initialize(net.opentsdb.core.TSDB)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void initialize(final TSDB tsdb) {
		log.info(">>>>> Initializing TSDBChronicleEventPublisher");
		this.tsdb = tsdb;
		metrics_width = TSDB.metrics_width();
		final Properties properties = new Properties();
		final Config cfg = tsdb.getConfig();
		properties.putAll(cfg.getMap());
		// ================  Configure Outbound Queue
		outQueueTextFormat = metricManager.getAndSetConfig(CONFIG_OUTQ_TEXT, DEFAULT_OUTQ_TEXT, properties, cfg);
		outQueueDirName = metricManager.getAndSetConfig(CONFIG_OUTQ_DIR, DEFAULT_OUTQ_DIR, properties, cfg); 
				
		outQueueDir = new File(outQueueDirName);
		outQueueBlockSize = metricManager.getAndSetConfig(CONFIG_OUTQ_BLOCKSIZE, DEFAULT_OUTQ_BLOCKSIZE, properties, cfg);
		outQueueRollCycle = metricManager.getAndSetConfig(CONFIG_OUTQ_ROLLCYCLE, DEFAULT_OUTQ_ROLLCYCLE, properties, cfg);
		outQueue = SingleChronicleQueueBuilder.binary(outQueueDir)
				.blockSize(outQueueBlockSize)
				.rollCycle(outQueueRollCycle)
				.storeFileListener(this)
				.onRingBufferStats(this)
				.wireType(outQueueTextFormat ? WireType.JSON : WireType.BINARY)
				.build();

		// ================  Configure Cache
		tsuidCacheDbFileName = metricManager.getAndSetConfig(CONFIG_CACHE_FILE, DEFAULT_CACHE_FILE, properties, cfg);
		tsuidCacheDbFile = new File(tsuidCacheDbFileName);
		tsuidCacheDbFile.getParentFile().mkdirs();
		avgKeySize = metricManager.getAndSetConfig(CONFIG_CACHE_AVGKEYSIZE, DEFAULT_CACHE_AVGKEYSIZE, properties, cfg);
		maxEntries = ConfigurationHelper.getLongSystemThenEnvProperty(CONFIG_CACHE_MAXKEYS, DEFAULT_CACHE_MAXKEYS, properties);
		try {
			cacheDb = ChronicleSetBuilder.of(byte[].class)
				.averageKeySize(avgKeySize)
				.entries(maxEntries)				
				.createOrRecoverPersistedTo(tsuidCacheDbFile);
			log.info("TSUID Lookup Cache Initialized. Initial Size: {}", cacheDb.size());
		} catch (Exception ex) {
			final String msg = "Failed to create TSUID lookup cache with file [" + tsuidCacheDbFileName + "]";
			log.error(msg, ex);
			throw new IllegalArgumentException(msg, ex);
		}
		
		// ================  Configure RingBuffer
		cacheRbThreads = metricManager.getAndSetConfig(CONFIG_CACHERB_THREADS, DEFAULT_CACHERB_THREADS, properties, cfg);
		dispatchRbThreads = metricManager.getAndSetConfig(CONFIG_DISPATCHRB_THREADS, DEFAULT_DISPATCHRB_THREADS, properties, cfg);
		cacheRbSize = metricManager.getAndSetConfig(CONFIG_CACHERB_SIZE, DEFAULT_CACHERB_SIZE, properties, cfg);
		dispatchRbSize = metricManager.getAndSetConfig(CONFIG_DISPATCHRB_SIZE, DEFAULT_DISPATCHRB_SIZE, properties, cfg);
		final Properties cacheRbWaitStratConfig = Props.extract(CONFIG_CACHERB_WAITSTRAT_PROPS, properties, true, false);
		final Properties dispatchRbWaitStratConfig = Props.extract(CONFIG_DISPATCHRB_WAITSTRAT_PROPS, properties, true, false);
		cacheRbWaitStrat = metricManager.getAndSetConfig(CONFIG_CACHERB_WAITSTRAT, DEFAULT_CACHERB_WAITSTRAT, properties, cfg);		
		dispatchRbWaitStrat = metricManager.getAndSetConfig(CONFIG_DISPATCHRB_WAITSTRAT, DEFAULT_DISPATCHRB_WAITSTRAT, properties, cfg);
		cacheRbDisruptor = new Disruptor<TSDBMetricMeta>(TSDBMetricMeta.FACTORY, cacheRbSize, cacheRbThreadFactory, ProducerType.MULTI, cacheRbWaitStrat.waitStrategy(cacheRbWaitStratConfig));
		cacheRbDisruptor.setDefaultExceptionHandler(cacheLookupExceptionHandler);
		cacheRbDisruptor.handleEventsWith(cacheLookupHandler);
		dispatchRbDisruptor = new Disruptor<TSDBMetricMeta>(TSDBMetricMeta.FACTORY, dispatchRbSize, dispatchRbThreadFactory, ProducerType.MULTI, cacheRbWaitStrat.waitStrategy(dispatchRbWaitStratConfig));
		dispatchRbDisruptor.setDefaultExceptionHandler(dispatchExceptionHandler);
		dispatchRbDisruptor.handleEventsWith(dispatchHandler);
		cacheRb = cacheRbDisruptor.start();
		log.info("Started CacheLookup RingBuffer");
		dispatchRb = dispatchRbDisruptor.start();
		log.info("Started MetricDispatch RingBuffer");
		
		if(rolledFileDeletionThread!=null) {
			rolledFileDeletionThread.setDaemon(true);
			rolledFileDeletionThread.start();
		}
		try {
			JMXHelper.registerMBean(this, JMXHelper.objectName("net.opentsdb:service=TSDBChronicleEventPublisher"));
		} catch (Exception ex) {
			log.warn("Failed to register management interface", ex);
		}
		log.info("<<<<< TSDBChronicleEventPublisher Initialized");
	}
	
	

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#shutdown()
	 */
	@Override
	public Deferred<Object> shutdown() {
		log.info(">>>>> Stopping TSDBChronicleEventPublisher");
		stopDisruptor("CacheLookup", cacheRbDisruptor);
		stopDisruptor("Dispatch", dispatchRbDisruptor);
		keepRunning.set(false);
		if(rolledFileDeletionThread!=null) rolledFileDeletionThread.interrupt();
		try { 
			outQueue.close();
			log.info("OutboundQueue Closed");
		} catch (Exception ex) {
			log.warn("Error closing OutboundQueue: {}", ex);
		}
		try {
			JMXHelper.unregisterMBean(JMXHelper.objectName("net.opentsdb:service=TSDBChronicleEventPublisher"));
		} catch (Exception x) {/* No Op */}

		log.info("<<<<< TSDBChronicleEventPublisher Stopped");
		return Deferred.fromResult(null);
	}
	
	
	/**
	 * Executes a controlled shutdown of the passed disruptor
	 * @param name The name of the disruptor for logging
	 * @param disruptor The disruptor to stop
	 */
	protected void stopDisruptor(final String name, final Disruptor<?> disruptor) {
		final long start = System.currentTimeMillis();
		try {			
			disruptor.shutdown(5000, TimeUnit.MILLISECONDS);
			final long elapsed = System.currentTimeMillis() - start;
			log.info("{} Disruptor stopped normally in {} ms.", name, elapsed);
		} catch (TimeoutException tex) {
			final long elapsed = System.currentTimeMillis() - start;
			log.warn("{} Disruptor failed to stop normally after {} ms. Halting....", name, elapsed);
			disruptor.halt();
			log.warn("{} Disruptor halted");
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#version()
	 */
	@Override
	public String version() {
		return "2.1";
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#collectStats(net.opentsdb.stats.StatsCollector)
	 */
	@Override
	public void collectStats(final StatsCollector collector) {	
		metricManager.collectStats(collector);
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#publishDataPoint(java.lang.String, long, long, java.util.Map, byte[])
	 */
	@Override
	public Deferred<Object> publishDataPoint(final String metric, final long timestamp, final long value, final Map<String, String> tags, final byte[] tsuid) {
		final long sequence = cacheRb.next();
		final TSDBMetricMeta meta = cacheRb.get(sequence);
		meta.reset().load(metric, tags, tsuid).startTimer();
		cacheRb.publish(sequence);
		return Deferred.fromResult(null);
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#publishDataPoint(java.lang.String, long, double, java.util.Map, byte[])
	 */
	@Override
	public Deferred<Object> publishDataPoint(final String metric, final long timestamp, final double value, final Map<String, String> tags, final byte[] tsuid) {
		final long sequence = cacheRb.next();
		final TSDBMetricMeta meta = cacheRb.get(sequence);
		meta.reset().load(metric, tags, tsuid).startTimer();
		cacheRb.publish(sequence);
		return Deferred.fromResult(null);
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#publishAnnotation(net.opentsdb.meta.Annotation)
	 */
	@Override
	public Deferred<Object> publishAnnotation(final Annotation annotation) {
		return Deferred.fromResult(null);
	}
	
	protected Deferred<TSMeta> getTSMetaAsync(final TSDBMetricMeta meta) {
		final Context ctx = getTSMetaTimer.time();
		return TSMeta.getTSMeta(tsdb, UniqueId.uidToString(meta.getTsuid())).addCallback(new Callback<TSMeta, TSMeta>() {
			@Override
			public TSMeta call(final TSMeta tsMeta) throws Exception {
				ctx.close();
				return tsMeta;
			}			
		});
	}
	
	/**
	 * The RTPublisher callbacks do not supply the metric name or tag UIDs, and so far as I can tell,
	 * it's not possible to link the TSUID segments to the tag values. So if the cacheDB does not contain
	 * the incoming TSUID, this method makes an async call to lookup all the UIDs. Not very efficient, but
	 * should only be called once per TSUID.
	 * <p>FIXME: The first {@link TSDB#metrics_width()} bytes of the TSUID are the metric name UID, so we don't need to fetch it.</p>
	 * @param meta The meta to resolve the UIDs for
	 * @return A deferred handle to a map of name to UID pairs within a map keyed by uniqueidtypes (metricname, tag key, tag value)
	 */
	protected Deferred<EnumMap<UniqueIdType, Map<String, byte[]>>> resolveUIDsAsync(final TSDBMetricMeta meta) {
		final Context ctx = resolveUidsTimer.time();
		final int tsize = meta.getTags().size();
		final Deferred<EnumMap<UniqueIdType, Map<String, byte[]>>> resultDef = new Deferred<EnumMap<UniqueIdType, Map<String, byte[]>>>();
		final EnumMap<UniqueIdType, Map<String, byte[]>> resolvedMap = new EnumMap<UniqueIdType, Map<String, byte[]>>(UniqueIdType.class);
		final byte[] metricUid = new byte[metrics_width];
		System.arraycopy(meta.getTsuid(), 0, metricUid, 0, metrics_width);
		resolvedMap.put(UniqueIdType.METRIC, Collections.singletonMap(meta.getMetricName(), metricUid));
		resolvedMap.put(UniqueIdType.TAGK, new HashMap<String, byte[]>(tsize));
		resolvedMap.put(UniqueIdType.TAGV, new HashMap<String, byte[]>(tsize));
		final ArrayList<Deferred<byte[]>> completion = new ArrayList<Deferred<byte[]>>((tsize * 2));		
		for (final Map.Entry<String, String> entry : meta.getTags().entrySet()) {
			final String tagKey = entry.getKey();
			final String tagValue = entry.getValue();
			final Deferred<byte[]> tagKeyDef = tsdb.getUIDAsync(UniqueIdType.TAGK, tagKey);
			tagKeyDef.addCallback(new Callback<Void, byte[]>(){
				@Override
				public Void call(final byte[] uid) throws Exception {
					resolvedMap.get(UniqueIdType.TAGK).put(tagKey, uid);
					return null;
				}
			});
			completion.add(tagKeyDef);
			final Deferred<byte[]> tagValueDef = tsdb.getUIDAsync(UniqueIdType.TAGV, tagValue);
			tagValueDef.addCallback(new Callback<Void, byte[]>(){
				@Override
				public Void call(final byte[] uid) throws Exception {
					resolvedMap.get(UniqueIdType.TAGV).put(tagValue, uid);
					return null;
				}
			});
			completion.add(tagValueDef);			
		}
		Deferred.group(completion).addCallback(new Callback<Void, ArrayList<byte[]>>() {
			@Override
			public Void call(final ArrayList<byte[]> arg) throws Exception {
				final long elapsed = ctx.stop();
				log.info("UID Resolution Elapsed: {} ms.", TimeUnit.NANOSECONDS.toMillis(elapsed));
				resultDef.callback(resolvedMap);
				return null;
			}
		});
		return resultDef;
	}
	
	/**
	 * A synchronous version of {@link #resolveUIDsAsync}.
	 * @param meta The meta to resolve the UIDs for
	 * @param timeout The timeout on the operation in ms.
	 * @return A map of name to UID pairs within a map keyed by uniqueidtypes (metricname, tag key, tag value)
	 */
	protected EnumMap<UniqueIdType, Map<String, byte[]>> resolveUIDs(final TSDBMetricMeta meta, final long timeout) {
		try {
			return resolveUIDsAsync(meta).join(timeout);
		} catch (Exception ex) {
			log.error("Synchronous resolveUIDs failed", ex);
			throw new RuntimeException("Synchronous resolveUIDs failed", ex);
		}
	}
	
	/**
	 * A synchronous version of {@link #resolveUIDsAsync} with a timeout of 5000 ms.
	 * @param meta The meta to resolve the UIDs for
	 * @return A map of name to UID pairs within a map keyed by uniqueidtypes (metricname, tag key, tag value)
	 */
	protected EnumMap<UniqueIdType, Map<String, byte[]>> resolveUIDs(final TSDBMetricMeta meta) {
		return resolveUIDs(meta, 5000);
	}
	
	

	/**
	 * {@inheritDoc}
	 * @see net.openhft.chronicle.queue.impl.StoreFileListener#onReleased(int, java.io.File)
	 */
	@Override
	public void onReleased(final int cycle, final File file) {
		rolledFiles.inc();
		if(file.delete()) {
			deletedRolledFiles.inc();
			log.info("Deleted rolled file [{}], cycle: [{}]", file, cycle);
		} else {
			pendingRolledFiles.inc();
			pendingDeletes.put(file.getName(), file);
		}
		
	}

	/**
	 * {@inheritDoc}
	 * @see java.util.function.Consumer#accept(java.lang.Object)
	 */
	@Override
	public void accept(final BytesRingBufferStats queueStats) {
		// enterprise only, I think
	}


	public String getOutQueueDir() {
		return outQueueDir.getAbsolutePath();
	}

	public int getOutQueueBlockSize() {
		return outQueueBlockSize;
	}

	public String getOutQueueRollCycle() {
		return outQueueRollCycle.format();
	}

	public int getPendingDeleteCount() {
		return IS_WIN ? pendingDeletes.size() : 0;
	}

	public long getRolledFiles() {
		return rolledFiles.getCount();
	}

	public long getDeletedRolledFiles() {
		return deletedRolledFiles.getCount();
	}

	public long getPendingRolledFiles() {
		return pendingRolledFiles.getCount();
	}

	public String getTsuidCacheDbFile() {
		return tsuidCacheDbFile.getAbsolutePath();
	}
	
	public long getTsuidCacheDbFileSize() {
		return tsuidCacheDbFile.length();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.TSDBChronicleEventPublisherMBean#getOutQueueFileCount()
	 * FIXME: Always returns 0
	 */
	public int getOutQueueFileCount() {
		return outQueueDir.listFiles(new FilenameFilter(){
			@Override
			public boolean accept(final File f, final String name) {				
				return f.isFile() && name.toLowerCase().endsWith(".cq4");
			}
		}).length;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.opentsdb.TSDBChronicleEventPublisherMBean#getOutQueueFileSize()
	 * FIXME: Always returns 0
	 */
	public long getOutQueueFileSize() {
		return Arrays.stream(
			outQueueDir.listFiles(new FilenameFilter(){
				@Override
				public boolean accept(final File f, final String name) {				
					return f.isFile() && name.toLowerCase().endsWith(".cq4");
				}
			})
		).mapToLong(f -> f.length()).sum();
	}
	

	public int getAvgKeySize() {
		return avgKeySize;
	}

	public int getLookupCacheSize() {
		return cacheDb.size();
	}
	
	public int getLookupCacheSegments() {
		return cacheDb.segments();
	}
	

	public int getCacheRbThreads() {
		return cacheRbThreads;
	}

	public int getDispatchRbThreads() {
		return dispatchRbThreads;
	}

	public int getCacheRbSize() {
		return cacheRbSize;
	}

	public int getDispatchRbSize() {
		return dispatchRbSize;
	}

	public String getCacheRbWaitStrat() {
		return cacheRbWaitStrat.name();
	}

	public String getDispatchRbWaitStrat() {
		return dispatchRbWaitStrat.name();
	}

	public long getCacheRbCapacity() {
		return cacheRb.remainingCapacity();
	}

	public long getDispatchRbCapacity() {
		return dispatchRb.remainingCapacity();
	}

	public long getDispatchHandleCount() {
		return dispatchHandlerTimer.getCount();
	}
	
	public double getDispatchHandle1mRate() {
		return dispatchHandlerTimer.getOneMinuteRate();
	}
	
	public double getDispatchHandle99PctElapsed() {
		return dispatchHandlerTimer.getSnapshot().get99thPercentile();
	}
	
	public long getCacheLookupHandleCount() {
		return cacheLookupHandlerTimer.getCount();
	}
	
	public double getCacheLookupHandle1mRate() {
		return cacheLookupHandlerTimer.getOneMinuteRate();
	}
	
	public double getCacheLookupHandle99PctElapsed() {
		return cacheLookupHandlerTimer.getSnapshot().get99thPercentile();
	}
	
	public long getResolveUidHandleCount() {
		return resolveUidsTimer.getCount();
	}
	
	public double getTSMetaLookup1mRate() {
		return getTSMetaTimer.getOneMinuteRate();
	}
	
	public double getTSMetaLookup99PctElapsed() {
		return getTSMetaTimer.getSnapshot().get99thPercentile();
	}
	
	public long getTSMetaLookupCount() {
		return getTSMetaTimer.getCount();
	}
	
	public double getResolveUidHandle1mRate() {
		return resolveUidsTimer.getOneMinuteRate();
	}
	
	public double getResolveUidHandle99PctElapsed() {
		return resolveUidsTimer.getSnapshot().get99thPercentile();
	}
	
	public void clearLookupCache() {
		cacheDb.clear();
	}
	
	public long getDispatchExceptionCount() {
		return dispatchExceptions.getCount();
	}
	
	public long getCacheLookupExceptionCount() {
		return cacheLookupExceptions.getCount();
	}
	
	public double getEndToEnd99PctElapsed() {
		return endToEndTimer.getSnapshot().get99thPercentile();
	}
	
	public double getEndToEnd999PctElapsed() {
		return endToEndTimer.getSnapshot().get999thPercentile();
	}
	
	public double getEndToEndMeanElapsed() {
		return endToEndTimer.getSnapshot().getMean();
	}
	
	public double getEndToEndMedianElapsed() {
		return endToEndTimer.getSnapshot().getMedian();
	}
	
	
	
}
