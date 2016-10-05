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
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.heliosapm.streams.chronicle.TSDBMetricMeta;
import com.heliosapm.streams.opentsdb.plugin.PluginMetricManager;
import com.heliosapm.streams.opentsdb.ringbuffer.RBWaitStrategy;
import com.heliosapm.utils.collections.Props;
import com.heliosapm.utils.config.ConfigurationHelper;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.openhft.chronicle.bytes.BytesRingBufferStats;
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
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.tsd.RTPublisher;
import net.opentsdb.uid.UniqueId.UniqueIdType;

/**
 * <p>Title: TSDBChronicleEventPublisher</p>
 * <p>Description: Publishes events to a chronicle out queue and accepts TSUID resolution requests
 * via an in chronicle queue.</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.opentsdb.TSDBChronicleEventPublisher</code></p>
 */

public class TSDBChronicleEventPublisher extends RTPublisher implements StoreFileListener, Consumer<BytesRingBufferStats> {
	
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
	
	
//	queue = SingleChronicleQueueBuilder.binary(baseQueueDirectory)
//			.blockSize(blockSize)
//			.rollCycle(rollCycle)
//			.storeFileListener(this)
//			.wireType(WireType.BINARY)
//			.build();
	
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
	/** Flag to switch off deletion thread on shutdown */
	protected final AtomicBoolean keepRunning = new AtomicBoolean(IS_WIN);
	/** Background rolled file deletion thread if we're on windows, otherwise null */
	protected final Thread rolledFileDeletionThread = IS_WIN ? new Thread() {
		@Override
		public void run() {
			while(keepRunning.get()) {
				try { Thread.currentThread().join(60000); } catch (Exception x) {/* No Op */}
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
							log.info("Deleted pending roll file [{}], size [{}} bytes", entry.getKey(), size);
							pendingDeletes.remove(entry.getKey());
						}
					}
				}
			}
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
	
	protected final ThreadFactory cacheRbThreadFactory = new ThreadFactory() {
		final AtomicInteger serial = new AtomicInteger(0);
		@Override
		public Thread newThread(final Runnable r) {
			final Thread t = new Thread(r, "CacheLookupThread#" + serial.incrementAndGet());
			t.setDaemon(true);
			return t;
		}		
	};
	protected ThreadFactory dispatchRbThreadFactory = new ThreadFactory() {
		final AtomicInteger serial = new AtomicInteger(0);
		@Override
		public Thread newThread(final Runnable r) {
			final Thread t = new Thread(r, "MetricDispatchThread#" + serial.incrementAndGet());
			t.setDaemon(true);
			return t;
		}		
	};
	
	protected Disruptor<TSDBMetricMeta> cacheRbDisruptor = null;
	protected Disruptor<TSDBMetricMeta> dispatchRbDisruptor = null;
	protected RingBuffer<TSDBMetricMeta> cacheRb = null;
	protected RingBuffer<TSDBMetricMeta> dispatchRb = null;
	
	/** The cache lookup handler */
	protected final EventHandler<TSDBMetricMeta> cacheLookupHandler = new EventHandler<TSDBMetricMeta>() {
		@Override
		public void onEvent(final TSDBMetricMeta event, final long sequence, final boolean endOfBatch) throws Exception {
			// TODO Auto-generated method stub
			
		}
	};
	
	/** The dispatch handler */
	protected final EventHandler<TSDBMetricMeta> dispatchHandler = new EventHandler<TSDBMetricMeta>() {
		@Override
		public void onEvent(final TSDBMetricMeta event, final long sequence, final boolean endOfBatch) throws Exception {
			// TODO Auto-generated method stub
			
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
		final Properties properties = new Properties();
		properties.putAll(tsdb.getConfig().getMap());
		// ================  Configure Outbound Queue
		outQueueDirName = ConfigurationHelper.getSystemThenEnvProperty(CONFIG_OUTQ_DIR, DEFAULT_OUTQ_DIR, properties);
		outQueueDir = new File(outQueueDirName);
		outQueueBlockSize = ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_OUTQ_BLOCKSIZE, DEFAULT_OUTQ_BLOCKSIZE, properties);
		outQueueRollCycle = ConfigurationHelper.getEnumSystemThenEnvProperty(RollCycles.class, CONFIG_OUTQ_ROLLCYCLE, DEFAULT_OUTQ_ROLLCYCLE, properties);
		outQueue = SingleChronicleQueueBuilder.binary(outQueueDir)
				.blockSize(outQueueBlockSize)
				.rollCycle(outQueueRollCycle)
				.storeFileListener(this)
				.onRingBufferStats(this)
				.wireType(WireType.BINARY)
				.build();
//		public static final String CONFIG_OUTQ_DIR = "eventpublisher.outq.dir";
//		public static final String CONFIG_OUTQ_BLOCKSIZE = "eventpublisher.outq.blocksize";
//		public static final String CONFIG_OUTQ_ROLLCYCLE = "eventpublisher.outq.rollcycle";
//		
//		queue = SingleChronicleQueueBuilder.binary(baseQueueDirectory)
//				.blockSize(blockSize)
//				.rollCycle(rollCycle)
//				.storeFileListener(this)
//				.wireType(WireType.BINARY)
//				.build();
//		
//		// ===============================================================================================
//		//		Out Queue Config
//		// ===============================================================================================
//		/** The queue base directory name */
//		protected String outQueueDirName = null;
//		/** The queue base directory */
//		protected File outQueueDir = null;	
//		/** The queue block size */
//		protected int outQueueBlockSize = -1;
//		/** The queue roll cycle */
//		protected RollCycle outQueueRollCycle = null;

		// ================  Configure Cache
		tsuidCacheDbFileName = ConfigurationHelper.getSystemThenEnvProperty(CONFIG_CACHE_FILE, DEFAULT_CACHE_FILE, properties);
		tsuidCacheDbFile = new File(tsuidCacheDbFileName);
		avgKeySize = ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_CACHE_AVGKEYSIZE, DEFAULT_CACHE_AVGKEYSIZE, properties);
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
		cacheRbThreads = ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_CACHERB_THREADS, DEFAULT_CACHERB_THREADS, properties);
		dispatchRbThreads = ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_DISPATCHRB_THREADS, DEFAULT_DISPATCHRB_THREADS, properties);
		cacheRbSize = ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_CACHERB_SIZE, DEFAULT_CACHERB_SIZE, properties);
		dispatchRbSize = ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_DISPATCHRB_SIZE, DEFAULT_DISPATCHRB_SIZE, properties);
		final Properties cacheRbWaitStratConfig = Props.extract(CONFIG_CACHERB_WAITSTRAT_PROPS, properties, true, false);
		final Properties dispatchRbWaitStratConfig = Props.extract(CONFIG_DISPATCHRB_WAITSTRAT_PROPS, properties, true, false);
		cacheRbWaitStrat = ConfigurationHelper.getEnumSystemThenEnvProperty(RBWaitStrategy.class, CONFIG_CACHERB_WAITSTRAT, DEFAULT_CACHERB_WAITSTRAT, properties);		
		dispatchRbWaitStrat = ConfigurationHelper.getEnumSystemThenEnvProperty(RBWaitStrategy.class, CONFIG_DISPATCHRB_WAITSTRAT, DEFAULT_DISPATCHRB_WAITSTRAT, properties);
		cacheRbDisruptor = new Disruptor<TSDBMetricMeta>(TSDBMetricMeta.FACTORY, cacheRbSize, cacheRbThreadFactory, ProducerType.MULTI, cacheRbWaitStrat.waitStrategy(cacheRbWaitStratConfig));
		cacheRbDisruptor.handleEventsWith(cacheLookupHandler);
		dispatchRbDisruptor = new Disruptor<TSDBMetricMeta>(TSDBMetricMeta.FACTORY, dispatchRbSize, dispatchRbThreadFactory, ProducerType.MULTI, cacheRbWaitStrat.waitStrategy(dispatchRbWaitStratConfig));
		dispatchRbDisruptor.handleEventsWith(dispatchHandler);
		cacheRb = cacheRbDisruptor.start();
		log.info("Started CacheLookup RingBuffer");
		dispatchRb = dispatchRbDisruptor.start();
		log.info("Started MetricDispatch RingBuffer");
		
		
		
		
		log.info("<<<<< TSDBChronicleEventPublisher Initialized");
		
		
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#shutdown()
	 */
	@Override
	public Deferred<Object> shutdown() {
		log.info("Stopping TSDBChronicleEventPublisher");
		return Deferred.fromResult(null);
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

		return Deferred.fromResult(null);
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#publishDataPoint(java.lang.String, long, double, java.util.Map, byte[])
	 */
	@Override
	public Deferred<Object> publishDataPoint(final String metric, final long timestamp, final double value, final Map<String, String> tags, final byte[] tsuid) {

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
	
	/**
	 * The RTPublisher callbacks do not supply the metric name or tag UIDs, and so far as I can tell,
	 * it's not possible to link the TSUID segments to the tag values. So if the cacheDB does not contain
	 * the incoming TSUID, this method makes an async call to lookup all the UIDs. Not very efficient, but
	 * should only be called once per TSUID.
	 * <p>FIXME: The first {@link TSDB#metrics_width()} bytes of the TSUID are the metric name UID, so we don't need to fetch it.</p>
	 * @param meta The meta to resolve the UIDs for
	 * @return A map of name to UID pairs within a map keyed by uniqueidtypes (metricname, tag key, tag value)
	 */
	protected Deferred<EnumMap<UniqueIdType, Map<String, byte[]>>> resolveUIDsAsync(final TSDBMetricMeta meta) {
		final Context ctx = resolveUidsTimer.time();
		final int tsize = meta.getTags().size();
		final Deferred<EnumMap<UniqueIdType, Map<String, byte[]>>> resultDef = new Deferred<EnumMap<UniqueIdType, Map<String, byte[]>>>();
		final EnumMap<UniqueIdType, Map<String, byte[]>> resolvedMap = new EnumMap<UniqueIdType, Map<String, byte[]>>(UniqueIdType.class);
		resolvedMap.put(UniqueIdType.METRIC, new HashMap<String, byte[]>(1));
		resolvedMap.put(UniqueIdType.TAGK, new HashMap<String, byte[]>(tsize));
		resolvedMap.put(UniqueIdType.TAGV, new HashMap<String, byte[]>(tsize));
		final String metricName = meta.getMetricName();
		final ArrayList<Deferred<byte[]>> completion = new ArrayList<Deferred<byte[]>>((tsize * 2)+1);		
		final Deferred<byte[]> metricNameDef = tsdb.getUIDAsync(UniqueIdType.METRIC, metricName);
		metricNameDef.addCallback(new Callback<Void, byte[]>(){
			@Override
			public Void call(final byte[] uid) throws Exception {
				resolvedMap.get(UniqueIdType.METRIC).put(metricName, uid);
				return null;
			}
		});
		completion.add(metricNameDef);
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
	 * {@inheritDoc}
	 * @see net.openhft.chronicle.queue.impl.StoreFileListener#onReleased(int, java.io.File)
	 */
	@Override
	public void onReleased(final int cycle, final File file) {
		
	}

	/**
	 * {@inheritDoc}
	 * @see java.util.function.Consumer#accept(java.lang.Object)
	 */
	@Override
	public void accept(final BytesRingBufferStats queueStats) {
		
	}


}
