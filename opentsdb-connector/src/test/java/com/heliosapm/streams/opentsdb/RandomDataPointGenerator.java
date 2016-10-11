package com.heliosapm.streams.opentsdb;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Timer;
import com.heliosapm.streams.chronicle.TSDBMetricMeta;
import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.lang.StringHelper;
import com.heliosapm.utils.time.SystemClock;
import com.heliosapm.utils.time.SystemClock.ElapsedTime;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.opentsdb.core.TSDB;
import net.opentsdb.tsd.RTPublisher;
import net.opentsdb.utils.Config;

/**
 * <p>Title: RandomDataPointGenerator</p>
 * <p>Description: Generates random data points</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.opentsdb.RandomDataPointGenerator</code></p>
 */
public class RandomDataPointGenerator implements Runnable {
	/** A random to generate seeds for the thread local random */
	protected static final Random R = new Random(System.currentTimeMillis());
	/** The random used to generate random values */
	protected static final ThreadLocal<Random> RANDOM = new ThreadLocal<Random>() {
		@Override
		protected Random initialValue() {
			return new Random(R.nextLong());
		}
	};
	/** The thread group runner threads will be created in */
	protected static final ThreadGroup THREAD_GROUP = new ThreadGroup("RandomDataPointGenerator");
	
	/** The thread factory to create threads that will run the generation */
	protected static final ThreadFactory THREAD_FACTORY = new ThreadFactory() {
		protected final AtomicLong serial = new AtomicLong(0L);

		@Override
		public Thread newThread(final Runnable r) {
			final Thread t = new Thread(THREAD_GROUP, r, "RandomDataPointGenerator#" + serial.incrementAndGet());
			t.setDaemon(true);
			return t;
		}
		
	};
	
	
	/** Instance logger */
	protected final Logger log;
	protected final ChronicleMap<byte[], TSDBMetricMeta> metricMetas;
	protected final ChronicleMap<String, String> tagKeyUids;
	protected final ChronicleMap<String, String> tagValueUids;
	protected final ChronicleMap<String, String> metricUids;
	protected final AtomicInteger tagKeyUidSeed = new AtomicInteger(0);
	protected final AtomicInteger tagValueUidSeed = new AtomicInteger(0);
	protected final AtomicInteger metricUidSeed = new AtomicInteger(0);
	protected final int loops;
	protected final long sleep;
	protected final int sleepFreq; 
	protected final boolean text;
	protected Thread runThread = null;
	/** Flag to indicate if the run thread should be running */
	protected final AtomicBoolean keepRunning = new AtomicBoolean(false);
	/** An arbitrary name for this generator */
	protected final String name;
	/** The RTPublisher to dispatch messages to */
	protected final RTPublisher publisher;
	/** The out queue reader thread */
	protected final Thread drainer;
	protected final Timer endToEndTimer = new Timer(); 

	static final String PLACEHOLDER = "¥©¿";
	
	public RandomDataPointGenerator(final RTPublisher publisher, final String name, final int sampleSize, final int loops, final long sleep, final int sleepFreq, final boolean text) {
		this.name = (name==null || name.trim().isEmpty()) ? ("RDPG@" + System.identityHashCode(this)) : name;
		log = LogManager.getLogger(getClass().getName() + "." + this.name);
		this.loops = loops < 1 ? Integer.MAX_VALUE : loops;
		this.sleep = sleep;
		this.sleepFreq = sleepFreq;
		this.text = text;
		this.publisher = publisher;
		drainer = new Thread("DrainThread") {
			public void run() {
				log.info("Drain thread starting");
				final File outQueueDir = ((TSDBChronicleEventPublisher)publisher).outQueueDir;
				final RollCycle rc = ((TSDBChronicleEventPublisher)publisher).outQueueRollCycle;
				final SingleChronicleQueue outQueue = SingleChronicleQueueBuilder.binary(outQueueDir)
					.rollCycle(rc)
					.build();
				try {
					final ExcerptTailer tailer = outQueue.createTailer();
					final TSDBMetricMeta mm = TSDBMetricMeta.FACTORY.newInstance(); 
					long read = 0;
					while(keepRunning.get()) {
						try {
							
							if(tailer.readBytes(mm)) {
								mm.recordTimer(endToEndTimer);
								read++;							
								if(read%128000==0) {								
									log.info("Drainer: {} messages drained\n\tMean Rate: {}\n\tMean E2E: {} ms.", read, endToEndTimer.getMeanRate(), TimeUnit.NANOSECONDS.toMillis((long)endToEndTimer.getSnapshot().getMean()));
								}
							} else {
								SystemClock.sleep(10);
							}
						} catch (Exception ex) {
							if(InterruptedException.class.isInstance(ex) || (ex.getCause()!=null && InterruptedException.class.isInstance(ex.getCause()))) {
								log.info("Drain thread stopping");
								break;
							} else {
								log.warn("Drain thread error", ex);
							}
						}
					}
					log.info("Drain thread stopped");
				} finally {
					try { outQueue.close(); } catch (Exception x) {/* No Op */};
				}
			}
		};
		drainer.setDaemon(true);
		
		
		
		metricMetas = ChronicleMapBuilder
				.of(byte[].class, TSDBMetricMeta.class)
				.maxBloatFactor(10)
				.averageKeySize(24)
				.averageValueSize(text ? 800 : 281)
				.entries(sampleSize)
				.create();
		tagKeyUids = ChronicleMapBuilder
				.of(String.class, String.class)
				.maxBloatFactor(10)
				.averageKey(getRandomFragment())
				.averageValueSize(8)
				.entries(sampleSize)
				.create();
		tagValueUids = ChronicleMapBuilder
				.of(String.class, String.class)
				.maxBloatFactor(10)
				.averageKey(getRandomFragment())
				.averageValueSize(8)
				.entries(sampleSize)
				.create();
		metricUids = ChronicleMapBuilder
				.of(String.class, String.class)
				.maxBloatFactor(10)
				.averageKey(getRandomFragment())
				.averageValueSize(8)
				.entries(sampleSize)
				.create();
		
		log.info("Generator [{}] created. Generating Meta Samples...", this.name);
		final ElapsedTime et = SystemClock.startClock();
		IntStream.range(0, sampleSize).parallel().forEach(i -> {
			try {
				final ByteBuffer buff = ByteBuffer.allocate(4);
				final HashMap<String, String> tags = new HashMap<String, String>(4);
				final HashMap<String, String> tagKeys = new HashMap<String, String>(4);
				final HashMap<String, String> tagValues = new HashMap<String, String>(4);
				while(tags.size()<4) {
					final String key = getRandomFragment();
					final String value = getRandomFragment();
					tags.put(key, value);					
					String keyUid = tagKeyUids.putIfAbsent(key, PLACEHOLDER);
					if(keyUid==null || keyUid==PLACEHOLDER) {
						int uid = tagKeyUidSeed.incrementAndGet();
						buff.putInt(0, uid);
						byte[] uidBytes = buff.array();
						keyUid = trimHex(StringHelper.bytesToHex(uidBytes));
						tagKeyUids.replace(key, PLACEHOLDER, keyUid);						
					} 
					tagKeys.put(key, keyUid);
					
					String valueUid = tagValueUids.putIfAbsent(value, PLACEHOLDER);
					if(valueUid==null || valueUid==PLACEHOLDER) {
						int uid = tagValueUidSeed.incrementAndGet();
						buff.putInt(0, uid);
						byte[] uidBytes = buff.array();
						valueUid = trimHex(StringHelper.bytesToHex(uidBytes));
						tagValueUids.replace(value, PLACEHOLDER, valueUid);						
					} 
					tagValues.put(value, valueUid);					
				}
				final String metricName = getRandomFragment();
				String metricUid = metricUids.putIfAbsent(metricName, PLACEHOLDER);
				if(metricUid==null || metricUid==PLACEHOLDER) {
					int uid = metricUidSeed.incrementAndGet();
					buff.putInt(0, uid);
					byte[] uidBytes = buff.array();
					metricUid = trimHex(StringHelper.bytesToHex(uidBytes));
					metricUids.replace(metricName, PLACEHOLDER, metricUid);						
				} 
				final String muid = metricUid;					
				final byte[] tsuid = randomBytes(24);
				if(i==0) {
					log.info("TSUID Size: {}", tsuid.length);
				}
				metricMetas.put(tsuid, TSDBMetricMeta.FACTORY.newInstance()
						.load(metricName, tags, tsuid)
						.resolved(muid, tagKeys, tagValues)
						.clone());
			} catch (Exception ex) {
				log.error("Failed at insert {}", i, ex);
				System.exit(-1);
			}
		});
		log.info("Loaded [{}] samples: {}, Cache Size: {}", sampleSize, et.printAvg("Sample Load Rate", sampleSize), metricMetas.size());		
	}
	
	public static String trimHex(final String value) {
		// 000001D7
		if(value.length()>6 && value.indexOf("00")==0) {
			return value.substring(2);
		}
		return value;
	}
	
	public static void main(String[] args) {
		try {
			JMXHelper.fireUpJMXMPServer(3259);
			final Config cfg = new Config(true);
			final TSDB tsdb = new TSDB(cfg);
			final TSDBChronicleEventPublisher pub = new TSDBChronicleEventPublisher(true);
			final int sampleSize = 1024;
			final int loops = 100000; 
			final long sleep = 0; //10000;
			final int sleepFreq = 1;
			final boolean text = false;			
			final RandomDataPointGenerator r = new RandomDataPointGenerator(pub, "Test", sampleSize, loops, sleep, sleepFreq, text);
			pub.setTestLookup(r.metricMetas);
			pub.initialize(tsdb);
			pub.clearLookupCache();
			r.start();
			StdInCommandHandler.getInstance().registerCommand("stop", new Runnable(){
				public void run() {
					r.stop();
					pub.shutdown();
					System.exit(0);
				}
			}).run();
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(-1);
		}

		
	}
	
	
	public TSDBMetricMeta getMeta(final byte[] tsuid) {
		return metricMetas.get(tsuid);
	}
	
	public void start() {
		if(keepRunning.compareAndSet(false, true)) {
			runThread = THREAD_FACTORY.newThread(this);
			runThread.start();
			drainer.start();
		}
	}
	
	public boolean isStarted() {
		return keepRunning.get();
	}
	
	public void stop() {
		if(keepRunning.compareAndSet(true, false)) {
			runThread.interrupt();
			drainer.interrupt();
		}
	}
	
	public void run() {
		log.info("Starting DP generator");		
		try {
			final int sz = metricMetas.size();
			for(int i = 0; i < loops; i++) {
				log.info("Starting loop #{}...", i);
				final long now = System.currentTimeMillis();
				final CountDownLatch latch = new CountDownLatch(sz);
				final ElapsedTime et = SystemClock.startClock();
				metricMetas.values().stream().forEach(mm -> { 
						publisher.publishDataPoint(mm.getMetricName(), now, nextPosDouble(), mm.getTags(), mm.getTsuid());
						latch.countDown();
					}
				);
				latch.await();
				final String msg = et.printAvg("Dispatches", sz);
				log.info("Dispatched [{}] message", msg);
				if(sleep > 0) {
					SystemClock.sleep(sleep);
				}
				
				if(i>0 && i%10==0) {
					System.err.println("========= CLEARING LOOKUP CACHE");
					((TSDBChronicleEventPublisher)publisher).clearLookupCache();
					//((TSDBChronicleEventPublisher)publisher).outQueue.clear();
				}
			}
			log.info("Instance completed");
		} catch (Exception ex) {
			if(InterruptedException.class.isInstance(ex)) {
				log.info("Stopping instance...");
			} else {
				log.error("Unexpected exception", ex);
			}
		}
	}
	
	
	/**
	 * Returns a random positive int within the bound
	 * @param bound the bound on the random number to be returned. Must be positive. 
	 * @return a random positive int
	 */
	public static int nextPosInt(int bound) {
		return Math.abs(RANDOM.get().nextInt(bound));
	}

	/**
	 * Returns a random positive long
	 * @return a random positive long
	 */
	public static long nextPosLong() {
		return Math.abs(RANDOM.get().nextLong());
	}

	/**
	 * Returns a random positive double
	 * @return a random positive double
	 */
	public static double nextPosDouble() {
		return Math.abs(RANDOM.get().nextDouble());
	}
	
	/**
	 * Generates and returns a byte array populated with random bytes
	 * @param size The size of the byte array to generate
	 * @return the random content byte array
	 */
	public static byte[] randomBytes(int size) {
		final byte[] bytes = new byte[size];
		RANDOM.get().nextBytes(bytes);
		return bytes;
	}

	/**
	 * Generates an array of random strings created from splitting a randomly generated UUID.
	 * @return an array of random strings
	 */
	public static String[] getRandomFragments() {
		return UUID.randomUUID().toString().split("-");
	}
	
	/**
	 * Generates a random string made up from a UUID.
	 * @return a random string
	 */
	public static String getRandomFragment() {
		return UUID.randomUUID().toString();
	}
	

}
