package com.heliosapm.streams.opentsdb;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.heliosapm.streams.chronicle.TSDBMetricMeta;
import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.time.SystemClock;
import com.heliosapm.utils.time.SystemClock.ElapsedTime;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
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
	/** The random used to generate random values */
	protected static final ThreadLocalRandom RANDOM = ThreadLocalRandom.current();
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

	
	public RandomDataPointGenerator(final RTPublisher publisher, final String name, final int sampleSize, final int loops, final long sleep, final int sleepFreq, final boolean text) {
		this.name = (name==null || name.trim().isEmpty()) ? ("RDPG@" + System.identityHashCode(this)) : name;
		log = LogManager.getLogger(getClass().getName() + "." + this.name);
		this.loops = loops < 1 ? Integer.MAX_VALUE : loops;
		this.sleep = sleep;
		this.sleepFreq = sleepFreq;
		this.text = text;
		this.publisher = publisher;
		metricMetas = ChronicleMapBuilder
				.of(byte[].class, TSDBMetricMeta.class)				
				.averageKeySize(128)
				.averageValueSize(text ? 800 : 281)
				.entries(sampleSize)
				.create();
		log.info("Generator [{}] created. Generating Meta Samples...", this.name);
		final ElapsedTime et = SystemClock.startClock();
		IntStream.range(0, sampleSize).parallel().forEach(i -> {
			try {
				final ByteBuffer buff = ByteBuffer.allocate(8);
				final HashMap<String, String> tags = new HashMap<String, String>(4);
				final HashMap<String, byte[]> tagKeys = new HashMap<String, byte[]>(4);
				final HashMap<String, byte[]> tagValues = new HashMap<String, byte[]>(4);
				for(int x = 0; x < 4; x++) {
					final String key = getRandomFragment();
					final String value = getRandomFragment();
					tags.put(key, value);
					tagKeys.put(key, key.getBytes());
					tagValues.put(value, value.getBytes());
				}
				final String metricName = getRandomFragment();
				final byte[] muid = metricName.getBytes();
				final byte[] tsuid = randomBytes(128);
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
	
	public static void main(String[] args) {
		try {
			final Config cfg = new Config(true);
			final TSDB tsdb = new TSDB(cfg);
			final TSDBChronicleEventPublisher pub = new TSDBChronicleEventPublisher();
			final RandomDataPointGenerator r = new RandomDataPointGenerator(pub, "Test", 512000, 1, 1, 1, false);
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
		}
	}
	
	public boolean isStarted() {
		return keepRunning.get();
	}
	
	public void stop() {
		if(keepRunning.compareAndSet(true, false)) {
			runThread.interrupt();
		}
	}
	
	public void run() {
		log.info("Starting DP generator");		
		try {
			for(int i = 0; i < loops; i++) {
				log.info("Starting loop #{}...", i);
				final long now = System.currentTimeMillis();
				metricMetas.values().stream().forEach(mm -> 
					publisher.publishDataPoint(mm.getMetricName(), now, nextPosDouble(), mm.getTags(), mm.getTsuid())
				);
				SystemClock.sleep(sleep);
			}
			log.info("Instance completed");
		} catch (Exception ex) {
			if(InterruptedException.class.isInstance(ex)) {
				log.info("Stopping instance...");
			}
		}
	}
	
	
	/**
	 * Returns a random positive int within the bound
	 * @param bound the bound on the random number to be returned. Must be positive. 
	 * @return a random positive int
	 */
	public static int nextPosInt(int bound) {
		return Math.abs(RANDOM.nextInt(bound));
	}

	/**
	 * Returns a random positive long
	 * @return a random positive long
	 */
	public static long nextPosLong() {
		return Math.abs(RANDOM.nextLong());
	}

	/**
	 * Returns a random positive double
	 * @return a random positive double
	 */
	public static double nextPosDouble() {
		return Math.abs(RANDOM.nextDouble());
	}
	
	public static byte[] randomBytes(int size) {
		final byte[] bytes = new byte[size];
		RANDOM.nextBytes(bytes);
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
