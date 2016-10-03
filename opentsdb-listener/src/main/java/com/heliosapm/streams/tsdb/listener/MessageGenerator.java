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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.heliosapm.streams.chronicle.DataPoint;
import com.heliosapm.utils.time.SystemClock;
import com.heliosapm.utils.time.SystemClock.ElapsedTime;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.wire.MarshallableOut.Padding;

/**
 * <p>Title: MessageGenerator</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tsdb.listener.MessageGenerator</code></p>
 */

public class MessageGenerator implements ThreadFactory {
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	
	/** The chronicle path to write to */
	final String chroniclePath;
	/** The number of messages to send per batch */
	final int batchSize;
	/** The ms pause time between batches */
	final long pauseTime;
	/** The number of sender threads to spin up */
	final int threadCount;
	/** The thread pool */
	final ExecutorService threadPool;
	/** the thread group the threadpool threads belong to */
	final ThreadGroup tg = new ThreadGroup("MessageGenGroup");
	
	final AtomicBoolean running = new AtomicBoolean(true);
	
	/** The chronicle to write to */
	final ChronicleQueue queue;
	/** A counter for sent messages */
	final LongAdder sendCounter = new LongAdder();
	
	final AtomicInteger threadSerial = new AtomicInteger();
	
	@Override
	public Thread newThread(final Runnable r) {
		final Thread t = new Thread(tg, r, "MessageSender#" + threadSerial.incrementAndGet());
		t.setDaemon(true);
		return t;
	}

	
	/** The number of random samples for metric names and tags */
	public static final int SAMPLE_SIZE = 100;
	/** A random value generator */
	protected static final Random RANDOM = new Random(System.currentTimeMillis());

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
	

	
	
	private static final List<String> metricNames = new ArrayList<String>(SAMPLE_SIZE);
	private static final List<String> tagKeys = new ArrayList<String>(SAMPLE_SIZE);
	private static final List<String> tagValues = new ArrayList<String>(SAMPLE_SIZE);
	private static final List<Double> doubleValues = new ArrayList<Double>(SAMPLE_SIZE);
	private static final List<Long> longValues = new ArrayList<Long>(SAMPLE_SIZE);
	private static final List<byte[]> tsuidValues = new ArrayList<byte[]>(SAMPLE_SIZE);
	
	static {
		for(int i = 0; i < SAMPLE_SIZE; i++) {
			metricNames.add(getRandomFragment());
			String[] x = getRandomFragments();
			tagKeys.add(x[0]);
			tagValues.add(x[0]);
			doubleValues.add(nextPosInt(1000) + nextPosDouble());
			longValues.add(nextPosLong());
			tsuidValues.add(randomBytes(nextPosInt(60) + 4));
		}
	}
	
	String randomMetricName() {
		return metricNames.get(nextPosInt(SAMPLE_SIZE));
	}
	String randomTagKey() {
		return tagKeys.get(nextPosInt(SAMPLE_SIZE));
	}
	String randomTagValue() {
		return tagValues.get(nextPosInt(SAMPLE_SIZE));
	}
	double randomD() {
		return doubleValues.get(nextPosInt(SAMPLE_SIZE));
	}
	double randomL() {
		return longValues.get(nextPosInt(SAMPLE_SIZE));
	}
	byte[] randomTsuid() {
		return tsuidValues.get(nextPosInt(SAMPLE_SIZE));
	}

	
	Map<String, String> randomTags(final Map<String, String> map, final int count) {
		map.clear();
		while(map.size() < count) {
			map.put(randomTagKey(), randomTagValue());
		}
		return map;
	}
	
	
	/**
	 * Creates a new MessageGenerator
	 * @param chroniclePath
	 * @param batchSize
	 * @param pauseTime
	 */
	public MessageGenerator(final String chroniclePath, final int batchSize, final long pauseTime, final int threadCount) {
		this.chroniclePath = chroniclePath;
		this.batchSize = batchSize;
		this.pauseTime = pauseTime;
		this.threadCount = threadCount;
		threadPool = Executors.newFixedThreadPool(threadCount, this);
		queue = ChronicleQueueBuilder.single(this.chroniclePath).build();
	}
	
	
	public void start() {
		for(int i = 0; i < threadCount; i++) {
			final int ID = i;
			threadPool.execute(new Runnable(){				
				public void run() {
					final Thread thread = Thread.currentThread();
					final ExcerptAppender ae = queue.acquireAppender();
					final HashMap<String, String> map = new HashMap<String, String>(4);
					final boolean pause = pauseTime > 0L;
					boolean dOrL = false;
					log.info("Starting writer thread #{}", ID);
					while(true) {
						if(!running.get()) break;
						try {
							final ElapsedTime et = SystemClock.startClock();
							for(int i = 0; i < batchSize; i++) {
								final DataPoint dp;
								if(dOrL) {
									dp = DataPoint.dp(randomMetricName(), System.currentTimeMillis(), randomD(), randomTags(map, 4), randomTsuid());
								} else {
									dp = DataPoint.dp(randomMetricName(), System.currentTimeMillis(), randomL(), randomTags(map, 4), randomTsuid());
								}
								dOrL = !dOrL;
								ae.writeBytes(dp);
								ae.padToCacheAlign(Padding.SMART);
								sendCounter.increment();
							}
							log.info("Sender: {}", et.printAvg("MessagesSent", batchSize));
							if(pause) thread.join(pauseTime);
						} catch (InterruptedException iex) {
							log.info("Stopped writer thread #{}", ID);
							break;
						} catch (Exception ex) {
							if(running.get()) {
								log.error("Processing error in writer", ex);
							}
						}
					}
				}
			});
		}
	}
	
	public void stop() {
		running.set(false);
		tg.interrupt();
		threadPool.shutdown();
		try {
			threadPool.awaitTermination(5000, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			// Won't happen
		}
		try { queue.close(); } catch (Exception x) {/* No Op */}
	}
	
	public long getSentCount() {
		return sendCounter.longValue();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
