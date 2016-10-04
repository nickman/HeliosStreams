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

import java.io.File;
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
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;

import com.heliosapm.streams.chronicle.DataPoint;
import com.heliosapm.utils.time.SystemClock;
import com.heliosapm.utils.time.SystemClock.ElapsedTime;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.set.ChronicleSet;
import net.openhft.chronicle.set.ChronicleSetBuilder;
import net.openhft.chronicle.wire.MarshallableOut.Padding;

/**
 * <p>Title: MessageGenerator</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tsdb.listener.MessageGenerator</code></p>
 */

public class MessageGenerator implements ThreadFactory, StoreFileListener {
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
	
	/** Unconfirmed datapoints */
	final NonBlockingHashMapLong<DataPoint> pendingDataPoints = new NonBlockingHashMapLong<DataPoint>(1024 * 500, true);
	
	final AtomicInteger threadSerial = new AtomicInteger();
	
	@Override
	public Thread newThread(final Runnable r) {
		final Thread t = new Thread(tg, r, "MessageSender#" + threadSerial.incrementAndGet());
		t.setDaemon(true);
		return t;
	}
	
		

	
	/** The number of random samples for metric names and tags */
	public static final int SAMPLE_SIZE = 10000;
	/** A random value generator */
	protected static final Random RANDOM = new Random(System.currentTimeMillis());

	static final ChronicleSet<byte[]> tsuidIndex = ChronicleSetBuilder
			.of(byte[].class)
			.averageKey(randomBytes(nextPosInt(60) + 4))
			.entries(SAMPLE_SIZE)
			.create();

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
	 * {@inheritDoc}
	 * @see net.openhft.chronicle.queue.impl.StoreFileListener#onReleased(int, java.io.File)
	 */
	@Override
	public void onReleased(final int cycle, final File file) {
		final boolean del = file.delete();
		log.info("Released File: [{}], cycle: {}, deleted: {}", file, cycle, del);
		
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
	

	
	
	private static final NonBlockingHashMapLong<DataPoint> dataPointSamples = new NonBlockingHashMapLong<DataPoint>(SAMPLE_SIZE, true); 
	static {
		for(int i = 0; i < SAMPLE_SIZE; i++) {
			final HashMap<String, String> tags = new HashMap<String, String>(4);
			for(int y = 0; y < 4; y++) {
				String[] frags = getRandomFragments();
				tags.put(frags[0], frags[1]);
			}
			byte[] tsuid = randomBytes(nextPosInt(60) + 4);
			while(tsuidIndex.contains(tsuid)) {
				tsuid = randomBytes(nextPosInt(60) + 4);
			}
			tsuidIndex.add(tsuid);
			
			
			if(i%2==0) {				
				dataPointSamples.put(i, DataPoint.dp(getRandomFragment(), System.currentTimeMillis(), (nextPosInt(1000) + nextPosDouble()), tags, tsuid).clone());
			} else {
				dataPointSamples.put(i, DataPoint.dp(getRandomFragment(), System.currentTimeMillis(), nextPosLong(), tags, tsuid).clone());
			}
		}
		System.err.println("TSUID Index: " + tsuidIndex.size() );
	}
	
	public static DataPoint randomDataPoint() {
		final DataPoint dp =  dataPointSamples.get(nextPosInt(SAMPLE_SIZE));
		if(dp.isDoubleType()) {
			dp.update((nextPosInt(1000) + nextPosDouble()));
		} else {
			dp.update(nextPosLong());
		}
		return dp;
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
		queue = ChronicleQueueBuilder.single(this.chroniclePath)
				.rollCycle(RollCycles.MINUTELY)
				.storeFileListener(this)
				.build();
	}
	
	
	public void start() {
		for(int i = 0; i < threadCount; i++) {
			final int ID = i;
			threadPool.execute(new Runnable(){				
				public void run() {
					final Thread thread = Thread.currentThread();
					final ExcerptAppender ae = queue.acquireAppender();
					ae.padToCacheAlign(Padding.SMART);
					final HashMap<String, String> map = new HashMap<String, String>(4);
					final boolean pause = pauseTime > 0L;
					log.info("Starting writer thread #{}", ID);
					while(true) {
						if(!running.get()) break;
						try {
							final ElapsedTime et = SystemClock.startClock();
							for(int i = 0; i < batchSize; i++) {
								final DataPoint dp = randomDataPoint();
								//pendingDataPoints.put(dp.longHashCode(), dp.clone());
								ae.writeBytes(dp);								
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
	
	public int getPendingMessages() {
		return pendingDataPoints.size();
	}
	
	public void confirm(final long longHashCode) {
		pendingDataPoints.remove(longHashCode);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
