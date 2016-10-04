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
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Meter;
import com.heliosapm.streams.chronicle.DataPoint;
import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.time.SystemClock;
import com.heliosapm.utils.time.SystemClock.ElapsedTime;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

/**
 * <p>Title: ListenerMain</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tsdb.listener.ListenerMain</code></p>
 */

public class ListenerMain implements Closeable, StoreFileListener, Runnable, ThreadFactory {
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** the thread group the threadpool threads belong to */
	final ThreadGroup tg = new ThreadGroup("MessageListGroup");
	
	final AtomicBoolean running = new AtomicBoolean(true);

//	/** The in/out queue */
//	protected final MessageQueue<AbstractMarshallable> q;
	
	
	/*
	 * Configurables:
	 * ==============
	 * chronicle directory | tmp
	 * chronicle name | tsdb-rtpublisher
	 * blockSize
	 * buffer capacity
	 * buffered(boolean)
	 * RollCycle
	 * 
	 */
	
	
	/*
	 * FX:
	 * ===
	 * pauser in tailer loop
	 * head chronicle writes with byte indicating type
	 * store file listener & delete retry loop
	 * 
	 */
	
	protected final String basePath;
	protected final ChronicleQueue queue;
	final int threadCount;
	protected final ExecutorService threadPool;
	final AtomicInteger threadSerial = new AtomicInteger();
	/** A counter for sent messages */
	final LongAdder receiveCounter = new LongAdder();
	final int batchSize;
	final long pauseTime;
	final Meter msgMeter = new Meter();
	final MessageGenerator mg;
	final ChronicleMap<byte[], DataPoint> dataPointMap;
	
	@Override
	public Thread newThread(final Runnable r) {
		final Thread t = new Thread(tg, r, "MessageListener#" + threadSerial.incrementAndGet());
		t.setDaemon(true);
		return t;
	}
	
	
	/** A random value generator */
	protected static final Random RANDOM = new Random(System.currentTimeMillis());
	
	public ListenerMain(final String basePath, final int threadCount, final int batchSize, final long pauseTime, final MessageGenerator mg) {		
		this.basePath = basePath;		
		this.threadCount = threadCount;
		this.batchSize = batchSize;
		this.pauseTime = pauseTime;
		this.mg = mg;
		final File dataPointMapDir = new File(this.basePath + File.separator + "datapoints.db");
		try {
			final DataPoint sample = mg.randomDataPoint();
			dataPointMap = ChronicleMapBuilder.of(byte[].class, DataPoint.class)
				.averageKey(sample.getTsuid())
				.averageValue(sample)
				.entries(mg.tsuidIndex.longSize())
				.createPersistedTo(dataPointMapDir);
				
			log.info("Created DataPointMap persisted at [{}]", dataPointMapDir);
			dataPointMap.clear();
		} catch (Exception ex) {
			log.error("Failed to create datapoint map at {}", dataPointMapDir, ex);
			throw new RuntimeException("Failed to create datapoint map", ex);
		}
			
			
//        this.rollCycle = RollCycles.DAILY;
//        this.blockSize = 64L << 20;
//        this.path = path;
//        this.wireType = WireType.BINARY_LIGHT;
//        this.epoch = 0;
//        this.bufferCapacity = 2 << 20;
//        this.indexSpacing = -1;
//        this.indexCount = -1;
		threadPool = Executors.newFixedThreadPool(threadCount, this);		
		queue = new SingleChronicleQueueBuilder(basePath)			
			.rollCycle(RollCycles.MINUTELY)
			.storeFileListener(this)
			.build();
	}

	
	

	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
//		final String path = "d:\\chronicle\\datapoints";
		final String path = "/tmp/chronicle/datapoints";
		final int batchSize = 1000000;
		final long pauseTime = 0L;
		final File dir = new File(path);
		final int threadCount = 1;
		if(dir.isDirectory()) {
			for(File f: dir.listFiles()) {
				if(f.isFile()) f.delete();
			}
		}
		final MessageGenerator mg = new MessageGenerator(path, batchSize, pauseTime, threadCount);
		final ListenerMain listener = new ListenerMain(path, threadCount, batchSize, pauseTime, mg);
		
				
		listener.start();
		mg.start();
		final AtomicBoolean b = new AtomicBoolean(false);
		StdInCommandHandler.getInstance().registerCommand("stop", new Runnable(){
			public void run() {
				b.set(true);
				mg.stop();
				listener.stop();
				listener.log.info("MESSAGE TOTAL:\n\tDPMap:{}\n\tSent:{}\n\tReceived:{}\n\tRate:{}\n\tPending: {}", listener.dataPointMap.size(), mg.getSentCount(), listener.receiveCounter.longValue(), listener.msgMeter.getMeanRate(), mg.getPendingMessages());
				System.exit(0);
			}
		}).runAsync(true);
		while(!b.get()) {
			final int pending = mg.getPendingMessages();
			final double rate = listener.msgMeter.getOneMinuteRate();
			final int maxPending = batchSize * 3;
			listener.log.info("--------> DPOINTS: {}, RATE: {}, MSGS REC: {}", listener.dataPointMap.size(), rate, listener.receiveCounter.longValue());
			if(pending > maxPending) {
				System.err.println("MAX PENDING EXCEEDED");
				System.exit(-1);
			}
			SystemClock.sleep(5000);
		}
	}
	
	public void stop() {
		running.set(false);
		tg.interrupt();
		threadPool.shutdown();
		try {
			if(!threadPool.awaitTermination(15000, TimeUnit.MILLISECONDS)) {
				final int tasks = threadPool.shutdownNow().size();
				log.warn("Timed out waiting on threadpool shutdown. Pending: {}", tasks);				
			}
		} catch (InterruptedException e) {
			// Won't happen
		}
		try { dataPointMap.close(); } catch (Exception x) {/* No Op */}
	}

	
	public void run() {
		
	}
	
	
	public void start() {
		for(int i = 0; i < threadCount; i++) {
			final int ID = i;
			threadPool.execute(new Runnable(){				
				public void run() {
					final Thread thread = Thread.currentThread();
					final ExcerptTailer tailer = queue.createTailer();
					final boolean pause = pauseTime > 0L;
					log.info("Starting reader thread #{}", ID);
					int messagesRead = 0;
					ElapsedTime et = SystemClock.startClock();
					while(true) {
						if(!running.get()) break;
						try {							
							final DataPoint dp = DataPoint.get();
							while(tailer.readBytes(dp.reset())) {								
								if(dp.getTags().size()!=4) {
									log.error("Incorrect Tag Count: {}" , dp);
								}
								msgMeter.mark();
//								mg.confirm(dp.longHashCode());
								dataPointMap.put(dp.getTsuid(), dp);
								receiveCounter.increment();
								messagesRead++;
								if(messagesRead==batchSize) {
									log.info("Receiver: {}", et.printAvg("MessagesReceived", batchSize));
									messagesRead = 0;
									et = SystemClock.startClock();
								}
							}						
							if(pause) thread.join(pauseTime);
						} catch (InterruptedException iex) {
							log.info("Stopped reader thread #{}", ID);
							break;
						} catch (Exception ex) {
							if(running.get()) {
								log.error("Processing error in reader", ex);
							}							
						}
					}
				}
			});			
		}
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
	 * {@inheritDoc}
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	/**
	 * Returns a random boolean
	 * @return a random boolean
	 */
	public static boolean nextBoolean() {
		return RANDOM.nextBoolean();
	}
	
	/**
	 * Returns a random positive int
	 * @return a random positive int
	 */
	public static int nextPosInt() {
		return Math.abs(RANDOM.nextInt());
	}
	/**
	 * Returns a random positive int within the bound
	 * @param bound the bound on the random number to be returned. Must be positive. 
	 * @return a random positive int
	 */
	public static int nextPosInt(int bound) {
		return Math.abs(RANDOM.nextInt(bound));
	}
	

}
