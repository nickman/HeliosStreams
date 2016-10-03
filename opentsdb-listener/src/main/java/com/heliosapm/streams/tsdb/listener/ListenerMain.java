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

import com.heliosapm.streams.chronicle.DataPoint;
import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.time.SystemClock;
import com.heliosapm.utils.time.SystemClock.ElapsedTime;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.StoreFileListener;

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
	
	@Override
	public Thread newThread(final Runnable r) {
		final Thread t = new Thread(tg, r, "MessageListener#" + threadSerial.incrementAndGet());
		t.setDaemon(true);
		return t;
	}
	
	
	/** A random value generator */
	protected static final Random RANDOM = new Random(System.currentTimeMillis());
	
	public ListenerMain(final String basePath, final int threadCount, final int batchSize, final long pauseTime) {		
		this.basePath = basePath;
		this.threadCount = threadCount;
		this.batchSize = batchSize;
		this.pauseTime = pauseTime;
//        this.rollCycle = RollCycles.DAILY;
//        this.blockSize = 64L << 20;
//        this.path = path;
//        this.wireType = WireType.BINARY_LIGHT;
//        this.epoch = 0;
//        this.bufferCapacity = 2 << 20;
//        this.indexSpacing = -1;
//        this.indexCount = -1;
		threadPool = Executors.newFixedThreadPool(threadCount, this);		
		queue = ChronicleQueueBuilder			
			.single(basePath)	
//			.rollCycle(RollCycles.MINUTELY)
			.build();
	}

	
	

	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		final String path = "d:\\chronicle\\datapoints";
		final int batchSize = 1000000;
		final long pauseTime = 0L;
		final File dir = new File(path);
		final int threadCount = 1;
		if(dir.isDirectory()) {
			for(File f: dir.listFiles()) {
				if(f.isFile()) f.delete();
			}
		}
		final ListenerMain listener = new ListenerMain(path, threadCount, batchSize, pauseTime);
		
		final MessageGenerator mg = new MessageGenerator(path, batchSize, pauseTime, threadCount);		
		listener.start();
		mg.start();
		final AtomicBoolean b = new AtomicBoolean(false);
		StdInCommandHandler.getInstance().registerCommand("stop", new Runnable(){
			public void run() {
				b.set(true);
				mg.stop();
				listener.stop();
				listener.log.info("MESSAGE TOTAL:\n\tSent:{}\n\tReceived:{}", mg.getSentCount(), listener.receiveCounter.longValue());
				System.exit(0);
			}
		}).runAsync(true);
		while(!b.get()) {
			listener.log.info("--------> MSGS REC: {}", listener.receiveCounter.longValue());
			SystemClock.sleep(5000);
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
