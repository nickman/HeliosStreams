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
package com.heliosapm.streams.chronicle;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.heliosapm.streams.buffers.ByteBufMarshallable;
import com.heliosapm.streams.common.metrics.SharedMetricsRegistry;
import com.heliosapm.streams.common.naming.AgentName;
import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.StreamedMetricValue;
import com.heliosapm.utils.collections.Props;
import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.jmx.JMXManagedThreadFactory;

import io.netty.buffer.ByteBuf;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireType;

/**
 * <p>Title: MessageQueue</p>
 * <p>Description: A disk persistent message queue to separate kafka consumers from the actual processors.</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.chronicle.MessageQueue</code></p>
 */

public class MessageQueue implements Closeable, StoreFileListener, Runnable {
	/** A map of MessageQueues keyed by the name */
	private static final NonBlockingHashMap<String, MessageQueue> instances = new NonBlockingHashMap<String, MessageQueue>(16); 
	
	private static final boolean IS_WIN = System.getProperty("os.name").toLowerCase().contains("windows");
	
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** The extracted config properties */
	protected final Properties queueConfig;
	/** The chronicle queue */
	protected final ChronicleQueue queue;
	/** The message queue logical name */
	protected final String queueName;
	/** The base directory */
	protected final File baseQueueDirectory;
	/** The message listener that will handle messages read back out of the queue */
	protected final MessageListener listener;
	/** The reader startup latch */
	protected final CountDownLatch startLatch;
	
	/** The number of reader threads */
	protected final int readerThreads;
	/** The reader thread thread pool */
	protected final ExecutorService threadPool;
	/** The thread pool's thread group */
	protected final ThreadGroup threadGroup;
	
	/** The idle pause time in ms. */
	protected final long idlePauseTime;
	/** The stop check count which is the number of records read before the reader checks to see if a stop has been called */
	protected final int stopCheckCount;
	/** The block size for the chronicle queue */
	protected final int blockSize;
	
	/** The roll cycle for the chronicle queue */
	protected final RollCycles rollCycle;
	
	/** A map to queue the windows rolled files so we can keep trying to delete them */
	protected final Map<String, File> pendingDeletes = IS_WIN ? new ConcurrentHashMap<String, File>() : null;
	/** The thread that periodically attempts to delete rolled windows files */
	protected final Thread pendingDeleteThread;
	
	/** The keep running flag for reader threads */
	protected final AtomicBoolean keepRunning = new AtomicBoolean(true);
	
	/** A counter of deleted roll files */
	protected final Counter deletedRollFiles;
	/** A periodic counter of chronicle reads */
	protected final Counter chronicleReads;
	/** A periodic counter of chronicle writes */
	protected final Counter chronicleWrites;
	/** A cummulative counter of read errors */
	protected final Counter chronicleReadErrs;
	/** A gauge of the backlog in the queue */
	protected final Gauge<Long> queueBacklog;
	
	
	
	

	
	private static final ThreadLocal<ByteBufMarshallable> container = new ThreadLocal<ByteBufMarshallable>() {
		@Override
		protected ByteBufMarshallable initialValue() {
			return new ByteBufMarshallable();
		}
	};
	
	
	/** The config key name for the number of reader threads */
	public static final String CONFIG_READER_THREADS = "reader.threads";
	/** The default number of reader threads */
	public static final int DEFAULT_READER_THREADS = 1;
	
	/** The config key name for the chronicle parent directory */
	public static final String CONFIG_BASE_DIR = "chronicle.dir";
	/** The default number of reader threads */
	public static final String DEFAULT_BASE_DIR = System.getProperty("user.home") + File.separator + ".messageQueue";
	
	/** The config key name for the reader idle pause time  in ms. */
	public static final String CONFIG_IDLE_PAUSE = "reader.idle.pause";
	/** The default reader idle pause time in ms. */
	public static final long DEFAULT_IDLE_PAUSE = 500L;
	
	/** The config key name for the queue's block size */
	public static final String CONFIG_BLOCK_SIZE = "chronicle.blocksize";
	/** The default queue block size */
	public static final int DEFAULT_BLOCK_SIZE = 1296 * 1024;
	
	/** The config key name for the queue roll cycle */
	public static final String CONFIG_ROLL_CYCLE = "chronicle.rollcycle";
	/** The default queue roll cycle */
	public static final RollCycles DEFAULT_ROLL_CYCLE = RollCycles.HOURLY;
	
	
	
	/** The config key name for the reader stop check count */
	public static final String CONFIG_STOPCHECK_COUNT = "reader.stopcheck";
	/** The default reader stop check count. */
	public static final int DEFAULT_STOPCHECK_COUNT = 500;
	
	/**
	 * Acquires the named MessageQueue
	 * @param name the message queue's logical name
	 * @param listener The message listener that will handle messages read back out of the queue
	 * @param config The message queue's config
	 * @return the named MessageQueue
	 */
	public static MessageQueue getInstance(final String name, final MessageListener listener, final Properties config) {
		if(name==null || name.trim().isEmpty()) throw new IllegalArgumentException("The passed name was null or empty");
		final String key = name.trim();
		MessageQueue q = instances.get(key);
		if(q==null) {
			synchronized(instances) {
				q = instances.get(key);
				if(q==null) {
					q = new MessageQueue(key, listener, config);
					instances.put(key, q);
				}
			}
		}
		return q;
	}
	
	/**
	 * Creates a new MessageQueue
	 * @param name the message queue's logical name
	 * @param listener The message listener that will handle messages read back out of the queue
	 * @param config The message queue's config
	 */
	private MessageQueue(final String name, final MessageListener listener, final Properties config) {
		if(listener==null) throw new IllegalArgumentException("The passed listener was null");
		queueName = name.trim();
		deletedRollFiles = SharedMetricsRegistry.getInstance().counter("chronicle.rollfile.deleted.queue=" + queueName);
		chronicleReads = SharedMetricsRegistry.getInstance().counter("chronicle.reads.queue=" + queueName);
		chronicleWrites = SharedMetricsRegistry.getInstance().counter("chronicle.writes.queue=" + queueName);
		chronicleReadErrs = SharedMetricsRegistry.getInstance().counter("chronicle.read.errors.queue=" + queueName);
		queueBacklog = SharedMetricsRegistry.getInstance().gauge("chronicle.backlog.queue=" + queueName, new Callable<Long>(){
			@Override
			public Long call() throws Exception {				
				return chronicleWrites.getCount() - chronicleReads.getCount();
			}
		});
		queueConfig = Props.extract(queueName, config, true, false);
		this.listener = listener;
		blockSize = ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_BLOCK_SIZE, DEFAULT_BLOCK_SIZE, queueConfig);
		readerThreads = ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_READER_THREADS, DEFAULT_READER_THREADS, queueConfig);
		idlePauseTime = ConfigurationHelper.getLongSystemThenEnvProperty(CONFIG_IDLE_PAUSE, DEFAULT_IDLE_PAUSE, queueConfig);
		stopCheckCount = ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_STOPCHECK_COUNT, DEFAULT_STOPCHECK_COUNT, queueConfig);
		rollCycle = ConfigurationHelper.getEnumSystemThenEnvProperty(RollCycles.class, CONFIG_ROLL_CYCLE, DEFAULT_ROLL_CYCLE, queueConfig);
		final String dirName = ConfigurationHelper.getSystemThenEnvProperty(CONFIG_BASE_DIR, DEFAULT_BASE_DIR, queueConfig);		
		final File parentDir = new File(dirName);
		baseQueueDirectory = new File(parentDir, name);
		if(!baseQueueDirectory.exists()) {
			baseQueueDirectory.mkdirs();
		}
		if(!baseQueueDirectory.isDirectory()) {
			throw new IllegalArgumentException("Cannot create configured baseQueueDirectory: [" + baseQueueDirectory + "]");
		}
		if(IS_WIN) {
			pendingDeleteThread = new Thread(queueName + "RolledFileDeleter") {
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
									deletedRollFiles.inc();
									log.info("Deleted pending roll file [{}], size [{}} bytes", entry.getKey(), size);
									pendingDeletes.remove(entry.getKey());
								}
							}
						}
					}
				}
			};
			pendingDeleteThread.setDaemon(true);
			pendingDeleteThread.start();
		} else {
			pendingDeleteThread = null;
		}
		IOTools.deleteDirWithFiles(baseQueueDirectory, 2);
		queue = SingleChronicleQueueBuilder.binary(baseQueueDirectory)
			.blockSize(blockSize)
			.rollCycle(rollCycle)
			.storeFileListener(this)
			.wireType(WireType.BINARY)
			.build();
		queue.firstIndex();
		startLatch = new CountDownLatch(readerThreads);
		final JMXManagedThreadFactory threadFactory = (JMXManagedThreadFactory)JMXManagedThreadFactory.newThreadFactory(name + "ReaderThread", true);
		threadPool = Executors.newFixedThreadPool(readerThreads, threadFactory);
		threadGroup = threadFactory.getThreadGroup();
		for(int i = 0; i < readerThreads; i++) {
			threadPool.execute(this);
		}
//		try {
//			if(!startLatch.await(10, TimeUnit.SECONDS)) {
//				throw new Exception("Reader threads failed to start");
//			}
//		} catch (InterruptedException iex) {
//			try { close(); } catch (Exception x) {/* No Op */}
//			throw new RuntimeException("Thread interrupted while waiting on reader thread startup", iex);
//		} catch (Exception ex) {
//			try { close(); } catch (Exception x) {/* No Op */}
//			throw new RuntimeException("Timeout waiting on reader thread startup", ex);
//		}
	}
	
	/**
	 * Closes this message queue
	 * @throws IOException will not be thrown
	 */
	@Override
	public void close() throws IOException {
		if(instances.remove(queueName)!=null) {
			keepRunning.set(false);
			try { threadPool.shutdown(); } catch (Exception x) {/* No Op */}
			try { threadPool.awaitTermination(10, TimeUnit.SECONDS); } catch (Exception x) {/* No Op */}
			if(!threadPool.isTerminated()) {
				threadGroup.interrupt();
				try { threadPool.shutdownNow(); } catch (Exception x) {/* No Op */}
			}
			try { queue.close(); } catch (Exception x) {/* No Op */}
		}
	}
	
	
	public static void main(String[] args) {
//		System.setProperty("io.netty.leakDetection.level", "advanced");
		System.setProperty("Test.chronicle.rollcycle", RollCycles.MINUTELY.name());
		
		final ThreadLocalRandom tlr = ThreadLocalRandom.current();
		
		final MetricRegistry mr = new MetricRegistry();
		final Meter listenerEvents = mr.meter("listener.events");
		final Timer writerTime = mr.timer("writer.time");
		final ConsoleReporter cr = ConsoleReporter
			.forRegistry(mr)
			.convertDurationsTo(TimeUnit.MICROSECONDS)
			.convertRatesTo(TimeUnit.SECONDS)
			.outputTo(System.err)
			.build();
		cr.start(5, TimeUnit.SECONDS);
		final MessageListener listener = new MessageListener() {
			/**
			 * {@inheritDoc}
			 * @see com.heliosapm.streams.chronicle.MessageListener#onMetric(com.heliosapm.streams.metrics.StreamedMetric)
			 */
			@Override
			public void onMetric(final ByteBuf streamedMetric) {
				listenerEvents.mark();
				streamedMetric.release();
				
				
			}
		};
		final MessageQueue mq = MessageQueue.getInstance("Test", listener, System.getProperties());
		final Thread producer = new Thread() {
			public void run() {
				try {
					for(int i = 0; i < Integer.MAX_VALUE; i++) {
						final Context ctx = writerTime.time();
						mq.writeEntry(new StreamedMetricValue(System.currentTimeMillis(), tlr.nextDouble(), "foo.bar", AgentName.getInstance().getGlobalTags()));
						ctx.stop();
					}
				} catch (Exception ex) {
					if(ex instanceof InterruptedException) {
						mq.log.info("Producer Thread is stopping");
					}
				}
			}
		};
		producer.setDaemon(true);
		producer.start();
		final AtomicBoolean closed = new AtomicBoolean(false);
		StdInCommandHandler.getInstance().registerCommand("shutdown", new Runnable(){
			public void run() {
				if(closed.compareAndSet(false, true)) {
					mq.log.info(">>>>> Stopping MessageQueue...");
					producer.interrupt();
					try { mq.close(); } catch (Exception x) {/* No Op */}
					mq.log.info("<<<<< MessageQueue Stopped");
					System.exit(1);
				}
			}
		}).shutdownHook("shutdown").run();
	}
	
	public static void log(Object msg) {
		System.out.println(msg);
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.lang.Runnable#run()
	 */
	public void run() {
		final ExcerptTailer tailer = queue.createTailer();
		final ByteBufMarshallable smm = new ByteBufMarshallable(); 
		startLatch.countDown();
		while(keepRunning.get()) {			
			try {
				long processed = 0L;
				long reads = 0L;
				while(tailer.readBytes(smm)) {
					chronicleReads.inc();
					reads++;
					final ByteBuf sm = smm.getAndNullByteBuf();
					if(sm!=null) {
						listener.onMetric(sm);
//						sm.release();
						processed++;
						if(processed==stopCheckCount) {
							processed = 0;
							if(!keepRunning.get()) break;
						}
					}
				}
				if(reads==0) {
					Jvm.pause(idlePauseTime);
				}
				reads = 0;
			} catch (Exception ex) {
				if(ex instanceof InterruptedException) {
					if(keepRunning.get()) {
						if(Thread.interrupted()) Thread.interrupted();
					}
					log.info("Reader Thread [{}] shutting down", Thread.currentThread());
				} else {
					log.warn("Unexpected exception in reader thread",  ex);
				}
			}
		}
	}
	
	
	
	/**
	 * Writes a bytes marshallable message to the queue
	 * @param sm the streamed metric to write
	 */
	public void writeEntry(final StreamedMetric sm) {
		queue.acquireAppender().writeBytes(container.get().setByteBuff(sm.toByteBuff()));
		chronicleWrites.inc();
	}


	
	


	/**
	 * {@inheritDoc}
	 * @see net.openhft.chronicle.queue.impl.StoreFileListener#onReleased(int, java.io.File)
	 */
	@Override
	public void onReleased(final int cycle, final File file) {
		final long size = file.length();
		final String name = file.getAbsolutePath();
		if(IS_WIN) {
			pendingDeletes.put(name, file);
			log.info("Added RollFile [{}], size:[{}] bytes to pending delete queue", name, size);
		} else {
			if(file.delete()) {
				deletedRollFiles.inc();
				log.info("Deleted RollFile [{}], size:[{}] bytes", name, size);
			} else {
				log.warn("Failed to Delete RollFile [{}], size:[{}] bytes", name, size);
			}					
		}
	}

}
