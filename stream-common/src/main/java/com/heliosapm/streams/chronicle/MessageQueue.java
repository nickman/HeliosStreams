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
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.heliosapm.streams.common.metrics.SharedMetricsRegistry;
import com.heliosapm.streams.common.naming.AgentName;
import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.StreamedMetricValue;
import com.heliosapm.utils.collections.Props;
import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.jmx.JMXManagedThreadFactory;

import net.openhft.chronicle.bytes.BytesRingBufferStats;
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

public class MessageQueue implements Closeable, Consumer<BytesRingBufferStats>, StoreFileListener, Runnable {
	/** A map of MessageQueues keyed by the name */
	private static final NonBlockingHashMap<String, MessageQueue> instances = new NonBlockingHashMap<String, MessageQueue>(16); 
	
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
	
	/** The keep running flag for reader threads */
	protected final AtomicBoolean keepRunning = new AtomicBoolean(true);
	
	/** A counter of deleted roll files */
	protected final Counter deletedRollFiles = SharedMetricsRegistry.getInstance().counter("chronicle.rollfile.deleted");
	/** A periodic counter of chronicle reads */
	protected final Counter chronicleReads = SharedMetricsRegistry.getInstance().counter("chronicle.reads");
	/** A periodic counter of chronicle writes */
	protected final Counter chronicleWrites = SharedMetricsRegistry.getInstance().counter("chronicle.writes");
	/** A cummulative counter of read errors */
	protected final Counter chronicleReadErrs = SharedMetricsRegistry.getInstance().counter("chronicle.read.errors");
	
	

	
	private static final ThreadLocal<StreamedMetricMarshallable> container = new ThreadLocal<StreamedMetricMarshallable>() {
		@Override
		protected StreamedMetricMarshallable initialValue() {
			return new StreamedMetricMarshallable();
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
		queueConfig = Props.extract(name, config, true, false);
		this.listener = listener;
		final String dirName = ConfigurationHelper.getSystemThenEnvProperty(CONFIG_BASE_DIR, DEFAULT_BASE_DIR, queueConfig);
		final File parentDir = new File(dirName);
		baseQueueDirectory = new File(parentDir, name);
		if(!baseQueueDirectory.exists()) {
			baseQueueDirectory.mkdirs();
		}
		if(!baseQueueDirectory.isDirectory()) {
			throw new IllegalArgumentException("Cannot create configured baseQueueDirectory: [" + baseQueueDirectory + "]");
		}
//		for(File f : baseQueueDirectory.listFiles()) {
//			IOTools.shallowDeleteDirWithFiles(f);
//		}
		IOTools.deleteDirWithFiles(baseQueueDirectory, 2);
		queue = SingleChronicleQueueBuilder.binary(baseQueueDirectory)
//			.blockSize(8096)
			.onRingBufferStats(this)
			.rollCycle(RollCycles.MINUTELY)
			.storeFileListener(this)
			.wireType(WireType.BINARY)
			.build();
		queue.firstIndex();
		readerThreads = ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_READER_THREADS, DEFAULT_READER_THREADS, queueConfig);
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
			public void onMetric(final StreamedMetric streamedMetric) {
				listenerEvents.mark();
			}
		};
		
		final MessageQueue mq = MessageQueue.getInstance("Test", listener, null);
		for(int i = 0; i < Integer.MAX_VALUE; i++) {
			final Context ctx = writerTime.time();
			mq.writeEntry(new StreamedMetricValue(System.currentTimeMillis(), tlr.nextDouble(), "foo.bar", AgentName.getInstance().getGlobalTags()));
			ctx.stop();
		}
		StdInCommandHandler.getInstance().registerCommand("shutdown", new Runnable(){
			public void run() {
				mq.log.info(">>>>> Stopping MessageQueue...");
				try { mq.close(); } catch (Exception x) {/* No Op */}
				mq.log.info("<<<<< MessageQueue Stopped");
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

		final StreamedMetricMarshallable smm = container.get();
		startLatch.countDown();
		while(keepRunning.get()) {
			try {
				long processed = 0L;
				long reads = 0L;
				while(tailer.readBytes(smm)) {
					if(tailer.readBytes(smm)) {
						reads++;
						final StreamedMetric sm = smm.getAndNullStreamedMetric();
						if(sm!=null) {
							listener.onMetric(sm);
							processed++;
							if(processed==1000) break;
						}
					}
				}
				if(reads==0) {
					Jvm.pause(100);
				}
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
		queue.acquireAppender().writeBytes(sm);
//		log.info("Wrote entry");
	}


	/**
	 * {@inheritDoc}
	 * @see java.util.function.Consumer#accept(java.lang.Object)
	 */
	@Override
	public void accept(final BytesRingBufferStats t) {
		chronicleReads.inc(t.getAndClearReadCount());
		chronicleWrites.inc(t.getAndClearWriteCount());		
		log.info("Reads: [{}], Writes: [{}]", chronicleReads.getCount(), chronicleWrites.getCount());
	}


	/**
	 * {@inheritDoc}
	 * @see net.openhft.chronicle.queue.impl.StoreFileListener#onReleased(int, java.io.File)
	 */
	@Override
	public void onReleased(final int cycle, final File file) {
		final long size = file.length();
		final String name = file.getAbsolutePath();
		if(file.delete()) {
			deletedRollFiles.inc();
			log.info("Deleted RollFile [{}], size:[{}] bytes", name, size);
		} else {
			log.warn("Failed to Delete RollFile [{}], size:[{}] bytes", name, size);
		}		
	}

}
