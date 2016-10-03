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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.heliosapm.utils.time.SystemClock;
import com.heliosapm.utils.time.SystemClock.ElapsedTime;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.StoreFileListener;

/**
 * <p>Title: ListenerMain</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tsdb.listener.ListenerMain</code></p>
 */

public class ListenerMain implements Closeable, StoreFileListener, Runnable {
	/** Instance logger */
	protected final Logger log = LogManager.getLogger();
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
	
	protected final String basePath = System.getProperty("java.io.tmpdir") + "/getting-started";
	protected final ChronicleQueue queue = ChronicleQueueBuilder.single(basePath).build();
	
	/** A random value generator */
	protected static final Random RANDOM = new Random(System.currentTimeMillis());
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
	
	public static byte[] randomBytes(int size) {
		final byte[] bytes = new byte[size];
		RANDOM.nextBytes(bytes);
		return bytes;
	}

	/**
	 * Creates a map of random tags
	 * @param tagCount The number of tags
	 * @return the tag map
	 */
	public static Map<String, String> randomTags(final int tagCount) {
		final Map<String, String> tags = new LinkedHashMap<String, String>(tagCount);
		for(int i = 0; i < tagCount; i++) {
			String[] frags = getRandomFragments();
			tags.put(frags[0], frags[1]);
		}
		return tags;
	}
	

	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		final ListenerMain listener = new ListenerMain();
		listener.run();
	}
	
	
	public void run() {
		final int CNT = 20000;
		final Set<DataPoint> set = new LinkedHashSet<DataPoint>(CNT);
		for(int i = 0; i < CNT; i++) {
			final DataPoint dp = DataPoint.dp(getRandomFragment(), System.currentTimeMillis(), nextPosInt(100) + nextPosDouble(), randomTags(4), randomBytes(12));
			set.add(dp.clone());
		}
		final ExcerptAppender ea = queue.acquireAppender();
		for(DataPoint dp: set) {
			
			try {
//				ea = queue.acquireAppender();
				ea.writeBytes(dp);
			} catch (Exception ex) {
				log.error("Failed to write", ex);
			}
		}
		
		ElapsedTime et = SystemClock.startClock();
		for(DataPoint dp: set) {
			try {
//				ea = queue.acquireAppender();
				ea.writeBytes(dp);
			} catch (Exception ex) {
				log.error("Failed to write", ex);
			}
		}
		log.info(et.printAvg("Writes", CNT));
		final Set<DataPoint> dataPoints = new LinkedHashSet<DataPoint>(CNT);
		final ExcerptTailer tailer = queue.createTailer();
		int i = 0;
		final DataPoint dp = DataPoint.getAndReset();
		et = SystemClock.startClock();		
		while(tailer.readBytes(dp.reset())) {
			i++;
			dataPoints.add(dp.clone());
			if(i==CNT) break;
		}
		log.info(et.printAvg("Reads", CNT));
//		for(DataPoint dp: dataPoints) {
//			//log.info(dp.toString());
//		}
	}


	/**
	 * {@inheritDoc}
	 * @see net.openhft.chronicle.queue.impl.StoreFileListener#onReleased(int, java.io.File)
	 */
	@Override
	public void onReleased(int cycle, File file) {
		// TODO Auto-generated method stub
		
	}


	/**
	 * {@inheritDoc}
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
	

}
