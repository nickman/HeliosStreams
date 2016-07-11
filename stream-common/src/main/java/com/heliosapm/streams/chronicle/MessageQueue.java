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

import java.io.File;
import java.util.Properties;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.heliosapm.utils.collections.Props;

import net.openhft.chronicle.bytes.BytesRingBufferStats;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

/**
 * <p>Title: MessageQueue</p>
 * <p>Description: A disk persistent message queue to separate kafka consumers from the actual processors.</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.chronicle.MessageQueue</code></p>
 */

public class MessageQueue implements Consumer<BytesRingBufferStats>, StoreFileListener {
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
	
//  int TEST_BLOCK_SIZE = 256 * 1024; // smallest safe block size for Windows 8+  
//	private Consumer<BytesRingBufferStats> onRingBufferStats = NoBytesRingBufferStats.NONE;
//    private TimeProvider timeProvider = SystemTimeProvider.INSTANCE;
//    private Supplier<Pauser> pauserSupplier = () -> new TimeoutPauser(500_000);
//    private long timeoutMS = 10_000; // 10 seconds.
//    private WireStoreFactory storeFactory;
//    private int sourceId = 0;
//    private StoreRecoveryFactory recoverySupplier = TimedStoreRecovery.FACTORY;
//    private StoreFileListener storeFileListener = (cycle, file) -> {
//        Jvm.debug().on(getClass(), "File released " + file);
//    };
//
//    public AbstractChronicleQueueBuilder(File path) {
//        this.rollCycle = RollCycles.DAILY;
//        this.blockSize = 64L << 20;
//        this.path = path;
//        this.wireType = WireType.BINARY;
//        this.epoch = 0;
//        this.bufferCapacity = 2 << 20;
//        this.indexSpacing = -1;
//        this.indexCount = -1;
//    }
	
	
	
	public MessageQueue(final String name, final Properties config) {
		if(name==null || name.trim().isEmpty()) throw new IllegalArgumentException("The passed name was null or empty");
		queueName = name.trim();
		queueConfig = Props.extract(name, config, true, false);
		baseQueueDirectory = null;
		queue = SingleChronicleQueueBuilder.binary(baseQueueDirectory)
			.blockSize(1)
			.bufferCapacity(1)
			.buffered(true)
			.indexSpacing(1)
			.onRingBufferStats(this)
			.rollCycle(RollCycles.HOURLY)
			.sourceId(1)
			.storeFileListener(this)
			.timeoutMS(1)
			.build();
	}


	@Override
	public void accept(BytesRingBufferStats t) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void onReleased(int cycle, File file) {
		// TODO Auto-generated method stub
		
	}

}
