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
package com.heliosapm.streams.buffers;

import io.netty.buffer.PoolArenaMetric;
import io.netty.buffer.PoolChunkListMetric;
import io.netty.buffer.PoolChunkMetric;
import io.netty.buffer.PooledByteBufAllocator;

import java.lang.management.ManagementFactory;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Title: BufferArenaMonitor</p>
 * <p>Description: A netty pooled byte buffer allocation monitor</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.buffers.BufferArenaMonitor</code></p>
 * FIXME:  !! the reset to zero will hand out bad stats. Need to put all stats
 * in a long array stored in an atomic ref.
 */

public class BufferArenaMonitor implements BufferArenaMonitorMBean, Runnable {
	/** A scheduler to schedule monitor stats collections */
	protected static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, new ThreadFactory(){
		final AtomicInteger serial = new AtomicInteger();
		@Override
		public Thread newThread(final Runnable r) {
			final Thread t = new Thread(r, "BufferArenaMonitorThread#" + serial.incrementAndGet());
			t.setDaemon(true);
			return t;
		}
	});
	
	/** The number of allocations */
	protected long allocations = -1;
	/** The number of tiny allocations */
	protected long tinyAllocations = -1;
	/** The number of small allocations */
	protected long smallAllocations = -1;
	/** The number of normal allocations */
	protected long normalAllocations = -1;
	/** The number of huge allocations */
	protected long hugeAllocations = -1;
	/** The number of deallocations */
	protected long deallocations = -1;
	/** The number of tiny deallocations */
	protected long tinyDeallocations = -1;
	/** The number of small deallocations */
	protected long smallDeallocations = -1;
	/** The number of normal deallocations */
	protected long normalDeallocations = -1;
	/** The number of huge deallocations */
	protected long hugeDeallocations = -1;
	/** The number of active allocations */
	protected long activeAllocations = -1;
	/** The number of active tiny allocations */
	protected long activeTinyAllocations = -1;
	/** The number of active small allocations */
	protected long activeSmallAllocations = -1;
	/** The number of active normal allocations */
	protected long activeNormalAllocations = -1;
	/** The number of active huge allocations */
	protected long activeHugeAllocations = -1;

	/** The number of chunk lists for the pooled allocator */
	protected int chunkLists = -1;
	/** The number of small allocation sub-pages for the pooled allocator */
	protected int smallSubPages = -1;
	/** The number of tiney allocation sub-pages for the pooled allocator */
	protected int tinySubPages = -1;

	/** The total chunk size allocation for this allocator */
	protected long totalChunkSize = -1;
	/** The free chunk size allocation for this allocator */
	protected long chunkFreeBytes = -1;
	/** The used chunk size allocation for this allocator */
	protected long chunkUsedBytes = -1;
	/** The percent chunk utilization for this allocator */
	protected int chunkUsage = -1;
	
	/** The elapsed time of the last stats aggregation */
	protected long lastElapsed = 0;
	
	/** The arena metric being monitored */
	protected final PooledByteBufAllocator pooledAllocator;
	/** Indicates if the arena is direct or heap */
	protected final boolean direct;
	/** The name of the type of arena */
	protected final String type;
	/** The JMX ObjectName for this monitor */
	protected final ObjectName objectName;
	/** Instance logger */
	protected final Logger log;
	

	/**
	 * Creates a new BufferArenaMonitor
	 * @param pooledAllocator The allocator being monitored
	 * @param direct true if direct, false if heap
	 */
	public BufferArenaMonitor(final PooledByteBufAllocator pooledAllocator, final boolean direct) {		
		this.pooledAllocator = pooledAllocator;
		this.direct = direct;
		this.type = direct ? "Direct" : "Heap";
		log = LoggerFactory.getLogger(getClass().getName() + "." + type);
		objectName = objectName("com.heliosapm.streams.buffers:service=BufferManagerStats,type=" + type);
		run();
		scheduler.scheduleAtFixedRate(this, 5, 5, TimeUnit.SECONDS);
		try {
			ManagementFactory.getPlatformMBeanServer().registerMBean(this, objectName);
		} catch (Exception ex) {
			log.warn("Failed to register JMX MBean for [{}} BufferArena. Continuing without.", type, ex);
		}
	}
	
	private static ObjectName objectName(final String name) {
		try {
			return new ObjectName(name);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
	
//  /**
//   * Collects the stats and metrics tracked by this instance.
//   * @param collector The collector to use.
//   */
//  public void collectStats(final StatsCollector collector) {
//  	
//  	collector.addExtraTag("type", type.toLowerCase());
//  	try {
//  		collector.record("bytebufs.allocs", hugeAllocations, "size=huge");
//	  	collector.record("bytebufs.allocs", normalAllocations, "size=normal");
//	  	collector.record("bytebufs.allocs", smallAllocations, "size=small");
//	  	collector.record("bytebufs.allocs", tinyAllocations, "size=tiny");
//	  	
//	  	collector.record("bytebufs.deallocs", hugeDeallocations, "size=huge");
//	  	collector.record("bytebufs.deallocs", normalAllocations, "size=normal");
//	  	collector.record("bytebufs.deallocs", smallDeallocations, "size=small");
//	  	collector.record("bytebufs.deallocs", tinyDeallocations, "size=tiny");
//	  	
//	  	collector.record("bytebufs.activeallocs", activeHugeAllocations, "size=huge");
//	  	collector.record("bytebufs.activeallocs", activeNormalAllocations, "size=normal");
//	  	collector.record("bytebufs.activeallocs", activeSmallAllocations, "size=small");
//	  	collector.record("bytebufs.activeallocs", activeTinyAllocations, "size=tiny");
//	  	
//
//	  	collector.record("bytebufs.subpages", smallSubPages, "size=small");
//	  	collector.record("bytebufs.subpages", tinySubPages, "size=tiny");
//	  		  	
//	  	collector.record("bytebufs.totalchunksize", totalChunkSize);
//	  	collector.record("bytebufs.freebytes", chunkFreeBytes);
//	  	collector.record("bytebufs.usedbytes", chunkUsedBytes);
//	  	collector.record("bytebufs.usage", chunkUsage);
//	  	
//	  	
//
//  	} finally {
//  		collector.clearExtraTag("type");
//  	}
//  	
////    collector.record("rpc.received", telnet_rpcs_received, "type=telnet");
////    collector.record("rpc.received", http_rpcs_received, "type=http");
////    collector.record("rpc.received", http_plugin_rpcs_received, "type=http_plugin");
////    collector.record("rpc.exceptions", exceptions_caught);
//  }
	
	
	/**
	 * <p>Refreshes the arena stats</p>
	 * {@inheritDoc}
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		final long start = System.currentTimeMillis();
		final List<PoolArenaMetric> arenaMetrics = direct ? pooledAllocator.directArenas() : pooledAllocator.heapArenas();
		
		totalChunkSize = 0;
		chunkFreeBytes = 0;
		chunkUsedBytes = 0;
		chunkUsage = 0;
		
		
		chunkLists = 0;
		smallSubPages = 0;
		tinySubPages = 0;		
		allocations = 0;
		tinyAllocations = 0;
		smallAllocations = 0;
		normalAllocations = 0;
		hugeAllocations = 0;
		deallocations = 0;
		tinyDeallocations = 0;
		smallDeallocations = 0;
		normalDeallocations = 0;
		hugeDeallocations = 0;
		activeAllocations = 0;
		activeTinyAllocations = 0;
		activeSmallAllocations = 0;
		activeNormalAllocations = 0;
		activeHugeAllocations = 0;		
		for(PoolArenaMetric poolArenaMetric: arenaMetrics) {
			for(PoolChunkListMetric pclm: poolArenaMetric.chunkLists()) {
				for(Iterator<PoolChunkMetric> iter = pclm.iterator(); iter.hasNext();) {
					PoolChunkMetric pcm = iter.next();
					totalChunkSize += pcm.chunkSize();
					chunkFreeBytes += pcm.freeBytes();
				}
			}		
			chunkLists += poolArenaMetric.chunkLists().size();
			smallSubPages += poolArenaMetric.smallSubpages().size();
			tinySubPages += poolArenaMetric.tinySubpages().size();					
			allocations += poolArenaMetric.numAllocations();
			tinyAllocations += poolArenaMetric.numTinyAllocations();
			smallAllocations += poolArenaMetric.numSmallAllocations();
			normalAllocations += poolArenaMetric.numNormalAllocations();
			hugeAllocations += poolArenaMetric.numHugeAllocations();
			deallocations += poolArenaMetric.numDeallocations();
			tinyDeallocations += poolArenaMetric.numTinyDeallocations();
			smallDeallocations += poolArenaMetric.numSmallDeallocations();
			normalDeallocations += poolArenaMetric.numNormalDeallocations();
			hugeDeallocations += poolArenaMetric.numHugeDeallocations();
			activeAllocations += poolArenaMetric.numActiveAllocations();
			activeTinyAllocations += poolArenaMetric.numActiveTinyAllocations();
			activeSmallAllocations += poolArenaMetric.numActiveSmallAllocations();
			activeNormalAllocations += poolArenaMetric.numActiveNormalAllocations();
			activeHugeAllocations += poolArenaMetric.numActiveHugeAllocations();
		}
		chunkUsedBytes = totalChunkSize - chunkFreeBytes;		
		chunkUsage = perc(totalChunkSize, chunkUsedBytes);
		
		lastElapsed = System.currentTimeMillis() - start;
		log.debug("Collected [{}] allocator stats from [{}] arena metrics in {} ms.", type, arenaMetrics.size(), lastElapsed);
	}
	
	private static int perc(final double total, final double part) {
		if(total==0 || part==0) return 0;
		final double d = part/total * 100D;
		return (int)d;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getAllocations()
	 */
	@Override
	public long getAllocations() {
		return allocations;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getTinyAllocations()
	 */
	@Override
	public long getTinyAllocations() {
		return tinyAllocations;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getSmallAllocations()
	 */
	@Override
	public long getSmallAllocations() {
		return smallAllocations;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getNormalAllocations()
	 */
	@Override
	public long getNormalAllocations() {
		return normalAllocations;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getHugeAllocations()
	 */
	@Override
	public long getHugeAllocations() {
		return hugeAllocations;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getDeallocations()
	 */
	@Override
	public long getDeallocations() {
		return deallocations;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getTinyDeallocations()
	 */
	@Override
	public long getTinyDeallocations() {
		return tinyDeallocations;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getSmallDeallocations()
	 */
	@Override
	public long getSmallDeallocations() {
		return smallDeallocations;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getNormalDeallocations()
	 */
	@Override
	public long getNormalDeallocations() {
		return normalDeallocations;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getHugeDeallocations()
	 */
	@Override
	public long getHugeDeallocations() {
		return hugeDeallocations;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getActiveAllocations()
	 */
	@Override
	public long getActiveAllocations() {
		return activeAllocations;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getActiveTinyAllocations()
	 */
	@Override
	public long getActiveTinyAllocations() {
		return activeTinyAllocations;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getActiveSmallAllocations()
	 */
	@Override
	public long getActiveSmallAllocations() {
		return activeSmallAllocations;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getActiveNormalAllocations()
	 */
	@Override
	public long getActiveNormalAllocations() {
		return activeNormalAllocations;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getActiveHugeAllocations()
	 */
	@Override
	public long getActiveHugeAllocations() {
		return activeHugeAllocations;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getType()
	 */
	@Override
	public String getType() {
		return type;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#isDirect()
	 */
	@Override
	public boolean isDirect() {
		return direct;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getChunkLists()
	 */
	@Override
	public int getChunkLists() {
		return chunkLists;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getSmallSubPages()
	 */
	@Override
	public int getSmallSubPages() {
		return smallSubPages;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getTinySubPages()
	 */
	@Override
	public int getTinySubPages() {
		return tinySubPages;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getTotalChunkSize()
	 */
	@Override
	public long getTotalChunkSize() {
		return totalChunkSize;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getChunkFreeBytes()
	 */
	@Override
	public long getChunkFreeBytes() {
		return chunkFreeBytes;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getChunkUsedBytes()
	 */
	@Override
	public long getChunkUsedBytes() {		
		return chunkUsedBytes;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getChunkUsage()
	 */
	@Override
	public int getChunkUsage() {
		return chunkUsage;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getLastElapsed()
	 */
	@Override
	public long getLastElapsed() {
		return lastElapsed;
	}

}
