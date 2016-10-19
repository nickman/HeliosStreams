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
package com.heliosapm.streams.opentsdb;

import java.nio.charset.Charset;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hbase.async.HBaseClient;

import com.heliosapm.streams.opentsdb.MetaDataSync.MetricMetaSink;

import net.openhft.chronicle.set.ChronicleSetBuilder;
import net.opentsdb.core.TSDB;

/**
 * <p>Title: MetaDataSync2</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.opentsdb.MetaDataSync2</code></p>
 */

public class MetaDataSync2 {
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** An hbase client connected to the target HBase */
	protected final HBaseClient client;
	/** The TSDB instance we're operating in */
	protected final TSDB tsdb;
	/** A set of tsuids that are known to have been replicated */
	protected final Set<byte[]> tsuidSet;
	/** The sink to write the discovered metas to */
	protected final MetricMetaSink sink;
	/** The number of threads to allocate to the sync */
	protected final int runThreads;
	/** Sink counter */
	private final AtomicLong sinkCount = new AtomicLong(0L);
	
	/** A charset for reconstituting bytes back into strings */
	private static final Charset CHARSET = Charset.forName("ISO-8859-1");
	
	/** Serial number factory for run threads */
	private static final AtomicLong runThreadSerial = new AtomicLong();
	/** global flag to indicate a metadatasync is running */
	private static final AtomicBoolean instanceRunning = new AtomicBoolean(false);
	
	/**
	 * Indicates if a sync is running
	 * @return true if a sync is running, false otherwise
	 */
	public static boolean isInstanceRunning() {
		return instanceRunning.get();
	}
	
	/**
	 * Creates a new MetaDataSync2
	 * @param tsdb The TSDB instance we're operating in
	 * @param sink The sink to write the discovered metas to
	 * @param threads The number of threads to run
	 */
	public MetaDataSync2(final TSDB tsdb, final MetricMetaSink sink, final int runThreads, final double avgKeySize) {
		this.client = tsdb.getClient();
		this.tsdb = tsdb;
		this.tsuidSet = ChronicleSetBuilder.of(byte[].class)
				.averageKeySize(avgKeySize)
				.entries(1000000)
				.maxBloatFactor(10.0D)
				.create();
		this.sink = sink;
		this.runThreads = runThreads;
	}

}
