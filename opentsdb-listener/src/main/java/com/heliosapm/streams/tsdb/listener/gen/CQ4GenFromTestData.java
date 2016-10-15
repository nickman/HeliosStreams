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
package com.heliosapm.streams.tsdb.listener.gen;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.heliosapm.streams.chronicle.TSDBMetricMeta;
import com.heliosapm.streams.tsdb.listener.ListenerMain;
import com.heliosapm.utils.lang.StringHelper;
import com.heliosapm.utils.time.SystemClock;
import com.heliosapm.utils.time.SystemClock.ElapsedTime;
import com.heliosapm.utils.url.URLHelper;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireType;

/**
 * <p>Title: CQ4GenFromTestData</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tsdb.listener.gen.CQ4GenFromTestData</code></p>
 */

public class CQ4GenFromTestData {
	/** The  chronicle queue directory */
	final File queueDir;
	/** The data file directory */
	final File dataDir;
	/** Instance logger */
	protected static final Logger log = LogManager.getLogger(CQ4GenFromTestData.class);
	
	/** The tag keys map */
	protected Map<String, String> tagKeys = null;
	/** The tag values map */
	protected Map<String, String> tagValues = null;
	/** The metric name map */
	protected Map<String, String> metrics = null;
	/** The tsuid map */
	protected Map<String, String> tsuids = null;
	
	/** The out queue to write to */
	protected ChronicleQueue outQueue = null;
	
	/** Thread local metric meta */
	protected static final ThreadLocal<TSDBMetricMeta> threadMeta = new ThreadLocal<TSDBMetricMeta>() {
		protected TSDBMetricMeta initialValue() {
			return TSDBMetricMeta.FACTORY.newInstance();
		};
	};
	
	/** Splitter to split a tsuid into blocks of 12 chars each */
	public static final Pattern UIDSPLITTER = Pattern.compile("(?<=\\G.{12})");

	
	/**
	 * Creates a new CQ4GenFromTestData
	 * @param queueDir The directory where the chronicle queue will go
	 * @param dataDir The directory where the test data files reside
	 */
	public CQ4GenFromTestData(final File queueDir, final File dataDir) {
		if(queueDir.isDirectory()) {
			Arrays.stream(queueDir.listFiles()).forEach(f -> f.delete());
		} else {
			if(!queueDir.mkdirs()) {
				throw new RuntimeException("Failed to create Queue Directory [" + queueDir + "]");				
			}
		}
		this.queueDir = queueDir;
		this.dataDir = dataDir;
		outQueue = SingleChronicleQueueBuilder.binary(queueDir)
				.rollCycle(RollCycles.HOURLY)
				.wireType(WireType.BINARY)
				.build();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		CQ4GenFromTestData gen = new CQ4GenFromTestData(new File(ListenerMain.DEFAULT_INQ_DIR), new File("/home/nwhitehead/hprojects/HeliosStreams/opentsdb-listener/src/test/resources/test-data"));
		gen.go();
	}
	
	public void go() {
		log.info("Starting CQ4GenFromTestData");
		tagKeys = buildMap(new File(dataDir, "tagk.csv"), false);
		tagValues = buildMap(new File(dataDir, "tagv.csv"), false);
		metrics = buildMap(new File(dataDir, "metric.csv"), false);
		tsuids = buildMap(new File(dataDir, "tsuid.csv"), true);
		log.info("Data Maps Loaded");
		final LongAdder recordsQueued = new LongAdder();
		final ElapsedTime et = SystemClock.startClock();
		final Set<String> tsuidKeys = new HashSet<String>(tsuids.keySet());		
		tsuidKeys.parallelStream().forEach(tsuid -> {		
			final String metricUid = tsuids.get(tsuid);
			final String metricName = metrics.get(metricUid);
			final Map<String, String>[] tags = parseTsuid(tsuid);
			try {
				final TSDBMetricMeta mm = threadMeta.get()
						.reset()
						.load(metricName, tags[0], StringHelper.hexToBytes(tsuid))
						.resolved(metricUid, tags[1], tags[2]);
				outQueue.acquireAppender().writeBytes(mm);
			} catch (Exception ex) {
				log.error("Meta Fail. m:[{}], tags:[{}]", metricName, tags, ex);
			}
			recordsQueued.increment();
		});
		final long totalRecords = recordsQueued.longValue();
		final String avg = et.printAvg("Records Queued", totalRecords);
		log.info("{} Records Queued. {}", totalRecords, avg);		
	}
	
	private Map<String, String>[] parseTsuid(final String tsuid) {
		
		final String tsuidMinusMetric = tsuid.substring(6);
		final int size = tsuid.length()/12;
		final Map<String, String> tags = new HashMap<String, String>(size);
		final Map<String, String> tagKeyUids = new HashMap<String, String>(size);
		final Map<String, String> tagValueUids = new HashMap<String, String>(size);
		final Map<String, String>[] maps = new Map[]{tags, tagKeyUids, tagValueUids};
		final String[] pairs = UIDSPLITTER.split(tsuidMinusMetric);
		String k = null, v = null, kv = null, vv = null;
		for(int i = 0; i < size; i++) {
			k = pairs[i].substring(0, 6);
			v = pairs[i].substring(6, 12);
			kv = tagKeys.get(k);
			vv = tagValues.get(v);
			tagKeyUids.put(kv, k);
			tagValueUids.put(vv, v);
			tags.put(kv, vv);
		}
		return maps;
	}
	
	private static Number[] computeStats(final File f) {
		final Number[] stats = new Number[3];
		final String[] lines = URLHelper.getTextFromFile(f).split("\n");
		stats[0] = lines.length;
		stats[1] = Arrays.stream(lines).parallel().map(line -> line.split(",")).mapToInt(a -> a[0].length()).average().getAsDouble();
		stats[2] = Arrays.stream(lines).parallel().map(line -> line.split(",")).mapToInt(a -> a[1].length()).average().getAsDouble();
		return stats;
	}
	
	private static Map<String, String> buildMap(final File f, final boolean reverse) {
		final ElapsedTime et = SystemClock.startClock();
		final Number[] stats = computeStats(f);
		final long entries = stats[0].longValue(); 
		final double avgKeySize = stats[1].doubleValue();
		final double avgValueSize = stats[2].doubleValue();
		final ChronicleMap<String, String> map = ChronicleMapBuilder.of(String.class, String.class)
				.averageKeySize(avgKeySize)
				.averageValueSize(avgValueSize)
				.entries(entries)				
				.maxBloatFactor(5.0)
				.create();		
		final String[] lines = URLHelper.getTextFromFile(f).split("\n");
		Arrays.stream(lines)
			.parallel()
			.map(line -> line.split(","))
			.forEach(kv -> {
				if(reverse) {
					map.put(kv[1], kv[0]);
				} else {
					map.put(kv[0], kv[1]);
				}
			});
		final String avg = et.printAvg("Records", entries);
		log.info("Loaded Map from {}. Entries:{}, Size:{},  {}", f.getName(), entries, map.size(), avg);
		return map;
	}

}
