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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

import com.heliosapm.utils.lang.StringHelper;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.set.ChronicleSetBuilder;
import net.opentsdb.core.Const;
import net.opentsdb.core.RowKey;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;

/**
 * <p>Title: MetaDataSync2</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.opentsdb.MetaDataSync2</code></p>
 */

public class MetaDataSync2 {
	/** Instance logger */
	protected final Logger LOG = LogManager.getLogger(getClass());
	/** An hbase client connected to the target HBase */
	protected final HBaseClient client;
	/** The TSDB instance we're operating in */
	protected final TSDB tsdb;
	/** A set of tsuids that are known to have been replicated */
	protected final Set<Integer> tsuidSet;
	/** The sink to write the discovered metas to */
	protected final MetricMetaSink sink;
	/** The number of threads to allocate to the sync */
	protected final int runThreads;
	/** Sink counter */
	private final LongAdder sinkCount = new LongAdder();
	/** List of metric UIDs and their earliest detected timestamp */
	final Map<String, Long> metric_uids;
	/** List of tagk UIDs and their earliest detected timestamp */
	final Map<String, Long> tagk_uids;
	/** List of tagv UIDs and their earliest detected timestamp */
	final Map<String, Long> tagv_uids;

	/** The completion latch */
	final CountDownLatch latch;
	/** The threads running the sync */
	final List<Thread> threads;

	/** A charset for reconstituting bytes back into strings */
	private static final Charset CHARSET = Charset.forName("ISO-8859-1");

	/** Serial number factory for run threads */
	private static final AtomicLong runThreadSerial = new AtomicLong();
	/** global flag to indicate a metadatasync is running */
	private static final AtomicBoolean instanceRunning = new AtomicBoolean(false);
	
	/**
	 * <p>Title: MetricMetaSink</p>
	 * <p>Description: Defines a sink that will accept metric meta data mined by a MetaDataSync instance.</p> 
	 * <p>Company: Helios Development Group LLC</p>
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>com.heliosapm.streams.opentsdb.MetaDataSync.MetricMetaSink</code></p>
	 */
	public static interface MetricMetaSink {
		/**
		 * Sinks a metric meta definition
		 * @param metric The metric name
		 * @param metricUid The metric uid
		 * @param tags The tags
		 * @param tagKeyUids The tag key uids keyed by the tag key
		 * @param tagValueUids The tag value uids keyed by the tag value
		 * @param tsuid The metric time series UID
		 */
		public void sinkMetricMeta(final String metric, final String metricUid, final Map<String, String> tags, final Map<String, String> tagKeyUids, final Map<String, String> tagValueUids, final byte[] tsuid);

	}
	

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
		this.tsuidSet = ChronicleSetBuilder.of(Integer.class)
				.entries(1000000)
				.maxBloatFactor(10.0D)
				.create();
		this.metric_uids = ChronicleMapBuilder.of(String.class, Long.class)
				.entries(10000)
				.averageKeySize(128)
				.maxBloatFactor(10.0D)
				.create();
		this.tagk_uids = ChronicleMapBuilder.of(String.class, Long.class)
				.entries(10000)
				.averageKeySize(128)
				.maxBloatFactor(10.0D)
				.create();
		this.tagv_uids = ChronicleMapBuilder.of(String.class, Long.class)
				.entries(10000)
				.averageKeySize(128)
				.maxBloatFactor(10.0D)
				.create();

		this.sink = sink;
		this.runThreads = runThreads;
		final long maxMetricId = getMaxMetricID();
		final List<Scanner> scanners = getDataTableScanners(maxMetricId, runThreads);
		final int actualRunThreads = scanners.size();
		threads = new ArrayList<Thread>(actualRunThreads);
		latch = new CountDownLatch(actualRunThreads);
		int threadId = 1;
		for(Scanner scanner : scanners) {
			final Thread t = new MSync(scanner, threadId, latch);
			t.setDaemon(true);
			threads.add(t);			
			threadId++;
		}


	}

	/**
	 * Runs the sync
	 * @return the total number of synced metrics
	 */
	public long run() {
		int tc = 0;
		for(Thread t : threads) {
			t.start();
			tc++;
		}
		LOG.info("Started {} Threads", tc);
		try {
			final boolean complete = latch.await(1, TimeUnit.HOURS); // config
			if(!complete) {
				LOG.warn("MetaDataSync timed out");
			}
		} catch (Exception ex) {
			LOG.error("MetaDataSync failed", ex);
			throw new RuntimeException("MetaDataSync failed", ex);
		}		
		return sinkCount.longValue();
	}


	/**
	 * Returns the max metric ID from the UID table
	 * @return The max metric ID as an integer value, may be 0 if the UID table
	 * hasn't been initialized or is missing the UID row or metrics column.
	 * @throws IllegalStateException if the UID column can't be found or couldn't
	 * be parsed
	 */
	protected long getMaxMetricID() {
		// first up, we need the max metric ID so we can split up the data table
		// amongst threads.
		final GetRequest get = new GetRequest(tsdb.uidTable(), new byte[] { 0 });
		get.family("id".getBytes(CHARSET));
		get.qualifier("metrics".getBytes(CHARSET));
		ArrayList<KeyValue> row;
		try {
			row = tsdb.getClient().get(get).joinUninterruptibly();
			if (row == null || row.isEmpty()) {
				return 0;
			}
			final byte[] id_bytes = row.get(0).value();
			if (id_bytes.length != 8) {
				throw new IllegalStateException("Invalid metric max UID, wrong # of bytes");
			}
			return Bytes.getLong(id_bytes);
		} catch (Exception e) {
			throw new RuntimeException("Shouldn't be here", e);
		}
	}


	protected List<Scanner> getDataTableScanners(final long max_id, final int num_scanners) {
		if (num_scanners < 1) {
			throw new IllegalArgumentException(
					"Number of scanners must be 1 or more: " + num_scanners);
		}
		// TODO - It would be neater to get a list of regions then create scanners
		// on those boundaries. We'll have to modify AsyncHBase for that to avoid
		// creating lots of custom HBase logic in here.
		final short metric_width = TSDB.metrics_width();
		final List<Scanner> scanners = new ArrayList<Scanner>();    

		if (Const.SALT_WIDTH() > 0) {
			// salting is enabled so we'll create one scanner per salt for now
			byte[] start_key = HBaseClient.EMPTY_ARRAY;
			byte[] stop_key = HBaseClient.EMPTY_ARRAY;

			for (int i = 1; i < Const.SALT_BUCKETS() + 1; i++) {
				// move stop key to start key
				if (i > 1) {
					start_key = Arrays.copyOf(stop_key, stop_key.length);
				}

				if (i >= Const.SALT_BUCKETS()) {
					stop_key = HBaseClient.EMPTY_ARRAY;
				} else {
					stop_key = RowKey.getSaltBytes(i);
				}
				final Scanner scanner = client.newScanner(tsdb.dataTable());
				scanner.setStartKey(Arrays.copyOf(start_key, start_key.length));
		        if (stop_key != null) {
		        	scanner.setStopKey(Arrays.copyOf(stop_key, stop_key.length));
		        }				
				scanner.setFamily(TSDB.FAMILY());
				scanners.add(scanner);
				LOG.info("Salted Scanner #{} Range: {} to {}", i, btos(start_key), btos(stop_key));
			}

		} else {
			// No salt, just go by the max metric ID

			final long quotient = max_id % num_scanners == 0 ? max_id / num_scanners : 
				(max_id / num_scanners) + 1;

			byte[] start_key = HBaseClient.EMPTY_ARRAY;
			byte[] stop_key = new byte[metric_width];

			for (int i = 0; i < num_scanners; i++) {
				// move stop key to start key
				if (i > 0) {
					start_key = Arrays.copyOf(stop_key, stop_key.length);
				}

				// setup the next stop key
				final byte[] stop_id;
				if ((i +1) * quotient > max_id) {
					stop_id = null;
				} else {
					stop_id = Bytes.fromLong((i + 1) * quotient);
				}
				if ((i +1) * quotient >= max_id) {
					stop_key = HBaseClient.EMPTY_ARRAY;
				} else {
					System.arraycopy(stop_id, stop_id.length - metric_width, stop_key, 
							0, metric_width);
				}

				final Scanner scanner = tsdb.getClient().newScanner(tsdb.dataTable());
				scanner.setStartKey(Arrays.copyOf(start_key, start_key.length));
				if (stop_key != null) {
					scanner.setStopKey(Arrays.copyOf(stop_key, stop_key.length));
				}
				
				scanner.setFamily(TSDB.FAMILY());
				scanners.add(scanner);
				LOG.info("Scanner #{} Range: {} to {}", i, btos(start_key), btos(stop_key));
			}

		}

		return scanners;
	}

	private static String btos(final byte[] arr) {
		try {
			return StringHelper.bytesToHex(arr);
		} catch (Exception ex) {
			return "[]";
		}
	}



	class MSync extends Thread {
		/** Diagnostic ID for this thread */
		final int thread_id;
		/** The scanner for this worker */
		final Scanner scanner;
		/** The completion latch */
		final CountDownLatch latch;

		final Deferred<Object> result = new Deferred<Object>();

		/**
		 * Constructor that sets unique variables
		 * @param scanner The scanner to use for this worker
		 * @param thread_id The ID of this thread (starts at 0)
		 * @param latch The completion latch
		 */
		public MSync(final Scanner scanner, final int thread_id, final CountDownLatch latch) {
			this.scanner = scanner;
			this.thread_id = thread_id;
			this.latch = latch;
			this.setName("MSyncThread#" + thread_id);
		}

		/**
		 * Loops through the entire TSDB data set and exits when complete.
		 */
		public void run() {


			final class MetaScanner implements Callback<Object, ArrayList<ArrayList<KeyValue>>> {
				protected final AtomicLong counter = new AtomicLong(0L);

				/**
				 * Fetches the next set of rows from the scanner and adds this class as
				 * a callback
				 * @return A meaningless deferred to wait on until all data rows have
				 * been processed.
				 */
				public Deferred<Object> scan() {
					return scanner.nextRows().addCallback(this).addErrback(new Callback<Void, Throwable>() {
						@Override
						public Void call(Throwable t) throws Exception {
							LOG.error("Scanner failure on Thread [{}]", t, thread_id);
							result.callback(null);
							return null;
						}
					});
				}

				protected Map<String, String> invert(final Map<String, String> map) {
					final Map<String, String> inverted = new HashMap<String, String>(map.size());
					for(Map.Entry<String, String> entry: map.entrySet()) {
						inverted.put(entry.getValue(), entry.getKey());
					}
					return inverted;
				}

				@Override
				public Object call(ArrayList<ArrayList<KeyValue>> rows) throws Exception {
					LOG.info("Scanner Starting");
					if (rows == null || rows.isEmpty()) {
						result.callback(null);
						return null;
					}		        
					for (final ArrayList<KeyValue> row : rows) {
						LOG.info("Thread [{}] Scanner returned {} rows", thread_id, rows.size());
						final byte[] tsuid = UniqueId.getTSUIDFromKey(row.get(0).key(), TSDB.metrics_width(), Const.TIMESTAMP_BYTES);
						final List<byte[]> tagBytes = new ArrayList<byte[]>(8);
						final Deferred<EnumMap<UniqueIdType, Map<String, String>>> resolvedMetric = resolveUIDsAsync(tsuid, tagBytes);

						resolvedMetric.addCallback(new Callback<Void, EnumMap<UniqueIdType,Map<String,String>>>() {
							@Override
							public Void call(final EnumMap<UniqueIdType, Map<String, String>> resolved) throws Exception {
								final long sub = counter.incrementAndGet();
								LOG.info("Thread [{}] processing UIDAsync [{}], total: {}", thread_id, sub, sinkCount.longValue());
								
								final int tagCount = tagBytes.size()/2;
								final Map<String, String> uidTagPairs = new HashMap<String, String>(tagCount);
								boolean inKey = true;
								String keyUid = null;
								for(byte[] um: tagBytes) {
									if(inKey) {
										keyUid = UniqueId.uidToString(um);
									} else {
										uidTagPairs.put(keyUid, UniqueId.uidToString(um));
									}
									inKey = !inKey;
								}

								final Map.Entry<String, String> mmap = resolved.get(UniqueIdType.METRIC).entrySet().iterator().next();
								// ====== UID --> Name Mapping
								final Map<String, String> tagKeyUids = resolved.get(UniqueIdType.TAGK);
								final Map<String, String> tagValueUids = resolved.get(UniqueIdType.TAGV);
								// ======
								final Map<String, String> tags = new HashMap<String, String>(tagCount);
								for(Map.Entry<String, String> entry: uidTagPairs.entrySet()) {
									tags.put(tagKeyUids.get(entry.getKey()), tagValueUids.get(entry.getValue()));
								}
								sink.sinkMetricMeta(mmap.getKey(), mmap.getValue(), tags, invert(tagKeyUids), invert(tagValueUids), tsuid);
								sinkCount.increment();
								return null;
							}
						});		          
					}			
					scan();
					return null;
				}
			}

			final MetaScanner scanner = new MetaScanner();
			try {
				scanner.scan();
				result.joinUninterruptibly();
				LOG.info("MetaSync Thread#" + thread_id + " Complete");
			} catch (Exception e) {
				LOG.error("[" + thread_id + "] Scanner Exception", e);
				throw new RuntimeException("[" + thread_id + "] Scanner exception", e);
			} finally {
				latch.countDown();
			}
		}
	}


	protected Deferred<EnumMap<UniqueIdType, Map<String, String>>> resolveUIDsAsync(final byte[] tsuid, final List<byte[]> tags) {
		final String tsuid_string = UniqueId.uidToString(tsuid);
		tags.addAll(UniqueId.getTagsFromTSUID(tsuid_string));
		final int tsize = tags.size();
		final ArrayList<Deferred<Void>> completion = new ArrayList<Deferred<Void>>((tsize * 2)+1);
		final Deferred<EnumMap<UniqueIdType, Map<String, String>>> resultDef = new Deferred<EnumMap<UniqueIdType, Map<String, String>>>();
		final EnumMap<UniqueIdType, Map<String, String>> resolvedMap = new EnumMap<UniqueIdType, Map<String, String>>(UniqueIdType.class);		
		resolvedMap.put(UniqueIdType.TAGK, new HashMap<String, String>(tsize));
		resolvedMap.put(UniqueIdType.TAGV, new HashMap<String, String>(tsize));


		final byte[] metricUidBytes = Arrays.copyOfRange(tsuid, 0, TSDB.metrics_width());
		final Deferred<Void> metricDef = UIDMeta.getUIDMeta(tsdb, UniqueIdType.METRIC, metricUidBytes).addCallback(new Callback<Void, UIDMeta>() {
			@Override
			public Void call(final UIDMeta uidMetric) throws Exception {
				resolvedMap.put(UniqueIdType.METRIC, Collections.singletonMap(uidMetric.getName(), uidMetric.getUID()));
				return null;
			}
		});
		completion.add(metricDef);
		boolean inKey = true;
		for(final byte[] uidBytes : tags) {
			final UniqueIdType type = inKey ? UniqueIdType.TAGK : UniqueIdType.TAGV;
			final Deferred<Void> uidDef = UIDMeta.getUIDMeta(tsdb, type, uidBytes).addCallback(new Callback<Void, UIDMeta>() {
				@Override
				public Void call(final UIDMeta uidMeta) throws Exception {
					resolvedMap.get(type).put(uidMeta.getUID(), uidMeta.getName());
					return null;
				}
			});
			completion.add(uidDef);
			inKey = !inKey;
		}

		Deferred.group(completion).addCallback(new Callback<Void, ArrayList<Void>>() {
			@Override
			public Void call(ArrayList<Void> complete) throws Exception {
				resultDef.callback(resolvedMap);
				return null;
			}
		});

		//		try {
		//			Deferred.group(completion).join();
		//		} catch (Exception ex) {
		//			throw new RuntimeException("Interrupted while resolving tsuid", ex);
		//		}

		return resultDef;
	}


}
