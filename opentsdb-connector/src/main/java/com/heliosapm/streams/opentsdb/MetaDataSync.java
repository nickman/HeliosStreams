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

import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

import com.heliosapm.streams.tracing.TagKeySorter;
import com.heliosapm.utils.lang.StringHelper;
import com.heliosapm.utils.time.SystemClock;
import com.heliosapm.utils.time.SystemClock.ElapsedTime;
import com.stumbleupon.async.Callback;

import net.opentsdb.core.Const;
import net.opentsdb.core.RowKey;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.uid.UniqueId;

/**
 * <p>Title: MetaDataSync</p>
 * <p>Description: Scans HBase for all TSUIDs and flushes each fully qualified metric definition found to the provided handler.
 * Intended to sync a new OpenTSDB HBase instance or to fill in missing metrics.</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.opentsdb.MetaDataSync</code></p>
 */

public class MetaDataSync {
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
	 * Creates a new MetaDataSync
	 * @param tsdb The TSDB instance we're operating in
	 * @param tsuidSet A set of tsuids that are known to have been replicated. <b><i>This should be a thread safe set.</i></b>.
	 * If null, all discovered tsuids will be sunk.
	 * @param sink The sink to write the discovered metas to
	 * @param threads The number of threads to run
	 */
	public MetaDataSync(final TSDB tsdb, final Set<byte[]> tsuidSet, final MetricMetaSink sink, final int runThreads) {
		this.client = tsdb.getClient();
		this.tsdb = tsdb;
		this.tsuidSet = tsuidSet;
		this.sink = sink;
		this.runThreads = runThreads;
	}
	
	/**
	 * Starts the meta data synchronization 
	 */
	public void go() {
		if(instanceRunning.compareAndSet(false, true)) {
			purgeMeta();
			tsuidSet.clear();
			sinkCount.set(0);
			try {
				final long maxMetricId = getMaxMetricID();
				if(maxMetricId==0) {
					log.warn("MaxMetricId was 0. Possible causes: UID table hasn't been initialized or is missing the UID row or metrics column");
					return;
				}
				log.info("MaxMetricId: {}", maxMetricId);
				final List<Scanner> scanners = getDataTableScanners(maxMetricId, runThreads);
				final CountDownLatch latch = new CountDownLatch(scanners.size()); 
				final List<Thread> threads = new ArrayList<Thread>(scanners.size());
				final ElapsedTime et = SystemClock.startClock();
				for(Scanner scanner: scanners) {
					final Thread t = new Thread(getRunnable(scanner, latch), "MetaDataSyncRunThread#" + runThreadSerial.incrementAndGet());
					t.setDaemon(true);
					threads.add(t);
					t.start();
				}
				latch.await();
				final String avg = et.printAvg("Metas Synced", sinkCount.get());				
				log.info("Meta Sync Complete: {}", avg);
			} catch (Exception ex) {
				log.error("Sync failed", ex);
				throw new RuntimeException("Sync failed", ex);
			} finally {
				instanceRunning.set(false);

			}
		} else {
			log.error("A MetaDataSync is already running");
			throw new IllegalStateException("A MetaDataSync is already running");
		}
	}
	
	
	protected TSMeta fixTSMeta(final byte[] tsuid, final long timestamp) throws Exception {
		final String tsuid_string = UniqueId.uidToString(tsuid);
        log.warn("Replacing corrupt meta data for timeseries [" + 
                tsuid_string + "]");
        TSMeta new_meta = new TSMeta(tsuid, timestamp);
        tsdb.indexTSMeta(new_meta);
        new_meta.storeNew(tsdb).joinUninterruptibly();
        return TSMeta.getTSMeta(tsdb, tsuid_string).joinUninterruptibly();
		
	}
	
	
	protected void purgeMeta() {
		try {			
			log.info("Purging Meta.....");
			final long start = System.currentTimeMillis();
			Class<?> uidManagerClazz =  Class.forName("net.opentsdb.tools.UidManager");
			Method method = uidManagerClazz.getDeclaredMethod("metaPurge", TSDB.class);
			method.setAccessible(true);
			Number x = (Number)method.invoke(null, tsdb);
			log.info("metaPurge Result:" + x);
			long elapsed = System.currentTimeMillis() - start;
			log.info("metaPurge complete in {}: {}", elapsed, x);
			
		} catch (Exception ex) {
			log.error("metaPurge Failed", ex);
			throw new RuntimeException("metaPurge Failed", ex);
		}		
	}

	
	protected Runnable getRunnable(final Scanner scanner, final CountDownLatch latch) {
		return new Runnable() {
			public void run() {
		        try {
					scanner.nextRows().addCallback(new Callback<Void, ArrayList<ArrayList<KeyValue>>>() {
						@Override
						public Void call(final ArrayList<ArrayList<KeyValue>> keyValues) throws Exception {
							log.info("KeyValues: {}", keyValues.size());
							if(keyValues!=null) {
								for (final ArrayList<KeyValue> row : keyValues) {
//									log.info("Rows: {}", row.size());
									final byte[] rowKey = row.get(0).key();
						            final byte[] tsuid = UniqueId.getTSUIDFromKey(rowKey, TSDB.metrics_width(), Const.TIMESTAMP_BYTES);
						            final long timestamp = Bytes.getUnsignedInt(row.get(0).key(), Const.SALT_WIDTH() + TSDB.metrics_width());

						            TSMeta.getTSMeta(tsdb, UniqueId.uidToString(tsuid)).addCallback(new Callback<Void, TSMeta>() {
						            	@Override
						            	public Void call(TSMeta tsMeta) throws Exception {
						            		final String tsUid = tsMeta.getTSUID();
						            		if(tsUid == null || tsUid.isEmpty()) {
						            			tsMeta = fixTSMeta(tsuid, timestamp);
						            		}
						            		final byte[] tsuid = StringHelper.hexToBytes(tsMeta.getTSUID());
//						            		if(tsuidSet.contains(tsuid)) {
//						            			log.info("TSUID already synced: [{}]", tsMeta.getTSUID());
//						            			return null;
//						            		}
						            		final Map<String, String> tags = new TreeMap<String, String>(TagKeySorter.INSTANCE);
						            		final Map<String, String> tagKeyUids = new TreeMap<String, String>();
						            		final Map<String, String> tagValueUids = new TreeMap<String, String>();
						            		UIDMeta tagKey = null;
						            		for(UIDMeta u: tsMeta.getTags()) {
						            			if(u.getType()==UniqueId.UniqueIdType.TAGK) {
						            				tagKey = u;
						            			} else {
						            				tags.put(tagKey.getName(), u.getName());
						            				tagKeyUids.put(tagKey.getName(), tagKey.getUID());
						            				tagValueUids.put(u.getName(), u.getUID());
						            			}
						            		}
						            		//final String metric, final String metricUid, 
						            		// final Map<String, String> tags, 
						            		//final Map<String, String> tagKeyUids, final Map<String, String> tagValueUids, 
						            		// final byte[] tsuid);
						            		sink.sinkMetricMeta(tsMeta.getMetric().getName(), tsMeta.getMetric().getUID(), 
						            				tags, tagKeyUids, tagValueUids, tsuid);
						            		tsuidSet.add(tsuid);
						            		final long sunk = sinkCount.incrementAndGet();
						            		if(sunk%1000==0) log.info("Sunk metric metas: [{}]", sunk);
						            		return null;
						            	}
									});
						            
								}
							}
							return null;
						}
					}).joinUninterruptibly();
				} catch (Exception ex) {
					log.error("Scanner error", ex);
				}
		        latch.countDown();
			}
		};
	}
	
	private static String btos(final byte[] arr) {
		try {
			return StringHelper.bytesToHex(arr);
		} catch (Exception ex) {
			return "[]";
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
				scanner.setStopKey(Arrays.copyOf(stop_key, stop_key.length));
				scanner.setFamily(TSDB.FAMILY());
				scanners.add(scanner);
				log.info("Salted Scanner #{} Range: {} to {}", i, btos(start_key), btos(stop_key));
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
				log.info("Scanner #{} Range: {} to {}", i, btos(start_key), btos(stop_key));
			}

		}

		return scanners;
	}


	/**
	   * Returns the max metric ID from the UID table
	   * @param tsdb The TSDB to use for data access
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
}
