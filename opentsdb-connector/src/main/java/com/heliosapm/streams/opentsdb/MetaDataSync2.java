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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

import com.heliosapm.streams.opentsdb.MetaDataSync.MetricMetaSink;
import com.heliosapm.utils.lang.StringHelper;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.set.ChronicleSetBuilder;
import net.opentsdb.core.Const;
import net.opentsdb.core.RowKey;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.uid.NoSuchUniqueId;
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
	private final AtomicLong sinkCount = new AtomicLong(0L);
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
				.averageKeySize(4)
				.entries(1000000)
				.maxBloatFactor(10.0D)
				.create();
		this.metric_uids = ChronicleMapBuilder.of(String.class, Long.class)
			.entries(10000)
			.averageKeySize(128)
			.averageValueSize(8)
			.maxBloatFactor(10.0D)
			.create();
		this.tagk_uids = ChronicleMapBuilder.of(String.class, Long.class)
			.entries(10000)
			.averageKeySize(128)
			.averageValueSize(8)
			.maxBloatFactor(10.0D)
			.create();
		this.tagv_uids = ChronicleMapBuilder.of(String.class, Long.class)
			.entries(10000)
			.averageKeySize(128)
			.averageValueSize(8)
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
		}
		
		
	}

	public long run() {
		for(Thread t : threads) {
			t.start();
		}
		try {
			final boolean complete = latch.await(1, TimeUnit.HOURS); // config
			
		} catch (Exception ex) {
			
		}
		
		return sinkCount.get();
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
				scanner.setStopKey(Arrays.copyOf(stop_key, stop_key.length));
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
		    
		    // list of deferred calls used to act as a buffer
		    final ArrayList<Deferred<Boolean>> storage_calls = 
		      new ArrayList<Deferred<Boolean>>();
		    final Deferred<Object> result = new Deferred<Object>();
		    
		    /**
		     * Called when we have encountered a previously un-processed UIDMeta object.
		     * This callback will update the "created" timestamp of the UIDMeta and
		     * store the update, replace corrupted metas and update search plugins.
		     */
		    final class UidCB implements Callback<Deferred<Boolean>, UIDMeta> {

		      private final UniqueIdType type;
		      private final byte[] uid;
		      private final long timestamp;
		      
		      /**
		       * Constructor that initializes the local callback
		       * @param type The type of UIDMeta we're dealing with
		       * @param uid The UID of the meta object as a byte array
		       * @param timestamp The timestamp of the timeseries when this meta
		       * was first detected
		       */
		      public UidCB(final UniqueIdType type, final byte[] uid, 
		          final long timestamp) {
		        this.type = type;
		        this.uid = uid;
		        this.timestamp = timestamp;
		      }
		      
		      /**
		       * A nested class called after fetching a UID name to use when creating a
		       * new UIDMeta object if the previous object was corrupted. Also pushes
		       * the meta off to the search plugin.
		       */
		      final class UidNameCB implements Callback<Deferred<Boolean>, String> {

		        @Override
		        public Deferred<Boolean> call(final String name) throws Exception {
		          UIDMeta new_meta = new UIDMeta(type, uid, name);
		          new_meta.setCreated(timestamp);
		          tsdb.indexUIDMeta(new_meta);
		          LOG.info("Replacing corrupt UID [" + UniqueId.uidToString(uid) + 
		            "] of type [" + type + "]");
		          
		          return new_meta.syncToStorage(tsdb, true);
		        }
		        
		      }
		      
		      @Override
		      public Deferred<Boolean> call(final UIDMeta meta) throws Exception {

		        // we only want to update the time if it was outside of an hour
		        // otherwise it's probably an accurate timestamp
		        if (meta.getCreated() > (timestamp + 3600) || 
		            meta.getCreated() == 0) {
		          LOG.info("Updating UID [" + UniqueId.uidToString(uid) + 
		              "] of type [" + type + "]");
		          meta.setCreated(timestamp);
		          
		          // if the UIDMeta object was missing any of these fields, we'll
		          // consider it corrupt and replace it with a new object
		          if (meta.getUID() == null || meta.getUID().isEmpty() || 
		              meta.getType() == null) {
		            return tsdb.getUidName(type, uid)
		              .addCallbackDeferring(new UidNameCB());
		          } else {
		            // the meta was good, just needed a timestamp update so sync to
		            // search and storage
		            tsdb.indexUIDMeta(meta);
		            LOG.info("Syncing valid UID [" + UniqueId.uidToString(uid) + 
		              "] of type [" + type + "]");
		            return meta.syncToStorage(tsdb, false);
		          }
		        } else {
		          LOG.debug("UID [" + UniqueId.uidToString(uid) + 
		              "] of type [" + type + "] is up to date in storage");
		          return Deferred.fromResult(true);
		        }
		      }
		      
		    }
		    
		    /**
		     * Called to handle a previously unprocessed TSMeta object. This callback
		     * will update the "created" timestamp, create a new TSMeta object if
		     * missing, and update search plugins.
		     */
		    final class TSMetaCB implements Callback<Deferred<Boolean>, TSMeta> {
		      
		      private final String tsuid_string;
		      private final byte[] tsuid;
		      private final long timestamp;
		      
		      /**
		       * Default constructor
		       * @param tsuid ID of the timeseries
		       * @param timestamp The timestamp when the first data point was recorded
		       */
		      public TSMetaCB(final byte[] tsuid, final long timestamp) {
		        this.tsuid = tsuid;
		        tsuid_string = UniqueId.uidToString(tsuid);
		        this.timestamp = timestamp;
		      }

		      @Override
		      public Deferred<Boolean> call(final TSMeta meta) throws Exception {
		        
		        /** Called to process the new meta through the search plugin and tree code */
		        final class IndexCB implements Callback<Deferred<Boolean>, TSMeta> {
		          @Override
		          public Deferred<Boolean> call(final TSMeta tsmeta) throws Exception {
		        	  // =========================
		        	  //  TSMeta Callback
		        	// =========================
//		        	  public void sinkMetricMeta(
//		        			  final String metric, 
//		        			  final String metricUid, 
//		        			  final Map<String, String> tags, 
//		        			  final Map<String, String> tagKeyUids, 
//		        			  final Map<String, String> tagValueUids, 
//		        			  final byte[] tsuid);
		        	final int size = tsmeta.getTags().size();
		        	final Map<String, String> tags = new HashMap<String, String>(size);
		        	final Map<String, String> tagKeyUids = new HashMap<String, String>(size);
		        	final Map<String, String> tagValueUids = new HashMap<String, String>(size);
		        	String key = null;
		        	boolean inKey = true;
		        	for(UIDMeta um: tsmeta.getTags()) {
		        		if(inKey) {
		        			key = um.getName();
		        			tagKeyUids.put(um.getName(), um.getUID());
		        			inKey = false;
		        		} else {
		        			tags.put(key, um.getName());
		        			tagValueUids.put(um.getName(), um.getUID());
		        			inKey = true;
		        		}
		        	}
		        	sink.sinkMetricMeta(tsmeta.getMetric().getName(), tsmeta.getMetric().getUID(), tags, tagKeyUids, tagValueUids, StringHelper.hexToBytes(tsmeta.getTSUID()));
		        	final long sunk = sinkCount.incrementAndGet();
		        	if(sunk%100==0) {
		        		LOG.info("Sunk {} metas", sunk);
		        	}
		            return Deferred.fromResult(false);
		          }
		        }
		        
		        /** Called to load the newly created meta object for passage onto the
		         * search plugin and tree builder if configured
		         */
		        final class GetCB implements Callback<Deferred<Boolean>, Boolean> {
		          @Override
		          public final Deferred<Boolean> call(final Boolean exists)
		              throws Exception {
		            if (exists) {
		              return TSMeta.getTSMeta(tsdb, tsuid_string)
		                  .addCallbackDeferring(new IndexCB());
		            } else {
		              return Deferred.fromResult(false);
		            }
		          }
		        }
		        
		        /** Errback on the store new call to catch issues */
		        class ErrBack implements Callback<Object, Exception> {
		          public Object call(final Exception e) throws Exception {
		            LOG.warn("Failed creating meta for: " + tsuid + 
		                " with exception: ", e);
		            return null;
		          }
		        }
		        
		        // if we couldn't find a TSMeta in storage, then we need to generate a
		        // new one
		        if (meta == null) {
		          
		          /**
		           * Called after successfully creating a TSMeta counter and object,
		           * used to convert the deferred long to a boolean so it can be
		           * combined with other calls for waiting.
		           */
		          final class CreatedCB implements Callback<Deferred<Boolean>, Long> {

		            @Override
		            public Deferred<Boolean> call(Long value) throws Exception {
		              LOG.info("Created counter and meta for timeseries [" + 
		                  tsuid_string + "]");
		              return Deferred.fromResult(true);
		            }
		            
		          }
		          
		          /**
		           * Called after checking to see if the counter exists and is used
		           * to determine if we should create a new counter AND meta or just a
		           * new meta
		           */
		          final class CounterCB implements Callback<Deferred<Boolean>, Boolean> {
		            
		            @Override
		            public Deferred<Boolean> call(final Boolean exists) throws Exception {
		              if (!exists) {
		                // note that the increment call will create the meta object
		                // and send it to the search plugin so we don't have to do that
		                // here or in the local callback
		                return TSMeta.incrementAndGetCounter(tsdb, tsuid)
		                  .addCallbackDeferring(new CreatedCB());
		              } else {
		                TSMeta new_meta = new TSMeta(tsuid, timestamp);
		                tsdb.indexTSMeta(new_meta);
		                LOG.info("Counter exists but meta was null, creating meta data "
		                    + "for timeseries [" + tsuid_string + "]");
		                return new_meta.storeNew(tsdb)
		                    .addCallbackDeferring(new GetCB())
		                    .addErrback(new ErrBack());    
		              }
		            }
		          }
		          
		          // Take care of situations where the counter is created but the
		          // meta data is not. May happen if the TSD crashes or is killed
		          // improperly before the meta is flushed to storage.
		          return TSMeta.counterExistsInStorage(tsdb, tsuid)
		            .addCallbackDeferring(new CounterCB());
		        }

		        // verify the tsuid is good, it's possible for this to become 
		        // corrupted
		        if (meta.getTSUID() == null || 
		            meta.getTSUID().isEmpty()) {
		          LOG.warn("Replacing corrupt meta data for timeseries [" + 
		              tsuid_string + "]");
		          TSMeta new_meta = new TSMeta(tsuid, timestamp);
		          tsdb.indexTSMeta(new_meta);
		          return new_meta.storeNew(tsdb)
		              .addCallbackDeferring(new GetCB())
		              .addErrback(new ErrBack());
		        } else {
		          // we only want to update the time if it was outside of an 
		          // hour otherwise it's probably an accurate timestamp
		          if (meta.getCreated() > (timestamp + 3600) || 
		              meta.getCreated() == 0) {
		            meta.setCreated(timestamp);
		            tsdb.indexTSMeta(meta);
		            LOG.info("Updated created timestamp for timeseries [" + 
		                tsuid_string + "]");
		            return meta.syncToStorage(tsdb, false);
		          }
		          
		          LOG.debug("TSUID [" + tsuid_string + "] is up to date in storage");
		          return Deferred.fromResult(false);
		        }
		      }
		      
		    }
		    
		    /**
		     * Scanner callback that recursively loops through all of the data point
		     * rows. Note that we don't process the actual data points, just the row
		     * keys.
		     */
		    final class MetaScanner implements Callback<Object, 
		      ArrayList<ArrayList<KeyValue>>> {
		      
		      private byte[] last_tsuid = null;
		      private String tsuid_string = "";

		      /**
		       * Fetches the next set of rows from the scanner and adds this class as
		       * a callback
		       * @return A meaningless deferred to wait on until all data rows have
		       * been processed.
		       */
		      public Object scan() {
		        return scanner.nextRows().addCallback(this);
		      }

		      @Override
		      public Object call(ArrayList<ArrayList<KeyValue>> rows)
		          throws Exception {
		        if (rows == null) {
		          result.callback(null);
		          return null;
		        }
		        
		        for (final ArrayList<KeyValue> row : rows) {

		          final byte[] tsuid = UniqueId.getTSUIDFromKey(row.get(0).key(), 
		              TSDB.metrics_width(), Const.TIMESTAMP_BYTES);
		          
		          // if the current tsuid is the same as the last, just continue
		          // so we save time
		          if (last_tsuid != null && Arrays.equals(last_tsuid, tsuid)) {
		            continue;
		          }
		          last_tsuid = tsuid;
		          
		          // see if we've already processed this tsuid and if so, continue
		          if (tsuidSet.contains(Arrays.hashCode(tsuid))) {
		            continue;
		          }
		          tsuid_string = UniqueId.uidToString(tsuid);
		          
		          // add tsuid to the processed list
		          tsuidSet.add(Arrays.hashCode(tsuid));
		          
		          // we may have a new TSUID or UIDs, so fetch the timestamp of the 
		          // row for use as the "created" time. Depending on speed we could 
		          // parse datapoints, but for now the hourly row time is enough
		          final long timestamp = Bytes.getUnsignedInt(row.get(0).key(), 
		              Const.SALT_WIDTH() + TSDB.metrics_width());
		          
		          LOG.debug("[" + thread_id + "] Processing TSUID: " + tsuid_string + 
		              "  row timestamp: " + timestamp);
		          
		          // now process the UID metric meta data
		          final byte[] metric_uid_bytes = 
		            Arrays.copyOfRange(tsuid, 0, TSDB.metrics_width());
		          final String metric_uid = UniqueId.uidToString(metric_uid_bytes);
		          Long last_get = metric_uids.get(metric_uid);
		          
		          if (last_get == null || last_get == 0 || timestamp < last_get) {
		            // fetch and update. Returns default object if the meta doesn't
		            // exist, so we can just call sync on this to create a missing
		            // entry
		            final UidCB cb = new UidCB(UniqueIdType.METRIC, 
		                metric_uid_bytes, timestamp);
		            final Deferred<Boolean> process_uid = UIDMeta.getUIDMeta(tsdb, 
		                UniqueIdType.METRIC, metric_uid_bytes).addCallbackDeferring(cb);
		            storage_calls.add(process_uid);
		            metric_uids.put(metric_uid, timestamp);
		          }
		          
		          // loop through the tags and process their meta
		          final List<byte[]> tags = UniqueId.getTagsFromTSUID(tsuid_string);
		          int idx = 0;
		          for (byte[] tag : tags) {
		            final UniqueIdType type = (idx % 2 == 0) ? UniqueIdType.TAGK : 
		              UniqueIdType.TAGV;
		            idx++;
		            final String uid = UniqueId.uidToString(tag);
		            
		            // check the maps to see if we need to bother updating
		            if (type == UniqueIdType.TAGK) {
		              last_get = tagk_uids.get(uid);
		            } else {
		              last_get = tagv_uids.get(uid);
		            }
		            if (last_get != null && last_get != 0 && last_get <= timestamp) {
		              continue;
		            }

		            // fetch and update. Returns default object if the meta doesn't
		            // exist, so we can just call sync on this to create a missing
		            // entry
		            final UidCB cb = new UidCB(type, tag, timestamp);
		            final Deferred<Boolean> process_uid = UIDMeta.getUIDMeta(tsdb, type, tag)
		              .addCallbackDeferring(cb);
		            storage_calls.add(process_uid);
		            if (type == UniqueIdType.TAGK) {
		              tagk_uids.put(uid, timestamp);
		            } else {
		              tagv_uids.put(uid, timestamp);
		            }
		          }
		          
		          /**
		           * An error callback used to cache issues with a particular timeseries
		           * or UIDMeta such as a missing UID name. We want to continue
		           * processing when this happens so we'll just log the error and
		           * the user can issue a command later to clean up orphaned meta
		           * entries.
		           */
		          final class ErrBack implements Callback<Deferred<Boolean>, Exception> {
		            
		            @Override
		            public Deferred<Boolean> call(Exception e) throws Exception {
		              
		              Throwable ex = e;
		              while (ex.getClass().equals(DeferredGroupException.class)) {
		                if (ex.getCause() == null) {
		                  LOG.warn("Unable to get to the root cause of the DGE");
		                  break;
		                }
		                ex = ex.getCause();
		              }
		              if (ex.getClass().equals(IllegalStateException.class)) {
		                LOG.error("Invalid data when processing TSUID [" + 
		                    tsuid_string + "]", ex);
		              } else if (ex.getClass().equals(IllegalArgumentException.class)) {
		                LOG.error("Invalid data when processing TSUID [" + 
		                    tsuid_string + "]", ex);
		              } else if (ex.getClass().equals(NoSuchUniqueId.class)) {
		                LOG.warn("Timeseries [" + tsuid_string + 
		                    "] includes a non-existant UID: " + ex.getMessage());
		              } else {
		                LOG.error("Unmatched Exception: " + ex.getClass());
		                throw e;
		              }
		              
		              return Deferred.fromResult(false);
		            }
		            
		          }
		          
		          // handle the timeseries meta last so we don't record it if one
		          // or more of the UIDs had an issue
		          final Deferred<Boolean> process_tsmeta = 
		            TSMeta.getTSMeta(tsdb, tsuid_string)
		              .addCallbackDeferring(new TSMetaCB(tsuid, timestamp));
		          process_tsmeta.addErrback(new ErrBack());
		          storage_calls.add(process_tsmeta);
		        }
		        
		        /**
		         * A buffering callback used to avoid StackOverflowError exceptions
		         * where the list of deferred calls can exceed the limit. Instead we'll
		         * process the Scanner's limit in rows, wait for all of the storage
		         * calls to complete, then continue on to the next set.
		         */
		        final class ContinueCB implements Callback<Object, ArrayList<Boolean>> {

		          @Override
		          public Object call(ArrayList<Boolean> puts)
		              throws Exception {
		            storage_calls.clear();
		            return scan();
		          }
		          
		        }
		        
		        /**
		         * Catch exceptions in one of the grouped calls and continue scanning.
		         * Without this the user may not see the exception and the thread will
		         * just die silently.
		         */
		        final class ContinueEB implements Callback<Object, Exception> {
		          @Override
		          public Object call(Exception e) throws Exception {
		            
		            Throwable ex = e;
		            while (ex.getClass().equals(DeferredGroupException.class)) {
		              if (ex.getCause() == null) {
		                LOG.warn("Unable to get to the root cause of the DGE");
		                break;
		              }
		              ex = ex.getCause();
		            }
		            LOG.error("[" + thread_id + "] Upstream Exception: ", ex);
		            return scan();
		          }
		        }
		        
		        // call ourself again but wait for the current set of storage calls to
		        // complete so we don't OOM
		        Deferred.group(storage_calls).addCallback(new ContinueCB())
		          .addErrback(new ContinueEB());
		        return null;
		      }
		      
		    }

		    final MetaScanner scanner = new MetaScanner();
		    try {
		      scanner.scan();
		      result.joinUninterruptibly();
		      LOG.info("[" + thread_id + "] Complete");
		    } catch (Exception e) {
		      LOG.error("[" + thread_id + "] Scanner Exception", e);
		      throw new RuntimeException("[" + thread_id + "] Scanner exception", e);
		    } finally {
		    	latch.countDown();
		    }
		  }
		  
		
	}

}
