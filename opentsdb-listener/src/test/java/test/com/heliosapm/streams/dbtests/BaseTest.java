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
package test.com.heliosapm.streams.dbtests;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;

import javax.management.Notification;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.TestName;

//import com.heliosapm.streams.metrics.StreamedMetric;
//import com.heliosapm.streams.metrics.StreamedMetricValue;
//import com.heliosapm.streams.opentsdb.mocks.UniqueIdRegistry;
//import com.heliosapm.streams.opentsdb.mocks.datapoints.DataPoint;
//import com.heliosapm.streams.opentsdb.mocks.datapoints.DoubleDataPoint;
//import com.heliosapm.streams.opentsdb.mocks.datapoints.LongDataPoint;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.reflect.PrivateAccessor;
import com.heliosapm.utils.time.SystemClock;
import com.heliosapm.utils.url.URLHelper;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.search.SearchPlugin;
import net.opentsdb.tsd.RTPublisher;
import net.opentsdb.tsd.RpcPlugin;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.Config;

/**
 * <p>Title: BaseTest</p>
 * <p>Description: Base test class</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.opentsdb.BaseTest</code></p>
 */
@Ignore
public class BaseTest {
	/** The currently executing test name */
	@Rule public final TestName name = new TestName();
	/** A random value generator */
	protected static final Random RANDOM = new Random(System.currentTimeMillis());
	
	/** Retain system out */
	protected static final PrintStream OUT = System.out;
	/** Retain system err */
	protected static final PrintStream ERR = System.err;
	
	/** Synthetic UIDMeta counter for metrics */
	protected static final AtomicInteger METRIC_COUNTER = new AtomicInteger();
	/** Synthetic UIDMeta counter for tag keys */
	protected static final AtomicInteger TAGK_COUNTER = new AtomicInteger();
	/** Synthetic UIDMeta counter for tag values */
	protected static final AtomicInteger TAGV_COUNTER = new AtomicInteger();
	
	/** The names for which metrics have been created */
	protected static final Map<String, UIDMeta> createdMetricNames = new HashMap<String, UIDMeta>();
	/** The names for which tag keys have been created */
	protected static final Map<String, UIDMeta> createdTagKeys = new HashMap<String, UIDMeta>();
	/** The names for which tag values have been created */
	protected static final Map<String, UIDMeta> createdTagValues = new HashMap<String, UIDMeta>();
	
	/** Counters for creating fake UIDs keyed by the UID type */
	protected static final Map<UniqueId.UniqueIdType, AtomicInteger> uidTypeCounters = new EnumMap<UniqueId.UniqueIdType, AtomicInteger>(UniqueId.UniqueIdType.class);
	/** Caches for fake UIDs keyed by the UID type */
	protected static final Map<UniqueId.UniqueIdType, Map<String, UIDMeta>> uidTypeCaches = new EnumMap<UniqueId.UniqueIdType, Map<String, UIDMeta>>(UniqueId.UniqueIdType.class);
	
	
	

	
	
	/** If true, we tear down the test TSDB after each test. Otherwise, it is torn down after each class */
	protected static boolean tearDownTSDBAfterTest = true;
	
	
	/** A shared testing scheduler */
	protected static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2, new ThreadFactory(){
		final AtomicInteger serial = new AtomicInteger();
		@Override
		public Thread newThread(Runnable r) {
			Thread t = new Thread(r, "BaseTestScheduler#" + serial.incrementAndGet());
			t.setDaemon(true);
			return t;
		}
	});
	
	/** Reflective access to the TSDB's RTPublisher */
	protected static Field sinkDataPointField;
	/** Reflective access to the TSDBEventDispatcher's reset method */
	protected static Method dispatcherResetMethod;
	/** Reflective access to the TSDBEventDispatcher's instance field */
	protected static Field dispatcherInstanceField;
	
	/** Temp directory for fake plugin jars */
	public static final String TMP_PLUGIN_DIR = "./tmp-plugins";
	
	static {
		System.setProperty("tsd.plugins.disableStatsCollect", "true");
		try {
			sinkDataPointField = TSDB.class.getDeclaredField("rt_publisher");
			sinkDataPointField.setAccessible(true);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		File f = new File(TMP_PLUGIN_DIR);
		if(f.exists() && !f.isDirectory()) {
			throw new RuntimeException("Temp plugin directory cannot be created [" + f.getAbsolutePath() + "]");
		}
		if(!f.exists()) {
			if(!f.mkdir()) {
				throw new RuntimeException("Failed to create Temp plugin directory [" + f.getAbsolutePath() + "]");
			}
		}
		uidTypeCounters.put(UniqueId.UniqueIdType.METRIC, METRIC_COUNTER);
		uidTypeCounters.put(UniqueId.UniqueIdType.TAGK, TAGK_COUNTER);
		uidTypeCounters.put(UniqueId.UniqueIdType.TAGV, TAGV_COUNTER);
		uidTypeCaches.put(UniqueId.UniqueIdType.METRIC, createdMetricNames);
		uidTypeCaches.put(UniqueId.UniqueIdType.TAGK, createdTagKeys);
		uidTypeCaches.put(UniqueId.UniqueIdType.TAGV, createdTagValues);				
	}
	
	
//	/**
//	 * Publishes a data point to the current test TSDB
//	 * @param dp The data point to publish
//	 */
//	public void publishDataPoint(DataPoint dp) {
//		try {
//			RTPublisher pub = (RTPublisher)sinkDataPointField.get(tsdb);
//			if(dp instanceof LongDataPoint) {
//				pub.publishDataPoint(dp.metricName, dp.timestamp, dp.longValue(), dp.tags, dp.getKey().getBytes());
//			} else {
//				pub.publishDataPoint(dp.metricName, dp.timestamp, dp.doubleValue(), dp.tags, dp.getKey().getBytes());
//			}
//		} catch (Exception ex) {
//			throw new RuntimeException(ex);
//		}
//	}
	
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
	
	
	
	/**
	 * Prints the test name about to be executed
	 */
	@Before
	public void printTestName() {
		log("\n\t==================================\n\tRunning Test [" + name.getMethodName() + "]\n\t==================================\n");
	}
	
	/** The current test's TSDB */
	protected static TSDB tsdb = null;
	/** The uniqueid for tag keys */
	protected static UniqueId tagKunik = null;
	/** The uniqueid for tag values */
	protected static UniqueId tagVunik = null;
	/** The uniqueid for metric names */
	protected static UniqueId tagMunik = null;
	
	/**
	 * Stalls the calling thread 
	 * @param time the time to stall in ms.
	 */
	public static void sleep(final long time) {
		try {
			Thread.currentThread().join(time);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
	
	/**
	 * Creates a new annotation with random content
	 * @param customMapEntries The number of custom map entries
	 * @return the annotation
	 */
	public static Annotation newAnnotation(final int customMapEntries) {
		final Annotation a = new Annotation();		
		if(customMapEntries>0) {
			HashMap<String, String> custs = new LinkedHashMap<String, String>(customMapEntries);
			for(int c = 0; c < customMapEntries; c++) {
				String[] frags = getRandomFragments();
				custs.put(frags[0], frags[1]);
			}
			a.setCustom(custs);
		}
		a.setDescription(getRandomFragment());
		long start = nextPosInt()==2 ? System.currentTimeMillis() : SystemClock.unixTime();
		a.setStartTime(start);
//		a.setEndTime(start + nextPosInt(10000));
		return a;
	}
	
	/**
	 * Creates a new annotation with random content and a random number of custom map entries 
	 * @return the annotation
	 */
	public static Annotation newAnnotation() {
		return newAnnotation(nextPosInt(10));
	}
	
	
	/**
	 * Returns the UIDMeta uid as a byte array for the passed int
	 * @param key The int ot generate a byte array for
	 * @return the uid byte array
	 */
	public static byte[] uidFor(int key) {
		StringBuilder b = new StringBuilder(Integer.toHexString(key));
		while(b.length()<6) {
			b.insert(0, "0");
		}
		return UniqueId.stringToUid(b.toString());
	}

	
	/**
	 * Creates a new UIDMeta
	 * @param type The meta type
	 * @param customMapEntries The number of custom map entries
	 * @return a new UIDMeta with randomized content
	 */
	public static UIDMeta newUIDMeta(final UniqueIdType type, final int customMapEntries) {
		final int id = uidTypeCounters.get(type).incrementAndGet();
		final String name = getRandomFragment();
		UIDMeta meta = uidTypeCaches.get(type).get(name);
		if(meta==null) {
			long unixTime = System.currentTimeMillis()/1000;
			meta = new UIDMeta(type, uidFor(id), name);
			meta.setCreated(unixTime);
			meta.setDescription(getRandomFragment());
			meta.setDisplayName(getRandomFragment());
			meta.setNotes(getRandomFragment());		
			meta.setCustom(randomTags(customMapEntries));
			uidTypeCaches.get(type).put(name, meta);
		}
		return meta;
	}
	
	public static void deleteDir(File dir) {
        // delete one level.
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null)
                for (File file : files)
                    if (file.isDirectory()) {
                        deleteDir(file);

                    } else if (!file.delete()) {
                        //LoggerFactory.getLogger(IOTools.class).info("... unable to delete {}", file);
                    }
        }
        dir.delete();
    }

    public static void deleteDir(String dir) {
    	final File f = new File(dir);
    	if(f.exists() && f.isDirectory()) {
    		deleteDir(f);
    	}
    }

	
	/**
	 * Creates a new TSMeta with random content
	 * @param tagCount The number of tags
	 * @param customMapEntries The number of custom map entries in the TSMeta and underlying UIDMetas
	 * @return the built TSMeta
	 */
	public static TSMeta newTSMeta(final int tagCount, final int customMapEntries) {
		
		LinkedList<UIDMeta> uidMetas = new LinkedList<UIDMeta>();
		for(int i = 0; i < tagCount+1; i++) {
			uidMetas.add(newUIDMeta(UniqueIdType.TAGK, customMapEntries));
			uidMetas.add(newUIDMeta(UniqueIdType.TAGV, customMapEntries));
		}
		
		UIDMeta metricMeta = newUIDMeta(UniqueIdType.METRIC, customMapEntries);
		StringBuilder strTsuid = new StringBuilder(metricMeta.getUID());
		for(UIDMeta umeta: uidMetas) {
			strTsuid.append(umeta.getUID());
		}
		byte[] tsuid = UniqueId.stringToUid(strTsuid.toString());
		TSMeta tsMeta = new TSMeta(tsuid, SystemClock.unixTime());
		setTags(tsMeta, new ArrayList<UIDMeta>(uidMetas));
		setMetric(tsMeta, metricMeta);
		return tsMeta;
	}
	
	
	/**
	 * Reflectively sets the metric UID on a TSMeta
	 * @param tsMeta The TSMeta to set on
	 * @param metricMeta The metric UIDMeta
	 */
	public static void setMetric(final TSMeta tsMeta, final UIDMeta metricMeta) {
		PrivateAccessor.setFieldValue(tsMeta, "metric", metricMeta);
	}

	/**
	 * Reflectively sets the tags on a TSMeta
	 * @param tsMeta The TSMeta to set on
	 * @param uids An array list of UIDMetas to create the tags from
	 */
	public static void setTags(TSMeta tsMeta, ArrayList<UIDMeta> uids) {
		PrivateAccessor.setFieldValue(tsMeta, "tags", uids);
		
	}
	
	
	

//	/**
//	 * Creates a new test TSDB
//	 * @param configName The config name to configure with
//	 * @return the created test TSDB
//	 */
//	public static TSDB newTSDB(String configName)  {		
//		try {
//			tsdb = new TSDB(getConfig(configName));
//			tsdb.getConfig().overrideConfig("helios.config.name", configName);
//			Config config = tsdb.getConfig();
//			StringBuilder b = new StringBuilder("\n\t=============================================\n\tTSDB Config\n\t=============================================");
//			for(Map.Entry<String, String> entry: config.getMap().entrySet()) {
//				b.append("\n\t").append(entry.getKey()).append("\t:[").append(entry.getValue()).append("]");
//			}
//			b.append("\n\t=============================================\n");
////			log(b.toString());
//			tsdb.initializePlugins(true);
//			final UniqueIdRegistry reg = UniqueIdRegistry.getInstance(tsdb);
//			tagKunik = reg.getTagKUniqueId();
//			tagVunik = reg.getTagVUniqueId();
//			tagMunik = reg.getMetricsUniqueId();
//			return tsdb;
//		} catch (Exception e) {
//			throw new RuntimeException("Failed to get test TSDB [" + configName + "]", e);
//		}		
//	}
	
	/**
	 * Adds a notification listener to an MBean and waits for a number of notifications
	 * @param objectName The ObjectName of the MBean to listen on notifications from
	 * @param numberOfNotifications The number of notifications to wait for
	 * @param filter An optional filter
	 * @param waitTime The number of milliseconds to wait for
	 * @param run An optional task to run once the listener has been registered
	 * @return A set of the received notifications
	 * @throws TimeoutException thrown when the wait period expires without all the expected notifications being received
	 */
	public static Set<Notification> waitForNotifications(final ObjectName objectName, final int numberOfNotifications, final NotificationFilter filter, final long waitTime, final Callable<?> run) throws TimeoutException {
		final Set<Notification> notifs = new LinkedHashSet<Notification>();
		final AtomicInteger count = new AtomicInteger(0);
		final CountDownLatch latch = new CountDownLatch(numberOfNotifications);
		final NotificationListener listener = new NotificationListener() {
			@Override
			public void handleNotification(final Notification notification, final Object handback) {
				final int x = count.incrementAndGet();
				if(x<=numberOfNotifications) {
					notifs.add(notification);
					latch.countDown();
				}
			}
		};
		try {
			JMXHelper.getHeliosMBeanServer().addNotificationListener(objectName, listener, filter, null);
			if(run!=null) {
				run.call();
			}
			final boolean complete = latch.await(waitTime, TimeUnit.MILLISECONDS);
			if(!complete) {
				throw new TimeoutException("Request timed out. Notifications received: [" + notifs.size() + "]");
			}
			return notifs;
		} catch (Exception ex) {			
			if(ex instanceof TimeoutException) {
				ex.printStackTrace(System.err);
				throw (TimeoutException)ex;
			}
			throw new RuntimeException(ex);
		} finally {
			try { JMXHelper.getHeliosMBeanServer().removeNotificationListener(objectName, listener); } catch (Exception x) {/* No Op */}
		}		
	}
	
	/**
	 * Adds a notification listener to an MBean and waits for a notification
	 * @param objectName The ObjectName of the MBean to listen on notifications from
	 * @param filter An optional filter
	 * @param waitTime The number of milliseconds to wait for
	 * @param run An optional task to run once the listener has been registered
	 * @return the received notification
	 * @throws TimeoutException thrown when the wait period expires without the expected notification being received
	 */
	public static Notification waitForNotification(final ObjectName objectName, final NotificationFilter filter, final long waitTime, final Callable<?> run) throws TimeoutException {
		final Set<Notification> notif = waitForNotifications(objectName, 1, filter, waitTime, run);
		if(notif.isEmpty()) throw new RuntimeException("No notification received");
		return notif.iterator().next();
	}
	
	/**
	 * Determines if the passed two annotations are logically equal
	 * @param one One annotation
	 * @param two Another annotation
	 * @return true if equal, false otherwise
	 */
	public static boolean equal(final Annotation one, final Annotation two) {
		if(one==null || two==null) return false;
		if(one==two) return true;
		if(!equal(one.getDescription(), two.getDescription())) return false;
		if(!equal(one.getNotes(), two.getNotes())) return false;
		if(!equal(one.getTSUID(), two.getTSUID())) return false;
		if(one.getEndTime()!=one.getEndTime()) return false;
		if(one.getStartTime()!=one.getStartTime()) return false;
		if(!equal(one.getCustom(), two.getCustom())) return false;
		return true;
	}
	
	/**
	 * Compares two string maps for equality where both being null means null
	 * @param a One string map
	 * @param b Another string map
	 * @return true if equal, false otherwise
	 */
	public static boolean equal(final Map<String, String> a, final Map<String, String> b) {
		if(a==null && b==null) return true;
		if(a==null || b==null) return false;
		if(a.size() != b.size()) return false;
		if(a.isEmpty()==b.isEmpty()) return true;
		for(Map.Entry<String, String> entry: a.entrySet()) {
			String akey = entry.getKey();
			String avalue = entry.getValue();
			String bvalue = b.get(akey);
			if(bvalue==null) return false;
			if(!equal(avalue, bvalue)) return false;			
		}
		return true;
		
	}
	
	/**
	 * Compares two strings for equality where both being null means null
	 * @param a One string
	 * @param b Another string
	 * @return true if equal, false otherwise
	 */
	public static boolean equal(final String a, final String b) {
		if(a==null && b==null) return true;
		if(a==null || b==null) return false;
		return a.equals(b);
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
	
//	/**
//	 * Creates and returns a new random data point
//	 * @return a data point
//	 */
//	public static DataPoint randomDataPoint() {
//		if(nextBoolean()) {
//			return randomDoubleDataPoint();
//		}
//		return randomLongDataPoint();
//	}
//	
//	
//	/**
//	 * Creates and returns a new random double data point
//	 * @return a double data point
//	 */
//	public static DataPoint randomDoubleDataPoint() {
//		final double d = nextPosLong() + nextPosDouble();
//		return new DoubleDataPoint(d, getRandomFragment(), randomTags(nextPosInt(6)+1), System.currentTimeMillis()); 		
//	}
	
//	/**
//	 * Creates and returns a new random long data point
//	 * @return a long data point
//	 */
//	public static DataPoint randomLongDataPoint() {
//		return new LongDataPoint(nextPosLong(), getRandomFragment(), randomTags(nextPosInt(6)+1), System.currentTimeMillis()); 		
//	}
//	
//	/**
//	 * Determines if the passed data points are logically equal
//	 * @param a A data point
//	 * @param b Another data point
//	 * @return true if they are equal, false otherwise
//	 */
//	public static boolean equal(final DataPoint a, final DataPoint b) {
//		if(a==null || b==null) return false;
//		if(a==b) return true;
//		if(a.isInteger()!=b.isInteger()) return false;
//		if(!a.getValue().equals(b.getValue())) return false;
//		if(!a.metricName.equals(b.metricName)) return false;
//		if(equal(a.tags, b.tags)) return false;
//		if(a.timestamp!=b.timestamp) return false;
//		return true;		
//	}
	
	
	/**
	 * Nothing yet
	 * @throws java.lang.Exception thrown on any error
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	/**
	 * Deletes the temp plugin directory
	 * @throws java.lang.Exception thrown on any error
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		log("\t\t----->> TearDown After Class:%s", !tearDownTSDBAfterTest);
		if(!tearDownTSDBAfterTest) {
			shutdownTest();
			log("AfterClass Shutdown Complete");
		}
		log("Deleted Temp Plugin Dir:" + new File(TMP_PLUGIN_DIR).delete());
		
	}

	/**
	 * Nothing yet...
	 * @throws java.lang.Exception thrown on any error
	 */
	@Before
	public void setUp() throws Exception {
	}
	

	/**
	 * Cleans up various artifacts after each test
	 * @throws java.lang.Exception thrown on any error
	 */
	@After
	public void tearDown() throws Exception {
		log("\t\t----->> TearDown After Test:%s", tearDownTSDBAfterTest);
		if(tearDownTSDBAfterTest) {			
			shutdownTest();
		}
	}
	
	static void shutdownTest() {
		//=========================================================================================
		//  Delete temp files created during test
		//=========================================================================================
		int files = TO_BE_DELETED.size();
		Iterator<File> iter = TO_BE_DELETED.iterator();
		while(iter.hasNext()) {
			iter.next().delete();			
		}
		TO_BE_DELETED.clear();
		log("Deleted [%s] Tmp Files", files);

		//=========================================================================================
		//  Shutdown the test TSDB and clear the test queues.
		//=========================================================================================
		
	}

	/**
	 * Out printer
	 * @param fmt the message format
	 * @param args the message values
	 */
	public static void log(String fmt, Object...args) {
		OUT.println(String.format(fmt, args));
	}
	
//	/**
//	 * Returns the environment classloader for the passed TSDB config
//	 * @param configName The config name
//	 * @return The classloader that would be created for the passed config
//	 */
//	public static ClassLoader tsdbClassLoader(String configName) {
//		try {
//			Config config = getConfig(configName);
//			return TSDBPluginServiceLoader.getSupportClassLoader(config);
//		} catch (Exception ex) {
//			throw new RuntimeException("Failed to load config [" + configName + "]", ex);
//			
//		}
//	}
	
	
	/**
	 * Loads the named TSDB config 
	 * @param name The name of the config file
	 * @return The named config
	 * @throws Exception thrown on any error
	 */
	public static Config getConfig(String name) throws Exception {
		File tmpFile = null;
		try {
			if(name==null || name.trim().isEmpty()) throw new Exception("File was null or empty");
			name = String.format("configs/%s.cfg", name);
			log("Loading config [%s]", name);
			final URL resourceURL = BaseTest.class.getClassLoader().getResource(name);
			if(resourceURL==null) throw new Exception("Cannot read from the resource [" + name + "]");
			final byte[] content = URLHelper.getBytesFromURL(resourceURL);
			tmpFile = File.createTempFile("config-" + name.replace("/", "").replace("\\", ""), ".cfg");
			URLHelper.writeToURL(URLHelper.toURL(tmpFile), content, false);
			if(!tmpFile.canRead()) throw new Exception("Cannot read from the file [" + tmpFile + "]");
			log("Loading [%s]", tmpFile.getAbsolutePath());
			return new Config(tmpFile.getAbsolutePath());
		} finally {			
			if(tmpFile!=null) tmpFile.deleteOnExit();
		}
	}
	
	/**
	 * Err printer
	 * @param fmt the message format
	 * @param args the message values
	 */
	public static void loge(String fmt, Object...args) {
		ERR.print(String.format(fmt, args));
		if(args!=null && args.length>0 && args[0] instanceof Throwable) {
			ERR.println("  Stack trace follows:");
			((Throwable)args[0]).printStackTrace(ERR);
		} else {
			ERR.println("");
		}
	}
	
	/** A set of files to be deleted after each test */
	protected static final Set<File> TO_BE_DELETED = new CopyOnWriteArraySet<File>();
	
	/**
	 * Returns the base plugin type of the passed class
	 * @param clazz The class to test
	 * @return the base plugin type or null if not a plugin or passed class was null
	 */
	public static Class<?> getPluginType(final Class<?> clazz) {
		if(clazz==null) return null;
		if(SearchPlugin.class.isAssignableFrom(clazz)) return SearchPlugin.class;
		if(RTPublisher.class.isAssignableFrom(clazz)) return RTPublisher.class;
		if(RpcPlugin.class.isAssignableFrom(clazz)) return RpcPlugin.class;
		return null;		
	}
	
	/**
	 * Indicates if the passed class is an OpenTSDB plugin type
	 * @param clazz The class to test 
	 * @return true if the class is a plugin type, false otherwise or passed class was null
	 */
	public static boolean isPluginType(final Class<?> clazz) {
		return getPluginType(clazz)!=null;
	}
	
	/**
	 * Creates a temp plugin jar in the plugin directory
	 * @param plugins The plugin classes to install
	 */
	public static void createPluginJar(Class<?>...plugins) {
		FileOutputStream fos = null;
		JarOutputStream jos = null;
		try {
			new File(TMP_PLUGIN_DIR).mkdirs();
			final Map<Class<?>, Set<Class<?>>> groupedPlugins = new HashMap<Class<?>, Set<Class<?>>>();
			groupedPlugins.put(SearchPlugin.class, new HashSet<Class<?>>());
			groupedPlugins.put(RTPublisher.class, new HashSet<Class<?>>());
			groupedPlugins.put(RpcPlugin.class, new HashSet<Class<?>>());
			StringBuilder b = new StringBuilder();
			int pluginCount = 0;
			for(Class<?> c: plugins) {
				if(!isPluginType(c)) throw new IllegalArgumentException("The class [" + c.getName() + "] is not a plugin");
				groupedPlugins.get(getPluginType(c)).add(c);
				pluginCount++;
				b.append(c.getSimpleName());
			}
			if(pluginCount==0) throw new IllegalArgumentException("No plugins defined");
			final String jarName = b.append(".jar").toString();
			File jarFile = new File(TMP_PLUGIN_DIR + "/" + jarName);
			log("Temp JAR File:" + jarFile.getAbsolutePath());
			TO_BE_DELETED.add(jarFile);
			//jarFile.deleteOnExit();		
			StringBuilder manifest = new StringBuilder();
			manifest.append("Manifest-Version: 1.0\n");
			ByteArrayInputStream bais = new ByteArrayInputStream(manifest.toString().getBytes());
			Manifest mf = new Manifest(bais);
			fos = new FileOutputStream(jarFile, false);
			jos = new JarOutputStream(fos, mf);
			for(Map.Entry<Class<?>, Set<Class<?>>> entry: groupedPlugins.entrySet()) {
				final Set<Class<?>> impls = entry.getValue();
				if(impls.isEmpty()) continue;
				final Class<?> pluginType = entry.getKey();
				jos.putNextEntry(new ZipEntry("META-INF/services/" + pluginType.getName()));
				for(Class<?> impl : impls) {
					jos.write((impl.getName() + "\n").getBytes());
				}
				jos.flush();
				jos.closeEntry();				
			}
			jos.flush();
			jos.close();
			fos.flush();
			fos.close();
		} catch (Exception e) {
			throw new RuntimeException("Failed to create Plugin Jar", e);
		} finally {
			if(fos!=null) try { fos.close(); } catch (Exception e) {/* No Op */}
		}		
	}
	
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
	
//	/**
//	 * Starts the periodic generation of data point events using randomly generated values for UIDs.
//	 * @param tsdb The TSDB to push the annotations to
//	 * @param quantity The total number of data points to push
//	 * @param tagCount The number of custom map entries per annotation
//	 * @param period The frequency of publication in ms. Frequencies of less than 1 ms. will push out the entire quantity at once.
//	 * @return a map of the generated data points.
//	 */
//	public Map<String, DataPoint> startDataPointStream(final TSDB tsdb, int quantity, int tagCount, final long period) {
//		final Map<String, DataPoint> dataPoints = new LinkedHashMap<String, DataPoint>(quantity);
//		for(int i = 0; i < quantity; i++) {
//			HashMap<String, String> tags = new LinkedHashMap<String, String>(tagCount);
//			for(int c = 0; c < tagCount; c++) {
//				String[] frags = getRandomFragments();
//				tags.put(frags[0], frags[1]);
//			}
//			DataPoint a = DataPoint.newDataPoint(nextPosDouble(), getRandomFragment(), tags);
//			dataPoints.put(a.getKey(), a);
//		}
//		Runnable r = new Runnable() {
//			@Override
//			public void run() {
//				try {
//					for(DataPoint dp: dataPoints.values()) {	
//						publishDataPoint(dp);
//						//dp.publish(tsdb);
//						if(period>0) {
//							Thread.currentThread().join(period);
//						}
//					}
//				} catch (Exception ex) {
//					ex.printStackTrace(System.err);
//					return;
//				}
//			}
//		};
//		startStream(r, "DataPointStream");
//		return dataPoints;
//	}
	

	/**
	 * Starts the periodic generation of annotation indexing events using randomly generated values for UIDs.
	 * @param tsdb The TSDB to push the annotations to
	 * @param quantity The total number of annotations to push
	 * @param customs The number of custom map entries per annotation
	 * @param period The frequency of publication in ms. Frequencies of less than 1 ms. will push out the entire quantity at once.
	 * @return a map of the generated annotations.
	 */
	public Map<String, Annotation> startAnnotationStream(final TSDB tsdb, int quantity, int customs, final long period) {
		final Map<String, Annotation> annotations = new LinkedHashMap<String, Annotation>(quantity);
		for(int i = 0; i < quantity; i++) {
			Annotation a = new Annotation();
			if(customs>0) {
				HashMap<String, String> custs = new LinkedHashMap<String, String>(customs);
				for(int c = 0; c < customs; c++) {
					String[] frags = getRandomFragments();
					custs.put(frags[0], frags[1]);
				}
				a.setCustom(custs);
			}
			a.setDescription(getRandomFragment());
			long start = nextPosLong();
			a.setStartTime(start);
			a.setEndTime(start + nextPosInt(10000));
			a.setTSUID(getRandomFragment());
			annotations.put(a.getTSUID() + "/" + a.getStartTime(), a);
		}
		Runnable r = new Runnable() {
			@Override
			public void run() {
				try {
					for(Annotation an: annotations.values()) {
						tsdb.indexAnnotation(an);
						if(period>0) {
							Thread.currentThread().join(period);
						}
					}
				} catch (Exception ex) {
					ex.printStackTrace(System.err);
					return;
				}
			}
		};
		startStream(r, "AnnotationStream");
		return annotations;
	}
	
	/** A serial number factory for stream threads */
	public static final AtomicLong streamThreadSerial = new AtomicLong();

	/**
	 * Starts a stream thread
	 * @param r The runnable to run
	 * @param threadName the name of the thread
	 */
	public void startStream(Runnable r, String threadName) {
		Thread t = new Thread(r, threadName + "#" + streamThreadSerial.incrementAndGet());
		t.setDaemon(true);
		t.start();
		log("Started Thread [%s]", threadName);
	}
	
	/**
	 * Asserts the equality of the passed indexable objects
	 * @param c the created indexable object
	 * @param r the retrieved indexable object
	 */
	protected void validate(Object c, Object r) {
		Assert.assertNotNull("The created indexable was null", c);
		Assert.assertNotNull("The retrieved indexable was null", r);
		Assert.assertEquals("The retrieved indexable is not the same type as the created", c.getClass(), r.getClass());
		if(c instanceof Annotation) {
			Annotation a = (Annotation)c, a2 = (Annotation)r;
			Assert.assertEquals("The annotation TSUIDs do not match", a.getTSUID(), a2.getTSUID());
			Assert.assertEquals("The annotation Start Times do not match", a.getStartTime(), a2.getStartTime());
			Assert.assertEquals("The annotation Descriptions do not match", a.getDescription(), a2.getDescription());
			Assert.assertEquals("The annotation Notes do not match", a.getNotes(), a2.getNotes());
			Assert.assertEquals("The annotation Customs do not match", a.getCustom(), a2.getCustom());						
		} else if(c instanceof TSMeta) {
			TSMeta t = (TSMeta)c, t2 = (TSMeta)r;
			Assert.assertEquals("The TSMeta TSUIDs do not match", t.getTSUID(), t2.getTSUID());
			Assert.assertEquals("The TSMeta Create Times do not match", t.getCreated(), t2.getCreated());
			Assert.assertEquals("The TSMeta descriptions do not match", t.getDescription(), t2.getDescription());
			Assert.assertEquals("The TSMeta notes do not match", t.getNotes(), t2.getNotes());
			Assert.assertEquals("The TSMeta customs do not match", t.getCustom(), t2.getCustom());
		} else if(c instanceof UIDMeta) {
			UIDMeta u = (UIDMeta)c, u2 = (UIDMeta)r;
			Assert.assertEquals("The UIDMeta UIDs do not match", u.getUID(), u2.getUID());
			Assert.assertEquals("The UIDMeta Create Times do not match", u.getCreated(), u2.getCreated());
			Assert.assertEquals("The UIDMeta descriptions do not match", u.getDescription(), u2.getDescription());
			Assert.assertEquals("The UIDMeta notes do not match", u.getNotes(), u2.getNotes());
			Assert.assertEquals("The UIDMeta customs do not match", u.getCustom(), u2.getCustom());
		} else {
			throw new RuntimeException("Invalid Indexable Type [" + c.getClass().getName() + "]");
		}
	}
	
	public static <T> T executeAsync(Deferred<T> deferred) throws Exception {
		return executeAsync(deferred, 2000);
	}
	
	public static <T> T executeAsync(Deferred<T> deferred, long timeoutMs) throws Exception {
		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicReference<T> result = new AtomicReference<T>(null); 
		final AtomicReference<Throwable> err = new AtomicReference<Throwable>(null);
		deferred.addCallback(new Callback<Void, T>() {
			@Override
			public Void call(T t) throws Exception {
				if(t instanceof Throwable) {
					err.set((Throwable)t);
				} else {
					result.set(t);
				}
				latch.countDown();
				return null;
			}
		});
		deferred.addErrback(new Callback<Void, T>() {
			@Override
			public Void call(T t) throws Exception {
				if(t instanceof Throwable) {
					err.set((Throwable)t);
				} else {
					result.set(t);
				}
				latch.countDown();
				return null;
			}
		});
		if(!latch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
			throw new Exception("Timeout after [" + timeoutMs + "] ms.");
		}
		if(err.get()!=null) {
			throw new Exception("Errback", err.get());
		}
		return result.get();
	}
	
	
	
//	/**
//	 * Joins the passed tags into a comma separated key value pair flat string
//	 * @param offset The offset in the array to start at
//	 * @param tags The tags to join
//	 * @return the comma separated key value pair flat string
//	 */
//	public static String joinTags(final int offset, final String...tags) {
//		final int len = tags.length - offset;
//		if(len<1) return "";
//		final String[] arr = new String[len];
//		System.arraycopy(tags, offset, arr, 0, len);
//		return joinTags(arr);
//	}
	

//	/**
//	 * Joins the passed tags into a comma separated key value pair flat string
//	 * @param tags The tags to join
//	 * @return the comma separated key value pair flat string
//	 */
//	public static String joinTags(final String...tags) {
//		final Map<String, String> map = StreamedMetric.tagsFromArray(tags);
//		if(map.isEmpty()) return "";
//		final StringBuilder b = new StringBuilder(",");
//		for(Map.Entry<String, String> entry: map.entrySet()) {
//			b.append(entry.getKey()).append("=").append(entry.getValue()).append(",");
//		}		
//		return b.deleteCharAt(b.length()-1).toString();
//	}
	
	
	/** The directed valueless format: [value type],[timestamp],[metric name], [host], [app], [flat tags] */ 
	public static final String DIRECTED_VALUELESS_FMT = "%s,%s,%s,%s,%s%s";
	/** The undirected valueless format: [timestamp],[metric name], [host], [app], [flat tags] */ 
	public static final String UNDIRECTED_VALUELESS_FMT = "%s,%s,%s,%s%s"; 
	/** The directed value format: [value type],[timestamp],[value],[metric name], [host], [app], [flat tags] */ 
	public static final String DIRECTED_VALUE_FMT = "%s,%s,%s,%s,%s,%s%s";
	/** The undirected value format: [timestamp],[value],[metric name], [host], [app], [flat tags] */ 
	public static final String UNDIRECTED_VALUE_FMT = "%s,%s,%s,%s,%s%s";
	
//	public static String splitJoin(final String format, final int offset, final String...values) {
//		final int len = offset + 1;
//		final String[] vars = new String[len +1];
//		System.arraycopy(values, 0, vars, 0, len);
//		vars[len] = joinTags(len, values);
//		return String.format(format, vars);
//	}
//	
//	
//	public static String directedValueless(final String valueType, final long timestamp,  final String metricName, final String host, final String app, final String...tags) {
//		return String.format(DIRECTED_VALUELESS_FMT, valueType, timestamp, metricName, host, app, joinTags(tags));
//	}
//	
//	public static String undirectedValueless(final long timestamp, final String metricName, final String host, final String app, final String...tags) {
//		return String.format(UNDIRECTED_VALUELESS_FMT, timestamp, metricName, host, app, joinTags(tags));
//	}
//	
//	public static String directedValue(final String valueType, final long timestamp, final Number value, final String metricName, final String host, final String app, final String...tags) {
//		return String.format(DIRECTED_VALUE_FMT, valueType, timestamp, value, metricName, host, app, joinTags(tags));
//	}
//	
//	public static String undirectedValue(final long timestamp, final Number value, final String metricName, final String host, final String app, final String...tags) {
//		return String.format(UNDIRECTED_VALUE_FMT, timestamp, value, metricName, host, app, joinTags(tags));
//	}
	
	
//	/**
//	 * Validates that the passed {@link StreamedMetric}s are equal
//	 * @param tr1 One trace
//	 * @param tr2 Another trace
//	 */
//	public static void assertEquals(final StreamedMetric tr1, final StreamedMetric tr2) {
//		Assert.assertNotNull("tr1 was null", tr1);
//		Assert.assertNotNull("tr2 was null", tr2);
//		if(tr1==tr2) return;
//		Assert.assertEquals("types not equal", tr1.getClass(), tr2.getClass());
//		Assert.assertEquals("metricNames not equal", tr1.getMetricName(), tr2.getMetricName());
//		Assert.assertEquals("metricKeys not equal", tr1.metricKey(), tr2.metricKey());
//		Assert.assertEquals("tags not equal", tr1.getTags(), tr2.getTags());
//		Assert.assertEquals("timestamps not equal", tr1.getTimestamp(), tr2.getTimestamp());
//		if(tr1.getValueType()==null) {
//			Assert.assertNull("tr1 vtype was null, but tr2 was not", tr2.getValueType());
//		} else {
//			Assert.assertNotNull("tr1 vtype was not null, but tr2 was", tr2.getValueType());
//			Assert.assertEquals("value types not equal", tr1.getValueType(), tr2.getValueType());
//		}
//		if(tr1.isValued()) {
//			final StreamedMetricValue tr1v = (StreamedMetricValue)tr1;
//			final StreamedMetricValue tr2v = (StreamedMetricValue)tr2;
//			Assert.assertEquals("dataTypes not equal", tr1v.isDoubleValue(), tr2v.isDoubleValue());
//			if(tr1v.isDoubleValue()) {
//				Assert.assertEquals("double values are not equal", tr1v.getDoubleValue(), tr2v.getDoubleValue(), 0D);
//			} else {
//				Assert.assertEquals("long values are not equal", tr1v.getLongValue(), tr2v.getLongValue());
//			}
//		}
//	}
//	
	
	
	
}
