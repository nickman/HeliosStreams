/**
 * Helios, OpenSource Monitoring
 * Brought to you by the Helios Development Group
 *
 * Copyright 2016, Helios Development Group and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org. 
 *
 */
package com.heliosapm.streams.opentsdb;

import java.io.File;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.StreamedMetricValue;


/**
 * <p>Title: TSDBTest</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.BaseTest</code></p>
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
	
	/** Rule to system.err fails */
	@Rule
	public TestWatcher watchman= new TestWatcher() {
		@Override
		protected void failed(final Throwable e, final Description d) {
			loge("\n\t==================================\n\tTest Failed [" + name.getMethodName() + "]\n\t==================================\n");
			loge("\n\t" + e.toString() + "\n\t==================================");
		}

		@Override
		protected void succeeded(final Description description) {

		}
	};
	
	
	@After
	public void printTestEnd() {
		
	}
	
	
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
	

	
	
	public static void deleteDir(File dir) {
        // delete one level.
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null)
                for (File file : files)
                    if (file.isDirectory()) {
                        deleteDir(file);

                    } else if (!file.delete()) {
                        System.err.println("... unable to delete [" + file + "]");
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
	
	
	
	
	
	/**
	 * Nothing yet
	 * @throws java.lang.Exception thrown on any error
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}


	/**
	 * Nothing yet...
	 * @throws java.lang.Exception thrown on any error
	 */
	@Before
	public void setUp() throws Exception {
	}
	
	
	/**
	 * Joins the passed tags into a comma separated key value pair flat string
	 * @param offset The offset in the array to start at
	 * @param tags The tags to join
	 * @return the comma separated key value pair flat string
	 */
	public static String joinTags(final int offset, final String...tags) {
		final int len = tags.length - offset;
		if(len<1) return "";
		final String[] arr = new String[len];
		System.arraycopy(tags, offset, arr, 0, len);
		return joinTags(arr);
	}
	

	/**
	 * Joins the passed tags into a comma separated key value pair flat string
	 * @param tags The tags to join
	 * @return the comma separated key value pair flat string
	 */
	public static String joinTags(final String...tags) {
		final Map<String, String> map = StreamedMetric.tagsFromArray(tags);
		if(map.isEmpty()) return "";
		final StringBuilder b = new StringBuilder(",");
		for(Map.Entry<String, String> entry: map.entrySet()) {
			b.append(entry.getKey()).append("=").append(entry.getValue()).append(",");
		}		
		return b.deleteCharAt(b.length()-1).toString();
	}
	
	// [<value-type>,]<timestamp>, [<value>,] <metric-name>, <host>, <app> [,<tagkey1>=<tagvalue1>,<tagkeyn>=<tagvaluen>]
	// final String vmft = "%s,%s,%s,%s,%s,%s";
	// StreamedMetric sm = StreamedMetric.fromString(String.format(vmft, "A", now, "sys.cpu.total", "webserver1", "ecomm-login", "dc=5"));
	
	/** The directed valueless format: [value type],[timestamp],[metric name], [host], [app], [flat tags] */ 
	public static final String DIRECTED_VALUELESS_FMT = "%s,%s,%s,%s,%s%s";
	/** The undirected valueless format: [timestamp],[metric name], [host], [app], [flat tags] */ 
	public static final String UNDIRECTED_VALUELESS_FMT = "%s,%s,%s,%s%s"; 
	/** The directed value format: [value type],[timestamp],[value],[metric name], [host], [app], [flat tags] */ 
	public static final String DIRECTED_VALUE_FMT = "%s,%s,%s,%s,%s,%s%s";
	/** The undirected value format: [timestamp],[value],[metric name], [host], [app], [flat tags] */ 
	public static final String UNDIRECTED_VALUE_FMT = "%s,%s,%s,%s,%s%s";
	
	public static String splitJoin(final String format, final int offset, final String...values) {
		final int len = offset + 1;
		final String[] vars = new String[len +1];
		System.arraycopy(values, 0, vars, 0, len);
		vars[len] = joinTags(len, values);
		return String.format(format, vars);
	}
	
	
	public static String directedValueless(final String valueType, final long timestamp,  final String metricName, final String host, final String app, final String...tags) {
		return String.format(DIRECTED_VALUELESS_FMT, valueType, timestamp, metricName, host, app, joinTags(tags));
	}
	
	public static String undirectedValueless(final long timestamp, final String metricName, final String host, final String app, final String...tags) {
		return String.format(UNDIRECTED_VALUELESS_FMT, timestamp, metricName, host, app, joinTags(tags));
	}
	
	public static String directedValue(final String valueType, final long timestamp, final Number value, final String metricName, final String host, final String app, final String...tags) {
		return String.format(DIRECTED_VALUE_FMT, valueType, timestamp, value, metricName, host, app, joinTags(tags));
	}
	
	public static String undirectedValue(final long timestamp, final Number value, final String metricName, final String host, final String app, final String...tags) {
		return String.format(UNDIRECTED_VALUE_FMT, timestamp, value, metricName, host, app, joinTags(tags));
	}
	

	/**
	 * Out printer
	 * @param fmt the message format
	 * @param args the message values
	 */
	public static void log(Object fmt, Object...args) {
		OUT.println(String.format(fmt.toString(), args));
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
	
	public static Map<String, String> getRandomTags() {
		final int size = nextPosInt(7)+1;
		final Map<String, String> map = new HashMap<String, String>(size);
		for(int i = 0; i < size; i++) {
			map.put(getRandomFragment(), getRandomFragment());
		}
		return map;
	}
	
	
//	/**
//	 * Validates that the passed {@link IAnnotation}s are equal
//	 * @param ann1 one annotation
//	 * @param ann2 another annotation
//	 */
//	public static void assertEquals(final IAnnotation ann1, final IAnnotation ann2) {
//		Assert.assertNotNull("mn1 was null", ann1);
//		Assert.assertNotNull("mn2 was null", ann2);
//		if(ann1==ann2) return;
//		Assert.assertEquals("long hash code not equal", ann1.getLongHashCode(), ann2.getLongHashCode());
//		Assert.assertEquals("start time not equal", ann1.getStartTime(), ann2.getStartTime());
//		Assert.assertEquals("end time not equal", ann1.getEndTime(), ann2.getEndTime());
//		Assert.assertEquals("int hash code not equal", ann1.hashCode(), ann2.hashCode());
//		Assert.assertEquals("description not equal", ann1.getDescription(), ann2.getDescription());
//		Assert.assertEquals("metric name id not equal", ann1.getMetricNameId(), ann2.getMetricNameId());
//		Assert.assertEquals("# of custom attributes not equal", ann1.getCustom().size(), ann2.getCustom().size());
//		int tag = 0;
//		final Iterator<Map.Entry<String, String>> mn1Tags = ann1.getCustom().entrySet().iterator();
//		final Iterator<Map.Entry<String, String>> mn2Tags = ann2.getCustom().entrySet().iterator();
//		while(mn1Tags.hasNext()) {
//			Map.Entry<String, String> m1Tag = mn1Tags.next();
//			Map.Entry<String, String> m2Tag = mn2Tags.next();
//			Assert.assertEquals("custom# [" + tag + "] key not equal", m1Tag.getKey(), m2Tag.getKey());
//			Assert.assertEquals("custom# [" + tag + "] value not equal", m1Tag.getValue(), m2Tag.getValue());
//		}
//		if(ann1.getNotes()!=null || ann2.getNotes()!=null) {
//			Assert.assertEquals("nites not equal", ann1.getNotes(), ann2.getNotes());
//		}		
//	}

	
	/**
	 * Validates that the passed {@link StreamedMetric}s are equal
	 * @param tr1 One trace
	 * @param tr2 Another trace
	 */
	public static void assertEquals(final StreamedMetric tr1, final StreamedMetric tr2) {
		Assert.assertNotNull("tr1 was null", tr1);
		Assert.assertNotNull("tr2 was null", tr2);
		if(tr1==tr2) return;
		Assert.assertEquals("types not equal", tr1.getClass(), tr2.getClass());
		Assert.assertEquals("metricNames not equal", tr1.getMetricName(), tr2.getMetricName());
		Assert.assertEquals("metricKeys not equal", tr1.metricKey(), tr2.metricKey());
		Assert.assertEquals("tags not equal", tr1.getTags(), tr2.getTags());
		Assert.assertEquals("timestamps not equal", tr1.getTimestamp(), tr2.getTimestamp());
		if(tr1.getValueType()==null) {
			Assert.assertNull("tr1 vtype was null, but tr2 was not", tr2.getValueType());
		} else {
			Assert.assertNotNull("tr1 vtype was not null, but tr2 was", tr2.getValueType());
			Assert.assertEquals("value types not equal", tr1.getValueType(), tr2.getValueType());
		}
		if(tr1.isValued()) {
			final StreamedMetricValue tr1v = (StreamedMetricValue)tr1;
			final StreamedMetricValue tr2v = (StreamedMetricValue)tr2;
			Assert.assertEquals("dataTypes not equal", tr1v.isDoubleValue(), tr2v.isDoubleValue());
			if(tr1v.isDoubleValue()) {
				Assert.assertEquals("double values are not equal", tr1v.getDoubleValue(), tr2v.getDoubleValue(), 0D);
			} else {
				Assert.assertEquals("long values are not equal", tr1v.getLongValue(), tr2v.getLongValue());
			}
		}
	}
	
}

