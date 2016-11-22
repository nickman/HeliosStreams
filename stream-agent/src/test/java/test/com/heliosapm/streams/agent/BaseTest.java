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
package test.com.heliosapm.streams.agent;

import java.io.File;
import java.io.PrintStream;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.security.SecureRandom;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
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

import javax.management.Notification;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.TestName;

import com.heliosapm.utils.file.FileHelper;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.net.SocketUtil;
import com.heliosapm.utils.reflect.PrivateAccessor;

import ch.ethz.ssh2.signature.DSAPrivateKey;
import ch.ethz.ssh2.signature.RSAPrivateKey;

/**
 * <p>Title: BaseTest</p>
 * <p>Description: Base unit test</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>test.com.heliosapm.streams.agent.BaseTest</code></p>
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
	
	public static final Random rnd = new SecureRandom();
	
	/** The UTF8 character set */
	public static final Charset UTF8 = Charset.forName("UTF8");
	
	
	/** A cache of zookeeper servers keyed by the listening port */
	private static final NonBlockingHashMapLong<ZooKeeperServerMain> zooKeeperServers = new NonBlockingHashMapLong<ZooKeeperServerMain>(); 
	
	
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
	 * Starts a new zookeeper instance using temp directories and a randomly assigned port
	 * @return the assigned port
	 */
	public static int startZooKeeper() {
		return startZooKeeper(null);
	}

	
	/**
	 * Starts a new zookeeper instance using temp directories and the specified port
	 * @param zooPort The zookeeper port to listen on. If null, uses a random port
	 * @return the assigned port
	 */
	public static int startZooKeeper(final Integer zooPort) {
		final QuorumPeerConfig qpc = new QuorumPeerConfig();
		final File dataDir = FileHelper.createTempDir("ZooKeeperDataDir", "");		
		final File logDir = FileHelper.createTempDir("ZooKeeperLogDir", "");
		final Properties p = new Properties();
		p.setProperty("dataDir", dataDir.getAbsolutePath());
		p.setProperty("logDir", logDir.getAbsolutePath());
		final int port = zooPort==null ?  SocketUtil.acquirePorts(1).nextPort() : zooPort;
		p.setProperty("clientPort", "" + port);
		

		try {
			qpc.parseProperties(p);
			final ZooKeeperServerMain zooKeeperServer = new ZooKeeperServerMain();
			final ServerConfig configuration = new ServerConfig();
			configuration.readFrom(qpc);
			zooKeeperServers.put(port, zooKeeperServer);
			final Exception[] thr = new Exception[1];
			
			final Thread t = new Thread("TestZooKeeperServer-" + port) {
				public void run() {
			        try {
			            zooKeeperServer.runFromConfig(configuration);			            
			        } catch (Exception ex) {
			            thr[0] = ex;
			        }
				}
			};
			t.setDaemon(true);
			t.start();
			return port;
		} catch (Exception ex) {
			zooKeeperServers.remove(port);
			throw new RuntimeException("Failed to start ZooKeeper instance", ex);
		}
	}
	
	
	/**
	 * Stops the zookeeper instance listening on the passed port
	 * @param port the listening port of the target zookeeper instance
	 */
	public static void stopZooKeeper(final int port) {
		final ZooKeeperServerMain zoo = zooKeeperServers.remove(port);
		if(zoo!=null) {
			PrivateAccessor.invoke(zoo, "shutdown");
		}
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
	
	
	
	/**
	 * Prints the test name about to be executed
	 */
	@Before
	public void printTestName() {
		log("\n\t==================================\n\tRunning Test [" + name.getMethodName() + "]\n\t==================================\n");
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
	 * Deletes the temp plugin directory
	 * @throws java.lang.Exception thrown on any error
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		
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
	}
	
	/**
	 * Out printer
	 * @param fmt the message format
	 * @param args the message values
	 */
	public static void log(String fmt, Object...args) {
		OUT.println(String.format(fmt, args));
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
	
	
	/**
	 * Generate random RSA private key
	 * @return the generated key
	 */
	public static RSAPrivateKey getRSAPrivateKey() {
    final int N = 1500;
    final BigInteger p = BigInteger.probablePrime(N / 2, rnd);
    final BigInteger q = BigInteger.probablePrime(N / 2, rnd);
    final BigInteger phi = (p.subtract(BigInteger.ONE)).multiply(q.subtract(BigInteger.ONE));
    final BigInteger n = p.multiply(q);
    final BigInteger e = new BigInteger("65537");
    final BigInteger d = e.modInverse(phi);
    return new RSAPrivateKey(d, e, n);		
	}
	
	/**
	 * Generate random DSA private key
	 * @return the generated key
	 */
	public static DSAPrivateKey getDSAPrivateKey() {
    final int N = 1500;
    final BigInteger p = BigInteger.probablePrime(N / 2, rnd);
    final BigInteger q = BigInteger.probablePrime(N / 2, rnd);
    final BigInteger phi = (p.subtract(BigInteger.ONE)).multiply(q.subtract(BigInteger.ONE));
    final BigInteger n = p.multiply(q);
    final BigInteger e = new BigInteger("65537");
    final BigInteger d = e.modInverse(phi);
    return new DSAPrivateKey(d, e, n, BigInteger.ONE, BigInteger.TEN);		
	}
	

}
