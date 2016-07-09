/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.heliosapm.streams.common.zoo;

import java.nio.charset.Charset;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.RetryPolicy;
import org.apache.curator.RetrySleeper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.zookeeper.ClientCnxn;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.jmx.JMXManagedThreadFactory;
import com.heliosapm.utils.jmx.JMXManagedThreadPool;

/**
 * <p>Title: AdminFinder</p>
 * <p>Description: Zookeeper client to find and listen on the StreamHubAdmin server.</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.common.zoo.AdminFinder</code></p>
 */

public class AdminFinder implements Watcher, RetryPolicy, ConnectionStateListener {
	/** The singleton instance */
	private static volatile AdminFinder instance = null;
	/** The singleton instance ctor lock */
	private static final Object lock = new Object();
	/** The zookeep parent node name to retrieve the streamhub admin url */
	public static final String ZOOKEEP_URL_ROOT = "/streamhub/admin";
	/** The zookeep node name to retrieve the streamhub admin url */
	public static final String ZOOKEEP_URL = ZOOKEEP_URL_ROOT + "/url";
	/** The command line arg prefix for the zookeep connect */
	public static final String ZOOKEEP_CONNECT_ARG = "--zookeep=";
	/** The default zookeep connect */
	public static final String DEFAULT_ZOOKEEP_CONNECT = "localhost:2181";
	/** The command line arg prefix for the zookeep session timeout in ms. */
	public static final String ZOOKEEP_TIMEOUT_ARG = "--ztimeout=";
	/** The default zookeep session timeout in ms. */
	public static final int DEFAULT_ZOOKEEP_TIMEOUT = 15000;
	/** The command line arg prefix for the retry pause period in ms. */
	public static final String RETRY_ARG = "--retry=";
	/** The default retry pause period in ms. */
	public static final int DEFAULT_RETRY = 15000;
	/** The command line arg prefix for the connect retry timeout in ms. */
	public static final String CONNECT_TIMEOUT_ARG = "--ctimeout=";
	/** The default zookeep connect timeout in ms. */
	public static final int DEFAULT_CONNECT_TIMEOUT = 5000;
	/** The UTF character set */
	public static final Charset UTF8 = Charset.forName("UTF8");
	
	/**
	 * Initializes and acquires the AdminFinder singleton instance
	 * @param args The app command line arguments
	 * @return the AdminFinder singleton instance
	 */
	public static AdminFinder getInstance(final String[] args) {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					instance = new AdminFinder(args);
				}
			}
		}
		instance.loff();
		instance.start();
		return instance;
	}
	
	/**
	 * Acquires the initialized AdminFinder singleton instance
	 * @return the AdminFinder singleton instance
	 */
	public static AdminFinder getInstance() {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					throw new IllegalStateException("The AdminFinder has not been initialized");
				}
			}
		}
		return instance;
	}
	
	
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(AdminFinder.class);
	/** The zookeep connect string */
	protected String zookeepConnect = null;
	/** The zookeep session timeout in ms. */
	protected int zookeepTimeout = -1;
	/** The curator zookeep client */
	protected CuratorZookeeperClient czk = null;
	/** The zookeep client */
	protected ZooKeeper zk = null;
	
	/** The tree cache instance */
	protected TreeCache treeCache = null;
	
	/** The curator framework */
	protected CuratorFramework cf = null;
	
	/** The admin URL */
	protected final AtomicReference<String> adminURL = new AtomicReference<String>(null);
	/** The admin URL latch to wait on if not set */
	protected final AtomicReference<CountDownLatch> adminURLLatch = new AtomicReference<CountDownLatch>(new CountDownLatch(1));
	
	/** The curator framework connection state */
	protected final AtomicReference<ConnectionState> cfState = new AtomicReference<ConnectionState>(ConnectionState.LOST);
	
	
	/** The zookeep session id */
	protected Long sessionId = null;
	/** The initial connect retry timeout in ms. */
	protected int connectTimeout = -1;
	/** The reconnect payse time in ms. */
	protected int retryPauseTime = -1;
	/** The thread factroy for the curator client */
	protected final ThreadFactory threadFactory = JMXManagedThreadFactory.newThreadFactory("ZooKeeperAdminFinder", true);
	/** The thread pool to run async tasks in */
	protected final JMXManagedThreadPool threadPool = JMXManagedThreadPool.builder()
		.corePoolSize(2)
		.maxPoolSize(12)
		.keepAliveTimeMs(60000)
		.objectName(JMXHelper.objectName("com.heliosapm.streams.common.zoo:service=ThreadPool,name=AdminFinder"))
		.poolName("AdminFinder")
		.prestart(2)
		.publishJMX(true)
		.queueSize(32)
		.threadFactory(threadFactory)
		.build();
	
	/** A set of registered admin URL listeners */
	protected final Set<AdminURLListener> listeners = new CopyOnWriteArraySet<AdminURLListener>();
	

	/**
	 * Creates a new AdminFinder
	 * @param args the command line args
	 */
	public AdminFinder(final String[] args) {
		zookeepConnect = findArg(ZOOKEEP_CONNECT_ARG, DEFAULT_ZOOKEEP_CONNECT, args);
		zookeepTimeout = findArg(ZOOKEEP_TIMEOUT_ARG, DEFAULT_ZOOKEEP_TIMEOUT, args);
		connectTimeout = findArg(CONNECT_TIMEOUT_ARG, DEFAULT_CONNECT_TIMEOUT, args);
		retryPauseTime = findArg(RETRY_ARG, DEFAULT_RETRY, args);
		cf = CuratorFrameworkFactory.builder()
				.canBeReadOnly(false)
				.connectionTimeoutMs(connectTimeout)
				.sessionTimeoutMs(zookeepTimeout)
				.connectString(zookeepConnect)
				.retryPolicy(new ExponentialBackoffRetry(5000, 200))
				//.retryPolicy(this)
				.threadFactory(threadFactory)
				.build();	
		cf.getConnectionStateListenable().addListener(this);		
	}
	
	/*
	 * States:
	 * =======
	 * cf not started
	 * cf started / not connected
	 * cf started / connected
	 * 
	 */
	
	/**
	 * Starts the connection attempt loop
	 * @return this AdminFinder 
	 */
	protected AdminFinder start() {
		if(!isConnected()) {			
			threadFactory.newThread(new Runnable(){
				@Override
				public void run() {
					log.info("Starting Connection Loop");
					if(cf.getState()!=CuratorFrameworkState.STARTED) {
						loff();
						try {							
							cf.start();
							final AtomicInteger attempt = new AtomicInteger(0);
							while(true) {
								final int att = attempt.incrementAndGet();
								final long start = System.currentTimeMillis();
								try {
									log.info("Starting connection attempt #{}", att);
									if(cf.blockUntilConnected(retryPauseTime, TimeUnit.MILLISECONDS)) {
										log.info("Connected to [{}] on attempt #{}", cf.getZookeeperClient().getCurrentConnectionString(), att);
										czk = cf.getZookeeperClient();
										try { zk = czk.getZooKeeper(); } catch (Exception ex) {
											log.warn("Failed to get ZooKeeper instance from connected CuratorZookeeperClient", ex);
										}
										acquireAdminURL();
										break;
									}
									final long elapsedSecs = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()-start);
									log.info("No connection acquired in last [{}] seconds for attempt #{}. Retrying....", elapsedSecs, att);
								} catch (InterruptedException iex) {
									if(Thread.interrupted()) Thread.interrupted();
									log.info("Connect attempt #{} Interrupted while waiting on connect. Starting new attempt.", att);
								}
							}
						} finally {
							lon();
						}
					}
					
				}
			}).start();
		}
		return this;
	}
	
	/**
	 * Acquires the admin url
	 * @param block if true, blocks if the admin url is not available yet
	 * @return the admin url or null if it has not been acquired yet and block was false
	 */
	public String getAdminURL(final boolean block) {
		String s = adminURL.get();
		if(s==null && block) {
			try {
				adminURLLatch.get().await();
				s = adminURL.get();
			} catch (InterruptedException iex) {
				log.error("Interrupted while waiting on AdminURL", iex);
				throw new RuntimeException("Interrupted while waiting on AdminURL", iex);
			}
		}
		return s;
	}
	
	
	/**
	 * Adds an adminURL listener
	 * @param listener the listener to add
	 */
	public void registerAdminURLListener(final AdminURLListener listener) {
		if(listener!=null) {
			listeners.add(listener);
			final String s = adminURL.get();
			if(s!=null) listener.onAdminURL(s);
		}
	}
	
	/**
	 * Removes an adminURL listener
	 * @param listener the listener to remove
	 */
	public void removeAdminURLListener(final AdminURLListener listener) {
		if(listener!=null) {
			listeners.remove(listener);
		}
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see org.apache.curator.framework.state.ConnectionStateListener#stateChanged(org.apache.curator.framework.CuratorFramework, org.apache.curator.framework.state.ConnectionState)
	 */
	@Override
	public void stateChanged(final CuratorFramework client, final ConnectionState newState) {
		final ConnectionState cs = cfState.getAndSet(newState);
		log.info("cfState transition: [{}] --> [{}]", cs, newState);
		
		switch(newState) {			
			case CONNECTED:
				break;
			case LOST:
				break;
			case READ_ONLY:
				break;
			case RECONNECTED:
				break;
			case SUSPENDED:
				break;
			default:
				break;
				
			}
	}
	
	/**
	 * Indicates if the underlying curator framework is started
	 * @return true if the underlying curator framework is started, false otherwise
	 */
	public boolean isStarted() {
		return cf.getState()!=CuratorFrameworkState.STOPPED;
	}
	
	/**
	 * Indicates if the underlying curator framework is connected
	 * @return true if the underlying curator framework is connected, false otherwise
	 */
	public boolean isConnected() {
		return cfState.get().isConnected();
	}
	
	
	
	/**
	 * Attempts to acquire the AdminURL. If not present, starts an AdminURL waiting loop.
	 */
	protected void acquireAdminURL() {
		try {
			log.info("Starting AdminURL Acquisition Loop");
			Stat stat = zk.exists(ZOOKEEP_URL, false);
			if(stat!=null) {
				updateAdminURL(zk.getData(ZOOKEEP_URL, false, stat));
			} else {
				log.error("Connected ZooKeeper server does not contain the StreamHubAdmin URL. Will wait for it on [{}]", czk.getCurrentConnectionString());
				waitForAdminURLBind();
			}			
		} catch (Exception ex) {
			throw new RuntimeException("Failed to acquire AdminURL from [" + zookeepConnect + "]", ex);
		}
	}
	
	/**
	 * Handles tasks required when the AdminURL is acquired
	 * @param data The data acquired from thezookeep bound URL
	 */
	protected void updateAdminURL(final byte[] data) {
		final String aUrl = new String(data, UTF8);
		log.info("AdminURL acquired: [{}]",  aUrl);
		final String prior = adminURL.getAndSet(aUrl);
		adminURLLatch.get().countDown();
		fireAdminURLAcquired(prior, aUrl);
	}
	
	/**
	 * Starts an AdminURL bind event loop
	 */
	protected void waitForAdminURLBind() {
		final TreeCache tc = TreeCache.newBuilder(cf, ZOOKEEP_URL)
				//.setExecutor(threadFactory)
				//.setExecutor((ExecutorService)threadPool)
				//.setCacheData(true)
				.build();
		final AtomicBoolean waiting = new AtomicBoolean(true);
		final Thread waitForAdminURLThread = threadFactory.newThread(new Runnable(){
			@Override
			public void run() {
				final Thread waitThread = Thread.currentThread();
				try {						
					final Listenable<TreeCacheListener> listen = tc.getListenable();			
					listen.addListener(new TreeCacheListener(){
						@Override
						public void childEvent(final CuratorFramework client, final TreeCacheEvent event) throws Exception {
							ChildData cd = event.getData();
							if(cd!=null) {
								log.info("TreeCache Bound [{}]", cd.getPath());
								final String boundPath = cd.getPath();
								if(ZOOKEEP_URL.equals(boundPath)) {
									updateAdminURL(cd.getData());
									tc.close();
									waiting.set(false);
									waitThread.interrupt();
								}
							}
						}
					});
					tc.start();
					log.debug("AdminURL TreeCache Started");			
					// Check for the data one more time in case we missed 
					// the bind event while setting up the listener
					final ZooKeeper z = cf.getZookeeperClient().getZooKeeper();
					final Stat st = z.exists(ZOOKEEP_URL, false);
					if(st!=null) {
						updateAdminURL(z.getData(ZOOKEEP_URL, false, st));
						tc.close();
					}
					while(true) {
						try {
							Thread.currentThread().join(retryPauseTime);
							log.info("Still waiting for AdminURL....");
						} catch (InterruptedException iex) {
							if(Thread.interrupted()) Thread.interrupted();
						}
						if(!waiting.get()) break;
					}
					log.info("Ended wait for AdminURL");					
				} catch (Exception ex) {
					log.error("Failed to wait for AdminURL bind",  ex);
					// FIXME:
				} finally {
					try { tc.close(); } catch (Exception x) {/* No Op */}
				}				
			}
		});
		waitForAdminURLThread.start();
	}
	
	/**
	 * Notifies all registered listeners of an AdminURL event
	 * @param priorUrl The prior URL, null if this is the first
	 * @param newUrl The new URL, null if a removed
	 */
	protected void fireAdminURLAcquired(final String priorUrl, final String newUrl) {
		if(!listeners.isEmpty()) {
			for(final AdminURLListener listener: listeners) {
				threadPool.execute(new Runnable(){
					@Override
					public void run() {
						if(newUrl!=null) {
							if(priorUrl!=null) {
								listener.onAdminURLChanged(priorUrl, newUrl);
							} else {
								listener.onAdminURL(newUrl);
							}
						} else {
							listener.onAdminURLRemoved(priorUrl);
						}
					}
				});							
			}
		}
	}

	/**
	 * Get Admn URL test
	 * @param args none
	 */
	public static void main(final String[] args) {
		final AdminFinder af = AdminFinder.getInstance(args);
		af.registerAdminURLListener(new EmptyAdminURLListener(){
			@Override
			public void onAdminURL(final String adminURL) {
				System.err.println("GOT ADMIN URL: " + adminURL);
			}
		});
		af.threadPool.execute(new Runnable(){
			public void run() {
				System.err.println("[" + Thread.currentThread().getName() + "] Starting wait for AdminURL...");
				final String s = af.getAdminURL(true);
				System.err.println("[" + Thread.currentThread().getName() + "] Got it: [" + s + "]");
			}
		});
		StdInCommandHandler.getInstance().run();
	}
	
	protected volatile Level cxnLevel = Level.INFO;
	
	protected void loff() {
		LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
		Configuration config = ctx.getConfiguration();
		LoggerConfig loggerConfig = config.getLoggerConfig(ClientCnxn.class.getName());
		cxnLevel = loggerConfig.getLevel();
		loggerConfig.setLevel(Level.ERROR);
		ctx.updateLoggers();		
	}
	
	protected void lon() {
		LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
		Configuration config = ctx.getConfiguration();
		LoggerConfig loggerConfig = config.getLoggerConfig(ClientCnxn.class.getName()); 
		loggerConfig.setLevel(cxnLevel);
		ctx.updateLoggers();		
	}
	
	
	/**
	 * Finds a command line arg value
	 * @param prefix The prefix
	 * @param defaultValue The default value if not found
	 * @param args The command line args to search
	 * @return the value
	 */
	private static int findArg(final String prefix, final int defaultValue, final String[] args) {
		final String s = findArg(prefix, (String)null, args);
		if(s==null) return defaultValue;
		try {
			return Integer.parseInt(s);
		} catch (Exception ex) {
			return defaultValue;
		}
	}
	
	/**
	 * Finds a command line arg value
	 * @param prefix The prefix
	 * @param defaultValue The default value if not found
	 * @param args The command line args to search
	 * @return the value
	 */
	@SuppressWarnings("unused")
	private static long findArg(final String prefix, final long defaultValue, final String[] args) {
		final String s = findArg(prefix, (String)null, args);
		if(s==null) return defaultValue;
		try {
			return Long.parseLong(s);
		} catch (Exception ex) {
			return defaultValue;
		}
	}
	
	
	
	/**
	 * Finds a command line arg value
	 * @param prefix The prefix
	 * @param defaultValue The default value if not found
	 * @param args The command line args to search
	 * @return the value
	 */
	private static String findArg(final String prefix, final String defaultValue, final String[] args) {
		for(String s: args) {
			if(s.startsWith(prefix)) {
				s = s.replace(prefix, "").trim();
				return s;
			}
		}
		return defaultValue;
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
	 */
	@Override
	public void process(final WatchedEvent event) {			
		switch(event.getState()) {
			case Disconnected:
				log.info("ZooKeep Session Disconnected");	
				sessionId = null;
				break;
			case Expired:
				log.info("ZooKeep Session Expired");
				sessionId = null;
				break;
			case SyncConnected:
				sessionId = getSessionId();
				log.info("ZooKeep Connected. SessionID: [{}]", sessionId);
				break;
			default:
				log.info("ZooKeep Event: [{}]", event);
				break;		
		}		
	}
	
	
	/**
	 * Acquires the curator client's zookeeper client's session id
	 * @return the curator client's zookeeper client's session id
	 */
	protected Long getSessionId() {
		if(czk==null) return null;
		try {
			return czk.getZooKeeper().getSessionId();
		} catch (Exception ex) {
			log.error("Disaster. Curator client does not have a zookeeper. Developer Error", ex);
			System.exit(-1);
			return null;
		}
	}

	private final AtomicInteger retries = new AtomicInteger(0); 
	
	/**
	 * {@inheritDoc}
	 * @see org.apache.curator.RetryPolicy#allowRetry(int, long, org.apache.curator.RetrySleeper)
	 */
	@Override
	public boolean allowRetry(final int retryCount, final long elapsedTimeMs, final RetrySleeper sleeper) {
		final int r = retries.incrementAndGet();
		if(elapsedTimeMs < retryPauseTime) {
			try {
				sleeper.sleepFor(retryPauseTime-elapsedTimeMs, TimeUnit.MILLISECONDS);
			} catch (Exception ex) {
				log.warn("RetrySleeper Interrupted", ex);
			}
		}
		log.info("Attempting connect retry #{} to [{}]", r, zookeepConnect);
		return true;
	}

	

}
