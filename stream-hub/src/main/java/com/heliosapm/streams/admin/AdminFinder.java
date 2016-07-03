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
package com.heliosapm.streams.admin;

import java.nio.charset.Charset;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.RetryPolicy;
import org.apache.curator.RetrySleeper;
import org.apache.curator.SessionFailRetryLoop;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.heliosapm.utils.jmx.JMXManagedThreadFactory;

/**
 * <p>Title: AdminFinder</p>
 * <p>Description: Zookeeper client to find and listen on the StreamHubAdmin server.</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.admin.AdminFinder</code></p>
 */

public class AdminFinder implements Watcher, RetryPolicy {
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
	
	/** Indicates if we're connected */
	protected final AtomicBoolean connected = new AtomicBoolean(false);
	/** The zookeep session id */
	protected Long sessionId = null;
	/** The initial connect retry timeout in ms. */
	protected int connectTimeout = -1;
	/** The reconnect payse time in ms. */
	protected int retryPauseTime = -1;
	
	protected final ThreadFactory threadFactory = JMXManagedThreadFactory.newThreadFactory("ZooKeeperAdminFiner", true); 
	

	/**
	 * Creates a new AdminFinder
	 * @param args the command line args
	 */
	public AdminFinder(final String[] args) {
		zookeepConnect = findArg(ZOOKEEP_CONNECT_ARG, DEFAULT_ZOOKEEP_CONNECT, args);
		zookeepTimeout = findArg(ZOOKEEP_TIMEOUT_ARG, DEFAULT_ZOOKEEP_TIMEOUT, args);
		connectTimeout = findArg(CONNECT_TIMEOUT_ARG, DEFAULT_CONNECT_TIMEOUT, args);
		retryPauseTime = findArg(RETRY_ARG, DEFAULT_RETRY, args);
		
	}
	
	public void start() {
		try {
			CuratorFramework cf = CuratorFrameworkFactory.newClient(zookeepConnect, zookeepTimeout, connectTimeout, this);
					
//					CuratorFrameworkFactory.builder()
//					.canBeReadOnly(true)
//					.connectionTimeoutMs(connectTimeout)
//					.sessionTimeoutMs(zookeepTimeout)
//					.connectString(zookeepConnect)
//					
//					//.retryPolicy(new ExponentialBackoffRetry(5000, 200))
//					.retryPolicy(this)
//					.threadFactory(threadFactory)
//					.build();
			cf.start();
			cf.blockUntilConnected();
			czk = cf.getZookeeperClient();
			
//			SessionFailRetryLoop retryLoop = czk.newSessionFailRetryLoop(SessionFailRetryLoop.Mode.RETRY);
//			final AtomicBoolean inited = new AtomicBoolean(false);
//			retryLoop.start();
//			try {
//				while ( retryLoop.shouldContinue()) {
//					try {
//						if(!inited.get()) {
//							czk.start();
//							inited.set(true);
//							break;
//						}						
//			         } catch ( Exception e ) {
////			             retryLoop.takeException(e);
//			             if(!retryLoop.shouldContinue()) {
//			            	 log.error("Retry Limit. We're outa here");
//			            	 System.exit(-1);
//			             }
//			         }
//			     }
//			 } finally {
//			     retryLoop.close();
//			 }			
			zk = czk.getZooKeeper();
			Stat stat = zk.exists(ZOOKEEP_URL_ROOT, false);
			if(stat==null) {
				log.error("Connected ZooKeeper server does not contain the StreamHubAdmin node. Are you connected to the right server ?");
				System.exit(-1);
			} else {
				byte[] data = zk.getData(ZOOKEEP_URL, false, stat);
				String urlStr = new String(data, UTF8);
				System.out.println("StreamHubAdmin is at: [" + urlStr + "]");
			}
			
		} catch (Exception ex) {
			throw new RuntimeException("Failed to acquire zookeeper connection at [" + zookeepConnect + "]", ex);
		}
	}
	
	public static void main(final String[] args) {
		AdminFinder af = new AdminFinder(args);
		try {
			af.start();
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
		}
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
				connected.set(false);
				log.info("ZooKeep Session Disconnected");	
				sessionId = null;
				break;
			case Expired:
				connected.set(false);
				log.info("ZooKeep Session Expired");
				sessionId = null;
				break;
			case SyncConnected:
				connected.set(true);
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
