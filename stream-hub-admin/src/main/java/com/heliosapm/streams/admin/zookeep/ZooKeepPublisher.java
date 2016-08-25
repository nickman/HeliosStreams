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
package com.heliosapm.streams.admin.zookeep;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanNotificationInfo;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.zookeeper.ClientCnxn;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.jmx.SharedNotificationExecutor;
import com.heliosapm.utils.net.LocalHost;

/**
 * <p>Title: ZooKeepPublisher</p>
 * <p>Description: Publishes the Admin Server HTTP URL to Zookeeper</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.admin.zookeep.ZooKeepPublisher</code></p>
 * FIXME: Freaks out when zookeeper nodes disappear. Should go back into poll mode.
[INFO ] 2016-08-25 12:21:13.057 [main-SendThread(10.22.114.37:2181)] ClientCnxn - Unable to read additional data from server sessionid 0x156bdd6b2c0001e, likely server has closed socket, closing socket connection and attempting reconnect
[INFO ] 2016-08-25 12:21:13.158 [main-EventThread] ZooKeepPublisher - ZooKeep Session Disconnected
[INFO ] 2016-08-25 12:21:14.794 [main-SendThread(10.22.114.37:2181)] ClientCnxn - Opening socket connection to server 10.22.114.37/10.22.114.37:2181. Will not attempt to authenticate using SASL (unknown error)
[WARN ] 2016-08-25 12:21:15.796 [main-SendThread(10.22.114.37:2181)] ClientCnxn - Session 0x156bdd6b2c0001e for server null, unexpected error, closing socket connection and attempting reconnect
java.net.ConnectException: Connection refused
	at sun.nio.ch.SocketChannelImpl.checkConnect(Native Method) ~[?:1.8.0_102]
	at sun.nio.ch.SocketChannelImpl.finishConnect(SocketChannelImpl.java:717) ~[?:1.8.0_102]
	at org.apache.zookeeper.ClientCnxnSocketNIO.doTransport(ClientCnxnSocketNIO.java:361) ~[zookeeper-3.4.8.jar:3.4.8--1]
	at org.apache.zookeeper.ClientCnxn$SendThread.run(ClientCnxn.java:1141) [zookeeper-3.4.8.jar:3.4.8--1]
[INFO ] 2016-08-25 12:21:17.200 [main-SendThread(10.22.114.37:2181)] ClientCnxn - Opening socket connection to server 10.22.114.37/10.22.114.37:2181. Will not attempt to authenticate using SASL (unknown error)
[WARN ] 2016-08-25 12:21:18.202 [main-SendThread(10.22.114.37:2181)] ClientCnxn - Session 0x156bdd6b2c0001e for server null, unexpected error, closing socket connection and attempting reconnect
java.net.ConnectException: Connection refused
	at sun.nio.ch.SocketChannelImpl.checkConnect(Native Method) ~[?:1.8.0_102]
	at sun.nio.ch.SocketChannelImpl.finishConnect(SocketChannelImpl.java:717) ~[?:1.8.0_102]
	at org.apache.zookeeper.ClientCnxnSocketNIO.doTransport(ClientCnxnSocketNIO.java:361) ~[zookeeper-3.4.8.jar:3.4.8--1]
	at org.apache.zookeeper.ClientCnxn$SendThread.run(ClientCnxn.java:1141) [zookeeper-3.4.8.jar:3.4.8--1]

 */

public class ZooKeepPublisher extends NotificationBroadcasterSupport implements Watcher, ZooKeepPublisherMBean {

	/** The config key for the zookeeper connect string */
	public static final String CONFIG_CONNECT = "streamhub.config.zookeeperconnect";
	/** The default zookeeper connect string */
	public static final String DEFAULT_CONNECT = "localhost:2181";

	/** The config key for the remotable host name the admin is listening on */
	public static final String CONFIG_HOST_NAME = "server.address";
	/** The config key for the port the admin is listening on */
	public static final String CONFIG_PORT = "server.port";
	
	
	
	/** Indicates if we're running on Windows */
	public static final boolean IS_WIN = System.getProperty("os.name", "").toLowerCase().contains("windows");
	/** The JVM's PID */
	public static final String SPID = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
	/** The JVM's host name according to the RuntimeMXBean */
	public static final String HOST = ManagementFactory.getRuntimeMXBean().getName().split("@")[1];

	/** The config key for the zookeeper session timeout */
	public static final String CONFIG_TIMEOUT = "streamhub.config.zookeeper.timeout";
	/** The default zookeeper session timeout */
	public static final int DEFAULT_TIMEOUT = 15000;
	
	/** The zookeep root node */
	public static final String ROOT_NODE = "/streamhub";
	/** The zookeep root admin node */
	public static final String ROOT_ADMIN_NODE = ROOT_NODE + "/admin";
	/** The zookeep root admin node containing the advertised URL */
	public static final String ROOT_ADMIN_NODE_URL = ROOT_ADMIN_NODE + "/url";
	
	/** The JMX notification infos */
	private static final MBeanNotificationInfo[] NOTIF_INFOS = new MBeanNotificationInfo[] {
			new MBeanNotificationInfo(new String[]{NOTIF_CONNECTED}, Notification.class.getName(), "Emitted when the ZooKeep connection is established"),
			new MBeanNotificationInfo(new String[]{NOTIF_DISCONNECTED}, Notification.class.getName(), "Emitted when the ZooKeep connection is cleanly closed"),
			new MBeanNotificationInfo(new String[]{NOTIF_EXPIRED}, Notification.class.getName(), "Emitted when the ZooKeep connection expires")
	};
	
	
	
	
	/** The UTF8 character set */
	public static final Charset UTF8 = Charset.forName("UTF8");

	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** The configured connect string */	
	protected final String connect;
	/** The session timeout in ms. */
	protected final int timeout;
	/** The zookeeper client instance */
	protected ZooKeeper zk = null;
	/** flag indicating if we're connected to zookeeper */
	protected final AtomicBoolean connected = new AtomicBoolean(false);
	/** The advertised URL to connect to the admin for worker nodes */
	protected final String adminUrl;
	
	/** Serial factory for JMX notifications */
	protected final AtomicLong notifSerial = new AtomicLong();
	
	/** The current session id */
	protected Long sessionId = null;
	
	/**
	 * Creates a new ZooKeepPublisher
	 */
	public ZooKeepPublisher() {
		super(SharedNotificationExecutor.getInstance(), NOTIF_INFOS);
		connect = ConfigurationHelper.getSystemThenEnvProperty(CONFIG_CONNECT, DEFAULT_CONNECT);
		timeout = ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_TIMEOUT, DEFAULT_TIMEOUT);
		adminUrl = "http://" + hostName() + ":" + ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_PORT, 7560) + "/streamhubadmin";
		//+ ConfigurationHelper.getSystemThenEnvProperty("server.context-path", "/" + ROOT_ADMIN_NODE.replace("/", ""));
		log.info("Advertised Admin URL: [{}]", adminUrl);
	}
	
	
	/**
	 * Starts the publisher
	 * @throws IOException thrown if we can't create the zk
	 */
	public void start() throws IOException {
		log.info(">>>>> Starting ZooKeepPublisher...");
		zk = new ZooKeeper(connect, timeout, this);
		lon();
		try {
			final List<ACL> rootAcls = Arrays.asList(
					new ACL(ZooDefs.Perms.ALL, ZooDefs.Ids.ANYONE_ID_UNSAFE)
			);
			Exception firstEx = null;
			try { zk.create(ROOT_NODE, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); } catch (Exception x) {
				firstEx = x;
			}
			try { zk.create(ROOT_ADMIN_NODE, new byte[0], rootAcls, CreateMode.PERSISTENT);} catch (Exception x) {
				if(firstEx==null) firstEx = x;
			}
			Stat rootStat = zk.exists(ROOT_ADMIN_NODE, false);
			if(rootStat==null) {
				throw new RuntimeException("No admin root [" + ROOT_ADMIN_NODE + "] found and failed to create", firstEx);
			}
			log.info("Root Admin Node: [{}]", rootStat);
			try { 
				zk.create(ROOT_ADMIN_NODE_URL, adminUrl.getBytes(UTF8), Ids.READ_ACL_UNSAFE, CreateMode.EPHEMERAL);
				log.info("Registered Admin URL [{}] in [{}]", ROOT_ADMIN_NODE_URL, ROOT_ADMIN_NODE_URL);
			} catch (Exception ex) {
				log.error("Failed to register admin URL", ex);
			}
			JMXHelper.registerMBean(this, OBJECT_NAME);
			log.info("<<<<< ZooKeepPublisher Started.");
		} catch (Exception ex) {
			throw new RuntimeException("Failed to start ZooKeeperPublisher", ex);
		}
		
	}
	
	/**
	 * Stops the publisher
	 */
	public void stop() {
		log.info(">>>>> Stopping ZooKeepPublisher...");
		if(zk!=null) {
			try { zk.delete(ROOT_ADMIN_NODE_URL, -1); } catch (Exception x) {/* No Op */}
			try { zk.close(); } catch (Exception x) {/* No Op */}
			try { JMXHelper.unregisterMBean(OBJECT_NAME); } catch (Exception x) {/* No Op */}
		}
		log.info("<<<<< ZooKeepPublisher Stopped.");
	}
	
	public static void main(String[] args) {
		try {
			JMXHelper.fireUpJMXMPServer(1829);
			final ZooKeepPublisher pub = new ZooKeepPublisher();
			pub.start();
			StdInCommandHandler.getInstance().registerCommand("shutdown", new Runnable(){
				@Override
				public void run() {
					pub.stop();
					System.exit(0);					
				}
			}).run();
//			pub.stop();
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(-1);
		}
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
	 * {@inheritDoc}
	 * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
	 */
	@Override
	public void process(final WatchedEvent event) {			
		switch(event.getState()) {
			case Disconnected:
				// FIXME:  when this happens (and we're not shutting down), start the connection poll
				loff();
				connected.set(false);
				log.warn("ZooKeep Session Disconnected. Waiting for reconnect....");	
				sessionId = null;
				sendNotification(new Notification(NOTIF_DISCONNECTED, OBJECT_NAME, notifSerial.incrementAndGet(), System.currentTimeMillis(), "ZooKeeperPublisher disconnected from ZooKeeper [" + connect + "]"));
				break;
			case Expired:
				connected.set(false);
				log.info("ZooKeep Session Expired");
				sessionId = null;
				sendNotification(new Notification(NOTIF_EXPIRED, OBJECT_NAME, notifSerial.incrementAndGet(), System.currentTimeMillis(), "ZooKeeperPublisher connection expored from ZooKeeper [" + connect + "]"));
				break;
			case SyncConnected:
				lon();
				connected.set(true);
				sessionId = zk.getSessionId();
				log.info("ZooKeep Connected. SessionID: [{}]", sessionId);
				sendNotification(new Notification(NOTIF_EXPIRED, OBJECT_NAME, notifSerial.incrementAndGet(), System.currentTimeMillis(), "ZooKeeperPublisher connected to ZooKeeper [" + connect + "]"));
				break;
			default:
				log.info("ZooKeep Event: [{}]", event);
				break;		
		}		
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.admin.zookeep.ZooKeepPublisherMBean#isConnected()
	 */
	@Override
	public boolean isConnected() {
		return connected.get();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.admin.zookeep.ZooKeepPublisherMBean#getZooConnect()
	 */
	@Override
	public String getZooConnect() {
		return connect;
	}



	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.admin.zookeep.ZooKeepPublisherMBean#getZooTimeout()
	 */
	@Override
	public int getZooTimeout() {
		return timeout;
	}

	/**
	 * Returns the zk instance
	 * @return the zk
	 */
	public ZooKeeper getZk() {
		return zk;
	}
	
	
	/**
	 * Attempts a series of methods of divining the host name
	 * @return the determined host name
	 */
	public static String hostName() {	
		String host = System.getProperty(CONFIG_HOST_NAME, "").trim();
		if(host!=null && !host.isEmpty()) return host;
		host = LocalHost.getHostNameByNic();
		if(host!=null) return host;		
		host = LocalHost.getHostNameByInet();
		if(host!=null) return host;
		host = System.getenv(IS_WIN ? "COMPUTERNAME" : "HOSTNAME");
		if(host!=null && !host.trim().isEmpty()) return host;
		return HOST;
	}	
	
	/**
	 * The real host name (no configs consulted)
	 * @return the determined host name
	 */
	public static String realHostName() {	
		String host = LocalHost.getHostNameByNic();
		if(host!=null) return host;		
		host = LocalHost.getHostNameByInet();
		if(host!=null) return host;
		host = System.getenv(IS_WIN ? "COMPUTERNAME" : "HOSTNAME");
		if(host!=null && !host.trim().isEmpty()) return host;
		return HOST;
	}

	/**
	 * Returns the advertised URL for admin services
	 * @return the advertised URL for admin services
	 */
	public String getAdminUrl() {
		return adminUrl;
	}	
	

}
