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
package com.heliosapm.streams.discovery;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import javax.management.MBeanNotificationInfo;
import javax.management.Notification;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.io.CloseableService;

/**
 * <p>Title: CustomEndpointListener</p>
 * <p>Description: Re-implementation of the {@link EndpointListener} to allow for multiple configurable zookeeper nodes and data unmarshalling models.</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.discovery.CustomEndpointListener</code></p>
 */

public class CustomEndpointListenerService implements CustomEndpointListenerServiceMBean, Closeable, ConnectionStateListener, TreeCacheListener {
	/** The singleton instance */
	private static volatile CustomEndpointListenerService instance = null;
	/** The singleton instance ctor lock */
	private static final Object lock = new Object();
	
	/** Definitions of JMX notifications emitted by this service */
	private static final MBeanNotificationInfo[] NOTIF_INFOS = new MBeanNotificationInfo[] {
		new MBeanNotificationInfo(new String[]{NOTIF_SERVICE_CONNECT}, Notification.class.getName(), "Initial Sookeeper connection"),
		new MBeanNotificationInfo(new String[]{NOTIF_SERVICE_DISCONNECT}, Notification.class.getName(), "Zookeeper disconnect"),
		new MBeanNotificationInfo(new String[]{NOTIF_SERVICE_RECONNECT}, Notification.class.getName(), "Zookeeper reconnect"),
		new MBeanNotificationInfo(new String[]{NOTIF_ENDPOINT_UP}, Notification.class.getName(), "Discovery of a new endpoint"),
		new MBeanNotificationInfo(new String[]{NOTIF_ENDPOINT_DOWN}, Notification.class.getName(), "Loss of a known endpoint")
	};
	
	
	/** The config key for the ZooKeeper connect string */
	public static final String ZK_CONNECT_CONF = "streamhub.discovery.zookeeper.connect";
	/** The default ZooKeeper connect string */
	public static final String ZK_CONNECT_DEFAULT = "localhost:2181";
	/** The config key for the zookeeper session timeout */
	public static final String DISC_SESS_TO_CONF = "streamhub.discovery.timeout.session";
	/** The default zookeeper session timeout in ms. */
	public static final int DISC_SESS_TO_DEFAULT = 60 * 1000;
	/** The config key for the zookeeper connect timeout */
	public static final String DISC_CONN_TO_CONF = "streamhub.discovery.timeout.connection";
	/** The default zookeeper session timeout in ms. */
	public static final int DISC_CONN_TO_DEFAULT = 15 * 1000;
	
	
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** The connect string for the zookeep ensemble */
	protected final String zkConnect;
	/** The zookeeper connect timeout in ms. */
	protected final int connectionTimeout;
	/** The zookeeper session timeout in ms. */
	protected final int sessionTimeout;
	/** Factory for notification serial numbers */
	protected final AtomicLong notifSerial = new AtomicLong();
	/** A counter of up endpoint events */
	protected final LongAdder upEvents = new LongAdder();
	/** A counter of down endpoint events */
	protected final LongAdder downEvents = new LongAdder();

	/** The zookeeper curator framework instance to listen with */
	protected final CuratorFramework curator;
	/** Indicates if the curator client is currently connected */
	protected final AtomicBoolean connected = new AtomicBoolean(false);
	/** Indicates if a curator client disconnect is intended */
	protected final AtomicBoolean intendToClose = new AtomicBoolean(false);
	/** The service cache thread factory */
	protected final ThreadFactory threadFactory = new ThreadFactory() {
		final AtomicInteger serial = new AtomicInteger();
		@Override
		public Thread newThread(final Runnable r) {
			final Thread t = new Thread(r, "CustomEndpointListenerThread#" + serial.incrementAndGet());
			t.setDaemon(true);
			return t;
		}
	};
	/** The service cache callback executor */
	protected final ExecutorService executor = Executors.newCachedThreadPool(threadFactory);
	
//	/** A set of registered endpoints */
//	protected final ConcurrentHashMap<String, AdvertisedEndpoint> registered = new ConcurrentHashMap<String, AdvertisedEndpoint>(); 
//	/** A set of registered AdvertisedEndpoint event listeners */
//	protected final Set<AdvertisedEndpointListener> listeners = new LinkedHashSet<AdvertisedEndpointListener>();
	
	
	


	/**
	 * Acquires the singleton CustomEndpointListenerService instance
	 * @return the singleton CustomEndpointListenerService instance
	 */
	public static CustomEndpointListenerService getInstance() {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					instance = new CustomEndpointListenerService();
				}
			}
		}
		return instance;
	}
	
	/**
	 * Creates a new CustomEndpointListener
	 */
	private CustomEndpointListenerService() {
		log.info(">>>>> Starting CustomEndpointListenerService....");
		CloseableService.getInstance().register(this);
		zkConnect = ConfigurationHelper.getSystemThenEnvProperty(ZK_CONNECT_CONF, ZK_CONNECT_DEFAULT);
		log.info("EndpointListener ZooKeep Connect: [{}]", zkConnect);
		connectionTimeout = ConfigurationHelper.getIntSystemThenEnvProperty(DISC_CONN_TO_CONF, DISC_CONN_TO_DEFAULT);
		sessionTimeout = ConfigurationHelper.getIntSystemThenEnvProperty(DISC_SESS_TO_CONF, DISC_SESS_TO_DEFAULT);
		curator = CuratorFrameworkFactory.newClient(zkConnect, sessionTimeout, connectionTimeout, new ExponentialBackoffRetry( 1000, 3 ));
		curator.getConnectionStateListenable().addListener(this);
		log.info("ZK Curator starting...");
		curator.start();
		log.info("<<<<< CustomEndpointListenerService started.");
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.curator.framework.recipes.cache.TreeCacheListener#childEvent(org.apache.curator.framework.CuratorFramework, org.apache.curator.framework.recipes.cache.TreeCacheEvent)
	 */
	@Override
	public void childEvent(final CuratorFramework client, final TreeCacheEvent event) throws Exception {
		
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.curator.framework.state.ConnectionStateListener#stateChanged(org.apache.curator.framework.CuratorFramework, org.apache.curator.framework.state.ConnectionState)
	 */
	@Override
	public void stateChanged(final CuratorFramework client, final ConnectionState newState) {
		
	}

	/**
	 * {@inheritDoc}
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() throws IOException {
		log.info(">>>>> Stopping CustomEndpointListenerService....");
		
		
		log.info("<<<<< CustomEndpointListenerService stopped.");
		
	}

}
