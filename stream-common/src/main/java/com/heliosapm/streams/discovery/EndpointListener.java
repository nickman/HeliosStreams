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
package com.heliosapm.streams.discovery;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import javax.management.MBeanNotificationInfo;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.data.Stat;

import com.heliosapm.streams.json.JSONOps;
import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.io.CloseableService;
import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.jmx.SharedNotificationExecutor;

/**
 * <p>Title: EndpointListener</p>
 * <p>Description: Listener for new and disappeared monitoring endpoints</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.discovery.EndpointListener</code></p>
 */

public class EndpointListener extends NotificationBroadcasterSupport implements EndpointListenerMBean, ConnectionStateListener, TreeCacheListener, Closeable {
	/** The singleton instance */
	private static volatile EndpointListener instance = null;
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

	/** The config key for the advertised service type */
	public static final String SERVICE_TYPE_CONF = "streamhub.discovery.servicetype";
	/** The default advertised service type */
	public static final String SERVICE_TYPE_DEFAULT = "/monitoring-endpoints";

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
	/** The endpoint service type */
	protected final String serviceType;
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
	/** The cache instance */
	protected TreeCache treeCache;
	
	/** Indicates if the curator client is currently connected */
	protected final AtomicBoolean connected = new AtomicBoolean(false);
	/** Indicates if a curator client disconnect is intended */
	protected final AtomicBoolean intendToClose = new AtomicBoolean(false);
	/** The service cache thread factory */
	protected final ThreadFactory threadFactory = new ThreadFactory() {
		final AtomicInteger serial = new AtomicInteger();
		@Override
		public Thread newThread(final Runnable r) {
			final Thread t = new Thread(r, "EndpointListenerThread#" + serial.incrementAndGet());
			t.setDaemon(true);
			return t;
		}
	};
	/** The service cache callback executor */
	protected final ExecutorService executor = Executors.newCachedThreadPool(threadFactory);
	
	
	/** A set of registered endpoints */
	protected final ConcurrentHashMap<String, AdvertisedEndpoint> registered = new ConcurrentHashMap<String, AdvertisedEndpoint>(); 
	/** A set of registered AdvertisedEndpoint event listeners */
	protected final Set<AdvertisedEndpointListener> listeners = new LinkedHashSet<AdvertisedEndpointListener>();
	/**
	 * Acquires the listener instance
	 * @param listeners an optional array of endpoint listeners to add
	 * @return the listener instance
	 */
	public static EndpointListener getInstance(final AdvertisedEndpointListener...listeners) {
		boolean created = false;
		if(instance == null) {
			synchronized(lock) {
				if(instance == null) {
					created = true;
					instance = new EndpointListener(listeners);
				}
			}
		}
		if(!created) {
			for(AdvertisedEndpointListener listener : listeners) {
				instance.addEndpointListener(listener);
			}
		}
		return instance;
	}
	
	
	/**
	 * Creates a new EndpointPubSub
	 * @param listeners an optional array of endpoint listeners to add
	 */
	private EndpointListener(final AdvertisedEndpointListener...listeners) {
		super(SharedNotificationExecutor.getInstance(), NOTIF_INFOS);
		for(AdvertisedEndpointListener listener : listeners) {
			instance.addEndpointListener(listener);
		}		
		CloseableService.getInstance().register(this);
		log.info("ZK_CONNECT_CONF: [{}]", System.getProperty(ZK_CONNECT_CONF, "undefined"));
		zkConnect = ConfigurationHelper.getSystemThenEnvProperty(ZK_CONNECT_CONF, ZK_CONNECT_DEFAULT);
		log.info("EndpointListener ZooKeep Connect: [{}]", zkConnect);
		serviceType = ConfigurationHelper.getSystemThenEnvProperty(SERVICE_TYPE_CONF, SERVICE_TYPE_DEFAULT);
		connectionTimeout = ConfigurationHelper.getIntSystemThenEnvProperty(DISC_CONN_TO_CONF, DISC_CONN_TO_DEFAULT);
		sessionTimeout = ConfigurationHelper.getIntSystemThenEnvProperty(DISC_SESS_TO_CONF, DISC_SESS_TO_DEFAULT);
		curator = CuratorFrameworkFactory.newClient(zkConnect, sessionTimeout, connectionTimeout, new ExponentialBackoffRetry( 1000, 3 ));
		curator.getConnectionStateListenable().addListener(this);
		curator.start();
		treeCache = TreeCache.newBuilder(curator, serviceType) 
			.setCacheData(true)
			.setCreateParentNodes(true)
			.setExecutor(executor)
			.setMaxDepth(5)
			.build();
		treeCache.getListenable().addListener(this, executor);
		try {
			JMXHelper.registerMBean(OBJECT_NAME, this);
		} catch (Exception ex) {
			log.warn("Failed to register management interface. Will continue without.", ex);
		}		
		connectClients();
	}
	
	/**
	 * Connects the tree cache
	 */
	protected void connectClients() {
		try {
			treeCache.start();
		} catch (Exception ex) {
			throw new RuntimeException("Failed to connect the path cache", ex);
		}
	}

	/**
	 * Stops and closes the publisher
	 * @throws IOException Won't be thrown, but required.
	 */
	public void close() throws IOException {
		intendToClose.set(true);		
		try { treeCache.close(); } catch (Exception x) {/* No Op */}
		try { curator.close(); } catch (Exception x) {/* No Op */}
		
		registered.clear();		
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.apache.curator.framework.state.ConnectionStateListener#stateChanged(org.apache.curator.framework.CuratorFramework, org.apache.curator.framework.state.ConnectionState)
	 */
	@Override
	public void stateChanged(final CuratorFramework client, final ConnectionState newState) {
		log.info("ZK Connection State Change to [{}]", newState.name());
		connected.set(newState.isConnected());
		switch(newState) {			
			case CONNECTED:	
				sendNotification(new Notification(NOTIF_SERVICE_CONNECT, OBJECT_NAME, notifSerial.incrementAndGet(), System.currentTimeMillis(), "EndpointListener connected to Zookeeper at [" + zkConnect + "]"));
				break;
			case LOST:
				final int lostEndpoints = registered.size();
				registered.clear();
				sendNotification(new Notification(NOTIF_SERVICE_DISCONNECT, OBJECT_NAME, notifSerial.incrementAndGet(), System.currentTimeMillis(), "EndpointListener lost connection to Zookeeper [" + zkConnect + "]. Lost [" + lostEndpoints + "] endpoints"));
				break;
			case READ_ONLY:
				break;
			case RECONNECTED:
				sendNotification(new Notification(NOTIF_SERVICE_RECONNECT, OBJECT_NAME, notifSerial.incrementAndGet(), System.currentTimeMillis(), "EndpointListener re-connected to Zookeeper at [" + zkConnect + "]"));
				break;
			case SUSPENDED:
				break;
			default:
				break;
		}		
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.apache.curator.framework.recipes.cache.TreeCacheListener#childEvent(org.apache.curator.framework.CuratorFramework, org.apache.curator.framework.recipes.cache.TreeCacheEvent)
	 */
	@Override
	public void childEvent(final CuratorFramework client, final TreeCacheEvent event) throws Exception {
		final String path;
		final long eph;
		final ChildData childData = event.getData();
		if(childData!=null) {
			path = childData.getPath();
			final Stat stat = childData.getStat();
			if(stat!=null) {
				eph = stat.getEphemeralOwner();
			} else {
				eph = -1;
			}
		} else {
			path = "{}";
			eph = -1;
		}
		
		log.debug("Cache Change [{}] --> [{}], eph:{}", event.getType().name(), path, eph);
		switch(event.getType()) {
			case NODE_ADDED:
				if(childData!=null) {
					onNodeAdded(childData);
				}
				break;
			case NODE_REMOVED:
				if(childData!=null) {
					onNodeRemoved(childData);
				}			
				break;
			case NODE_UPDATED:
				break;
			default:
				break;
		}
	}
	

	/**
	 * Callback when an endpoint is removed
	 * @param data the data representing the endpoint 
	 */
	protected void onNodeAdded(final ChildData data) {
		final Stat stat = data.getStat();
		if(stat!=null) {
			if(stat.getEphemeralOwner() > 0) {
				final byte[] bytes = data.getData();
				final AdvertisedEndpoint ae = JSONOps.parseToObject(bytes, AdvertisedEndpoint.class);
				registered.put(data.getPath(), ae);
				log.info("Discovered Endpoint: [{}]", ae);
				fireOnlinedEndpoint(ae);
				final Notification notif = new Notification(NOTIF_ENDPOINT_UP, OBJECT_NAME, notifSerial.incrementAndGet(), System.currentTimeMillis(), "EndpointListener discovered new endpoint [" + ae + "]");
				notif.setUserData(JSONOps.serializeToString(ae));
				sendNotification(notif);
				upEvents.increment();
			}
		}
	}
	
	/**
	 * Callback when an endpoint is removed
	 * @param data the data representing the endpoint 
	 */
	protected void onNodeRemoved(final ChildData data) {
		final Stat stat = data.getStat();
		if(stat!=null) {
			if(stat.getEphemeralOwner() > 0) {
				final byte[] bytes = data.getData();
				final AdvertisedEndpoint ae = JSONOps.parseToObject(bytes, AdvertisedEndpoint.class);
				registered.remove(data.getPath());
				log.info("Endpoint Down: [{}]", ae);
				fireOfflinedEndpoint(ae);
				final Notification notif = new Notification(NOTIF_ENDPOINT_DOWN, OBJECT_NAME, notifSerial.incrementAndGet(), System.currentTimeMillis(), "Endpoint down [" + ae + "]");
				notif.setUserData(JSONOps.serializeToString(ae));
				sendNotification(notif);
				downEvents.increment();
			}
		}	
	}
	
	
	/**
	 * Notifies registered listeners of a new endpoint
	 * @param endpoint the new endpoint
	 */
	protected void fireOnlinedEndpoint(final AdvertisedEndpoint endpoint) {
		for(final AdvertisedEndpointListener listener: listeners) {
			executor.submit(new Runnable(){
				@Override
				public void run() {
					listener.onOnlineAdvertisedEndpoint(endpoint);
				}
			});
		}
	}
	
	/**
	 * Notifies registered listeners of a lost endpoint
	 * @param endpoint the lost endpoint
	 */
	protected void fireOfflinedEndpoint(final AdvertisedEndpoint endpoint) {
		for(final AdvertisedEndpointListener listener: listeners) {
			executor.submit(new Runnable(){
				@Override
				public void run() {
					listener.onOfflineAdvertisedEndpoint(endpoint);
				}
			});
		}		
	}
	
	/**
	 * Adds a listener to be notified on endpoint events
	 * @param listener the listener to register
	 */
	public void addEndpointListener(final AdvertisedEndpointListener listener) {
		if(listener!=null) {
			listeners.add(listener);
		}
	}
	
	/**
	 * Removes a registered endpoint listener
	 * @param listener the listener to remove
	 */
	public void removeEndpointListener(final AdvertisedEndpointListener listener) {
		if(listener!=null) {
			listeners.remove(listener);
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.discovery.EndpointListenerMBean#getListenerCount()
	 */
	@Override
	public int getListenerCount() {
		return listeners.size();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.discovery.EndpointListenerMBean#getZkConnection()
	 */
	@Override
	public String getZkConnection() {
		return zkConnect;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.discovery.EndpointListenerMBean#getServiceType()
	 */
	@Override
	public String getServiceType() {
		return serviceType;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.discovery.EndpointListenerMBean#getConnectionTimeout()
	 */
	@Override
	public int getConnectionTimeout() {
		return connectionTimeout;
	}


	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.discovery.EndpointListenerMBean#getSessionTimeout()
	 */
	@Override
	public int getSessionTimeout() {
		return sessionTimeout;
	}


	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.discovery.EndpointListenerMBean#isConnected()
	 */
	@Override
	public boolean isConnected() {
		return connected.get();
	}
	

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.discovery.EndpointListenerMBean#getUpEvents()
	 */
	@Override
	public long getUpEvents() {
		return upEvents.longValue();
	}


	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.discovery.EndpointListenerMBean#getDownEvents()
	 */
	@Override
	public long getDownEvents() {
		return downEvents.longValue();
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.discovery.EndpointListenerMBean#getRegisteredEndpoints()
	 */
	@Override
	public int getRegisteredEndpoints() {
		return registered.size();
	}

	public static void main(String[] args) {
		log("ListenerTest");
		JMXHelper.fireUpJMXMPServer(1209);
		final EndpointListener p = EndpointListener.getInstance();
		p.addEndpointListener(new AdvertisedEndpointListener() {
			public void onOnlineAdvertisedEndpoint(final AdvertisedEndpoint endpoint) {
				log("ONLINE:" + endpoint);
			}
			public void onOfflineAdvertisedEndpoint(final AdvertisedEndpoint endpoint) {
				elog("OFFLINE:" + endpoint);
			}
		});
		
		StdInCommandHandler.getInstance()
		.registerCommand("stop", new Runnable(){
			public void run() {
				try {
					p.close();
				} catch (Exception x) {/* No Op */}				
				System.exit(0);
			}
		})
		.registerCommand("up", new Runnable(){
			public void run() {
				log("UP NODES:" + p.registered.size());
			}
		})
		
		.run();

		
	}
	
	public static void log(Object msg) {
		System.out.println(msg);
	}
	
	public static void elog(Object msg) {
		System.err.println(msg);
	}




	
}
