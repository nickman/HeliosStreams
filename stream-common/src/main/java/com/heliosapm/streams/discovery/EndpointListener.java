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

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
import com.heliosapm.utils.io.StdInCommandHandler;

/**
 * <p>Title: EndpointListener</p>
 * <p>Description: Listener for new and disappeared monitoring endpoints</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.discovery.EndpointListener</code></p>
 */

public class EndpointListener implements ConnectionStateListener, TreeCacheListener {
	/** The singleton instance */
	private static volatile EndpointListener instance = null;
	/** The singleton instance ctor lock */
	private static final Object lock = new Object();
	
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
		for(AdvertisedEndpointListener listener : listeners) {
			instance.addEndpointListener(listener);
		}		
		zkConnect = ConfigurationHelper.getSystemThenEnvProperty(ZK_CONNECT_CONF, ZK_CONNECT_DEFAULT);
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
				//new PathChildrenCache(curator, serviceType, true, false, executor);
		treeCache.getListenable().addListener(this, executor);
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
	 */
	public void close() {
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
				break;
			case LOST:
				registered.clear();
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
		
		log.info("Cache Change [{}] --> [{}], eph:{}", event.getType().name(), path, eph);
		switch(event.getType()) {
		case CONNECTION_LOST:
			break;
		case CONNECTION_RECONNECTED:
			break;
		case CONNECTION_SUSPENDED:
			break;
		case INITIALIZED:
			break;
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
			}
		}
	
	}
	
	
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
	 * Returns 
	 * @return the serviceType
	 */
	public String getServiceType() {
		return serviceType;
	}
	
	/**
	 * Returns 
	 * @return the connectionTimeout
	 */
	public int getConnectionTimeout() {
		return connectionTimeout;
	}


	/**
	 * Returns 
	 * @return the sessionTimeout
	 */
	public int getSessionTimeout() {
		return sessionTimeout;
	}


	/**
	 * Returns 
	 * @return the connected
	 */
	public AtomicBoolean getConnected() {
		return connected;
	}

	public static void main(String[] args) {
		log("ListenerTest");
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
				p.close();
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
