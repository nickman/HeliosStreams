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

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.ServiceCacheListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.heliosapm.streams.json.JSONOps;
import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.io.StdInCommandHandler;

/**
 * <p>Title: EndpointPubSub</p>
 * <p>Description: Publishes advertised endpoints to zookeeper</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.discovery.EndpointPubSub</code></p>
 */

public class EndpointPubSub implements ServiceCacheListener {
	/** The singleton instance */
	private static volatile EndpointPubSub instance = null;
	/** The singleton instance ctor lock */
	private static final Object lock = new Object();
	
	/** The config key for the ZooKeeper connect string */
	public static final String ZK_CONNECT_CONF = "streamhub.discovery.zookeeper.connect";
	/** The default ZooKeeper connect string */
	public static final String ZK_CONNECT_DEFAULT = "localhost:2181";

	/** The config key for the advertised service type */
	public static final String SERVICE_TYPE_CONF = "streamhub.discovery.servicetype";
	/** The default advertised service type */
	public static final String SERVICE_TYPE_DEFAULT = "monitoring-endpoints";

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
	
	/** The zookeeper curator framework instance to publish with */
	protected final CuratorFramework curator;
	/** Indicates if the curator client is currently connected */
	protected final AtomicBoolean connected = new AtomicBoolean(false);
	/** Indicates if a curator client disconnect is intended */
	protected final AtomicBoolean intendToClose = new AtomicBoolean(false);
	/** The service discovery instance */
	protected final ServiceDiscovery<AdvertisedEndpoint> serviceDiscovery;
	/** The service discovery cache */
	protected final ServiceCache<AdvertisedEndpoint> serviceCache;
	/** The service cache thread factory */
	protected final ThreadFactory threadFactory = new ThreadFactory() {
		final AtomicInteger serial = new AtomicInteger();
		@Override
		public Thread newThread(final Runnable r) {
			final Thread t = new Thread(r, "ServiceDiscoveryCacheThread#" + serial.incrementAndGet());
			t.setDaemon(true);
			return t;
		}
	};
	/** The service cache callback executor */
	protected final ExecutorService executor = Executors.newCachedThreadPool(threadFactory);
	
	/** The seri<alizer */
	protected final JsonInstanceSerializer<AdvertisedEndpoint> serializer = new JsonInstanceSerializer<AdvertisedEndpoint>(); 
	
	/** A set of unregistered endpoints */
	protected final ConcurrentHashMap<String, ServiceInstance<AdvertisedEndpoint>> unregistered = new ConcurrentHashMap<String, ServiceInstance<AdvertisedEndpoint>>(); 
	/** A set of registered endpoints */
	protected final ConcurrentHashMap<String, ServiceInstance<AdvertisedEndpoint>> registered = new ConcurrentHashMap<String, ServiceInstance<AdvertisedEndpoint>>(); 
	/** A set of registered AdvertisedEndpoint event listeners */
	protected final Set<AdvertisedEndpointListener> listeners = new LinkedHashSet<AdvertisedEndpointListener>();
	/**
	 * Acquires the publisher instance
	 * @return the publisher instance
	 */
	public static EndpointPubSub getInstance() {
		if(instance == null) {
			synchronized(lock) {
				if(instance == null) {
					instance = new EndpointPubSub();
				}
			}
		}
		return instance;
	}
	
	
	/**
	 * Creates a new EndpointPubSub
	 */
	public EndpointPubSub() {
		zkConnect = ConfigurationHelper.getSystemThenEnvProperty(ZK_CONNECT_CONF, ZK_CONNECT_DEFAULT);
		serviceType = ConfigurationHelper.getSystemThenEnvProperty(SERVICE_TYPE_CONF, SERVICE_TYPE_DEFAULT);
		connectionTimeout = ConfigurationHelper.getIntSystemThenEnvProperty(DISC_CONN_TO_CONF, DISC_CONN_TO_DEFAULT);
		sessionTimeout = ConfigurationHelper.getIntSystemThenEnvProperty(DISC_SESS_TO_CONF, DISC_SESS_TO_DEFAULT);
		curator = CuratorFrameworkFactory.newClient(zkConnect, sessionTimeout, connectionTimeout, new ExponentialBackoffRetry( 1000, 3 ));
		curator.getConnectionStateListenable().addListener(this);
		curator.start();
		serviceDiscovery = ServiceDiscoveryBuilder.builder(AdvertisedEndpoint.class)
			.basePath(serviceType)
			.client(curator)
			.serializer(serializer)
			.watchInstances(true)
			.build();	
		serviceCache = serviceDiscovery.serviceCacheBuilder()
			.name(serviceType)
			.threadFactory(threadFactory)
			.build();		
		serviceCache.addListener(this, executor);
		
		try { 
			serviceDiscovery.start();
			serviceCache.start();
			cacheChanged();
		} catch (Exception ex) {
			throw new RuntimeException("Failed to start EndpointPubSub", ex);
		} 
	}
	
	public static void main(String[] args) {
		log("PublisherTest");
		final String template = "{ \"jmx\" : \"service:jmx:jmxmp://localhost:%s\", \"host\" : \"njwmint\", \"app\" : \"%s\",  \"port\" : \"%s\"," + 
				"\"endpoints\" : [\"kafka\", \"jvm\"] }";
		final EndpointPubSub p = EndpointPubSub.getInstance();
		p.addEndpointListener(new AdvertisedEndpointListener() {
			public void onOnlineAdvertisedEndpoint(final AdvertisedEndpoint endpoint) {
				log("ONLINE:" + endpoint);
			}
			public void onOfflineAdvertisedEndpoint(final AdvertisedEndpoint endpoint) {
				elog("OFFLINE:" + endpoint);
			}
		});
		for(int i = 0; i < 20; i++) {
			String s = String.format(template, 1420 + i, "FooApp", 1420 + i);
			log("  ---- REG:" + s);
			final AdvertisedEndpoint ae = JSONOps.parseToObject(s, AdvertisedEndpoint.class);
			p.register(ae);
		}
		StdInCommandHandler.getInstance()
		.registerCommand("stop", new Runnable(){
			public void run() {
				p.close();
				System.exit(0);
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
	
	/**
	 * Stops and closes the publisher
	 */
	public void close() {
		intendToClose.set(true);
		try { serviceCache.close(); } catch (Exception x) {/* No Op */}
		try { serviceDiscovery.close(); } catch (Exception x) {/* No Op */}
		try { curator.close(); } catch (Exception x) {/* No Op */}
		unregistered.clear();
		registered.clear();		
	}
	
	/**
	 * <p>Called when the ServiceInstance cache changes</p>
	 * {@inheritDoc}
	 * @see org.apache.curator.x.discovery.details.ServiceCacheListener#cacheChanged()
	 */
	@Override
	public void cacheChanged() {
		final List<ServiceInstance<AdvertisedEndpoint>> cached =  serviceCache.getInstances();
		synchronized(serviceCache) {			
			final Set<String> knowRegistereds = new HashSet<String>(registered.keySet());
			for(ServiceInstance<AdvertisedEndpoint> si: cached) {
				knowRegistereds.remove(si.getId());
				final boolean reg = registered.containsKey(si.getId());
				final boolean notreg = unregistered.containsKey(si.getId());
				if(reg) {
					if(notreg) {
						log.info("Mysterious event. Event [{}] marked as registered and unregistered", si.getPayload());
						// we'll assume it's a new up						
						unregistered.remove(si.getId());
						fireOnlinedEndpoint(si.getPayload());
					} else {
						log.info("Confirmed registration of event [{}]", si.getPayload());
						// Noop
					}
				} else {
					if(notreg) {
						// endpoint marked unregistered came back to life
						registered.put(si.getId(), si);
						unregistered.remove(si.getId());
						fireOnlinedEndpoint(si.getPayload());
					} else {
						// endpoint outside of this vm was registered
						registered.put(si.getId(), si);
						fireOnlinedEndpoint(si.getPayload());						
					}
				}
				// now we know these guys are down
				for(String downKey : knowRegistereds) {
					final ServiceInstance<AdvertisedEndpoint> downSi = registered.remove(downKey);
					if(downSi!=null) {
						fireOffinedEndpoint(downSi.getPayload());
					}
				}
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
	
	protected void fireOffinedEndpoint(final AdvertisedEndpoint endpoint) {
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
	
	
	@Override
	public void stateChanged(final CuratorFramework client, final ConnectionState newState) {
		log.info("ZK Connection State Change to [{}]", newState.name());
		connected.set(newState.isConnected());
		switch(newState) {			
			case CONNECTED:							
				registerPending();
				break;
			case LOST:
				setAllPending();
				break;
			case READ_ONLY:
				break;
			case RECONNECTED:
				registerPending();
				break;
			case SUSPENDED:
				break;
			default:
				break;
		
		}		
	}
	
	
	
	/**
	 * Attempts to register the passed end point.
	 * If registration fails or the client is disconnected, will retry on connection resumption.
	 * @param endpoint The endpoint to register
	 */
	public void register(final AdvertisedEndpoint endpoint) {
		if(endpoint==null) throw new IllegalArgumentException("The passed endpoint was null");
		final ServiceInstance<AdvertisedEndpoint> si = endpoint.getServiceInstance();		
		unregistered.put(si.getId(), si);
		if(connected.get()) {
			try {
				try {
					curator.getZookeeperClient().getZooKeeper().delete(endpoint.getZkPath(serviceType), -1);
				} catch (Exception x) {/* No Op */}
				serviceDiscovery.registerService(si);
				registered.put(si.getId(), unregistered.remove(si.getId()));
				fireOnlinedEndpoint(endpoint);
			} catch (Exception ex) {
				log.error("Failed to register endpoint [{}]", endpoint, ex);
			}
		}
	}
	
	/**
	 * Attempts to register the passed ServiceInstance.
	 * If registration fails or the client is disconnected, will retry on connection resumption.
	 * @param serviceInstance The ServiceInstance to register
	 */
	public void register(final ServiceInstance<AdvertisedEndpoint> serviceInstance) {
		if(serviceInstance==null) throw new IllegalArgumentException("The passed ServiceInstance was null");
		unregistered.put(serviceInstance.getId(), serviceInstance);
		if(connected.get()) {
			try {
				try {
					curator.getZookeeperClient().getZooKeeper().delete(serviceInstance.getPayload().getZkPath(serviceType), -1);
				} catch (Exception x) {/* No Op */}
				
				serviceDiscovery.registerService(serviceInstance);
				registered.put(serviceInstance.getId(), unregistered.remove(serviceInstance));
				fireOnlinedEndpoint(serviceInstance.getPayload());
			} catch (Exception ex) {
				log.error("Failed to register endpoint [{}]", serviceInstance.getPayload(), ex);
			}
		}
	}
	
	/**
	 * Registers pending or disconnected endpoints
	 */
	protected void registerPending() {
		LinkedHashSet<ServiceInstance<AdvertisedEndpoint>> toRegister = new LinkedHashSet<ServiceInstance<AdvertisedEndpoint>>(unregistered.values());
		for(ServiceInstance<AdvertisedEndpoint> si: toRegister) {
			register(si);
		}
	}
	
	/**
	 * Sets all registered endpoints to unregistered
	 */
	protected void setAllPending() {
		unregistered.putAll(registered);
		registered.clear();
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



}
