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

import java.util.LinkedHashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
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
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.curator.x.discovery.details.ServiceCacheListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.heliosapm.utils.config.ConfigurationHelper;

/**
 * <p>Title: Publisher</p>
 * <p>Description: Publishes advertised endpoints to zookeeper</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.discovery.Publisher</code></p>
 */

public class Publisher implements ConnectionStateListener, ServiceCacheListener {
	/** The singleton instance */
	private static volatile Publisher instance = null;
	/** The singleton instance ctor lock */
	private static final Object lock = new Object();
	
	/** The config key for the ZooKeeper connect string */
	public static final String ZK_CONNECT_CONF = "streamhub.config.zookeeperconnect";
	/** The default ZooKeeper connect string */
	public static final String ZK_CONNECT_DEFAULT = "localhost:2181";

	/** The config key for the advertised service type */
	public static final String SERVICE_TYPE_CONF = "streamhub.config.servicetype";
	/** The default advertised service type */
	public static final String SERVICE_TYPE_DEFAULT = "monitoring-endpoints";
	
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** The connect string for the zookeep ensemble */
	protected final String zkConnect;
	/** The endpoint service type */
	protected final String serviceType;
	
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
	
	/** The serializer */
	protected final JsonInstanceSerializer<AdvertisedEndpoint> serializer = new JsonInstanceSerializer<AdvertisedEndpoint>(AdvertisedEndpoint.class); 
	
	/** A set of unregistered endpoints */
	protected final ConcurrentHashMap<String, ServiceInstance<AdvertisedEndpoint>> unregistered = new ConcurrentHashMap<String, ServiceInstance<AdvertisedEndpoint>>(); 
	/** A set of registered endpoints */
	protected final ConcurrentHashMap<String, ServiceInstance<AdvertisedEndpoint>> registered = new ConcurrentHashMap<String, ServiceInstance<AdvertisedEndpoint>>(); 
	
	public static Publisher getInstance() {
		if(instance == null) {
			synchronized(lock) {
				if(instance == null) {
					instance = new Publisher();
				}
			}
		}
		return instance;
	}
	
	
	/**
	 * Creates a new Publisher
	 */
	public Publisher() {
		zkConnect = ConfigurationHelper.getSystemThenEnvProperty(ZK_CONNECT_CONF, ZK_CONNECT_DEFAULT);
		serviceType = ConfigurationHelper.getSystemThenEnvProperty(SERVICE_TYPE_CONF, SERVICE_TYPE_DEFAULT);
		curator = CuratorFrameworkFactory.newClient(zkConnect,  new ExponentialBackoffRetry( 1000, 3 ));
		curator.getConnectionStateListenable().addListener(this);
		curator.start();
		serviceDiscovery = ServiceDiscoveryBuilder.builder(AdvertisedEndpoint.class)
			.basePath(serviceType)
			.client(curator)
			.serializer(serializer)
			.build();	
		serviceCache = serviceDiscovery.serviceCacheBuilder()
			.name(serviceType)
			.threadFactory(threadFactory)
			.build();		
		serviceCache.addListener(this, executor);
		try { 
			serviceDiscovery.start();
			serviceCache.start();
		} catch (Exception ex) {
			throw new RuntimeException("Failed to start Publisher", ex);
		} 
	}
	
	/**
	 * Stops and closes the publisher
	 */
	public void close() {
		intendToClose.set(true);
		try { curator.close(); } catch (Exception x) {/* No Op */}
		unregistered.clear();
		registered.clear();		
	}
	
	@Override
	public void cacheChanged() {
		
		
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
				serviceDiscovery.registerService(si);
				registered.put(si.getId(), unregistered.remove(si.getId()));				
			} catch (Exception ex) {
				log.error("Failed to register endpoint [{}]", endpoint, ex);
			}
		}
	}
	
	/**
	 * Registers pending or disconnected endpoints
	 */
	protected void registerPending() {
		LinkedHashSet<AdvertisedEndpoint> toRegister = new LinkedHashSet<AdvertisedEndpoint>(unregistered);
		for(AdvertisedEndpoint ae: toRegister) {
			register(ae);
		}
	}
	
	/**
	 * Sets all registered endpoints to unregistered
	 */
	protected void setAllPending() {
		unregistered.addAll(registered);
		registered.clear();
	}



}
