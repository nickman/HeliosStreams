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
package com.heliosapm.streams.collector.jmx.discovery;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.cliffc.high_scale_lib.NonBlockingHashSet;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ApplicationContextEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.ContextStoppedEvent;
import org.springframework.stereotype.Component;

import com.google.common.io.Files;
import com.heliosapm.streams.collector.groovy.ManagedScriptFactory;
import com.heliosapm.streams.discovery.AdvertisedEndpoint;
import com.heliosapm.streams.discovery.AdvertisedEndpointListener;
import com.heliosapm.streams.discovery.EndpointListener;
import com.heliosapm.utils.tuples.NVP;

/**
 * <p>Title: EndpointDiscoveryService</p>
 * <p>Description: Listens on zookeeper events for registered JMX monitoring endpoints and starts a dynamic monitor for them</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.jmx.discovery.EndpointDiscoveryService</code></p>
 */
@Component
public class EndpointDiscoveryService implements AdvertisedEndpointListener, ApplicationContextAware, ApplicationListener<ApplicationContextEvent> {
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** The endpoint listener */
	protected EndpointListener endpointListener = null;
	
	/** The injected application context */
	protected ApplicationContext appCtx = null;
	/** Indicates if service is started */
	protected final AtomicBoolean started = new AtomicBoolean(false);
	/** The discovery dynamic script directory where we will deploy scripts for dynamically discovered endpoints */
	protected File dynamicDirectory = null;
	/** The root directory for endpoint activation scripts */
	protected File endpointTemplateDirectory = null;
	/** The script factory reference */
	protected ManagedScriptFactory scriptFactory = null;
	/** NVPs of the endpoint instance and a set of the deployed scripts keyed by the endpoint id */
	protected final NonBlockingHashMap<String, NVP<AdvertisedEndpoint, Set<File>>> deployments = new NonBlockingHashMap<String, NVP<AdvertisedEndpoint, Set<File>>>(); 
	
	/**
	 * Creates a new EndpointDiscoveryService
	 */
	public EndpointDiscoveryService() {
		log.info("Created EndpointDiscoveryService");
	}
	
	/**
	 * Starts the listener
	 */
	protected void start() {
		// TODO: clear the dynamic dir
		if(started.compareAndSet(false, true)) {
			log.info(">>>>> Starting EndpointDiscoveryService...");
			endpointListener = EndpointListener.getInstance();
			endpointListener.addEndpointListener(this);			
			scriptFactory = ManagedScriptFactory.getInstance();
			dynamicDirectory = scriptFactory.getDynamicDirectory();
			endpointTemplateDirectory = new File(scriptFactory.getTemplateDirectory(), "endpoints");
			log.info("<<<<< EndpointDiscoveryService Started.");
		}
	}
	
	/**
	 * Stops the listener
	 */
	protected void stop() {
		if(started.compareAndSet(true, false)) {
			log.info(">>>>> Stoppping EndpointDiscoveryService...");
			endpointListener.removeEndpointListener(this);
			try {
				endpointListener.close();
			} catch (Exception x) {/* No Op */}
			log.info("<<<<< EndpointDiscoveryService Stopped.");			
		}
		
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.discovery.AdvertisedEndpointListener#onOnlineAdvertisedEndpoint(com.heliosapm.streams.discovery.AdvertisedEndpoint)
	 */
	@Override
	public void onOnlineAdvertisedEndpoint(final AdvertisedEndpoint endpoint) {
		log.info("Endpoint UP [{}]", endpoint);
		try {
			final File appDirectory = createEndpointMonitorDirectory(endpoint);
		} catch (Exception ex) {
			log.error("Failed to activate monitoring for endpoint [{}]", endpoint, ex);
		}
	}
	
	/**
	 * Creates the script directory for the passed activated monitoring endpoint
	 * @param endpoint The activated endpoint
	 * @return the directory the scripts will be written to
	 */
	protected File createEndpointMonitorDirectory(final AdvertisedEndpoint endpoint) {
		final File hostDirectory = new File(dynamicDirectory, endpoint.getHost());
		final File appDirectory = new File(hostDirectory, endpoint.getApp() + "-" + endpoint.getPort());
		if(appDirectory.exists()) {
			if(appDirectory.isFile()) throw new RuntimeException("AppDirectory [" + appDirectory + "] is a file");
		} else {
			if(!appDirectory.mkdirs()) throw new RuntimeException("Failed to create AppDirectory [" + appDirectory + "] is a file");
			log.info("Created AppDirectory [{}]",  appDirectory);
		}		
		return appDirectory;
	}
	
	/**
	 * @param endpoint
	 * TODO: add execution schedule configuration options
	 */
	protected void deployEndpointMonitors(final AdvertisedEndpoint endpoint) {
		final File appDirectory = createEndpointMonitorDirectory(endpoint);
		final Map<String, Object> bindings = new HashMap<String, Object>();
		
		final Set<File> deployedFiles = new NonBlockingHashSet<File>();
		for(String endpointType: endpoint.getEndPoints()) {
			File templateScript = new File(endpointTemplateDirectory, endpointType + "-15s.groovy");
			if(!templateScript.canRead()) {
				log.warn("No endpoint script found for endpoint type [{}]",  endpointType);
				continue;
			}
			File deployedScript = new File(appDirectory, endpointType + ".groovy");
			if(deployedScript.exists()) deployedScript.delete();
			try {
				Files.copy(templateScript, deployedScript);
			} catch (IOException iex) {
				log.error("Failed to copy template [{}} to deployment [{}]", templateScript, deployedScript, iex);
				continue;
			}
			log.info("Activating [{}].....", deployedScript);
			scriptFactory.compileScript(deployedScript);
			deployedFiles.add(deployedScript);
		}
		final NVP<AdvertisedEndpoint, Set<File>> deploys = new NVP<AdvertisedEndpoint, Set<File>>(endpoint, deployedFiles);
		this.deployments.put(endpoint.getId(), deploys);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.discovery.AdvertisedEndpointListener#onOfflineAdvertisedEndpoint(com.heliosapm.streams.discovery.AdvertisedEndpoint)
	 */
	@Override
	public void onOfflineAdvertisedEndpoint(final AdvertisedEndpoint endpoint) {
		log.info("Endpoint DOWN [{}]", endpoint);
	}

	/**
	 * @param appCtx
	 * @throws BeansException
	 */
	@Override
	public void setApplicationContext(final ApplicationContext appCtx) throws BeansException {
		this.appCtx = appCtx;
		
	}


	/**
	 * {@inheritDoc}
	 * @see org.springframework.context.ApplicationListener#onApplicationEvent(org.springframework.context.ApplicationEvent)
	 */
	@Override
	public void onApplicationEvent(final ApplicationContextEvent event) {
		if(event instanceof ContextRefreshedEvent) {
			start();
		} else if(event instanceof ContextStoppedEvent) {
			stop();
		}		
	}

	

}
