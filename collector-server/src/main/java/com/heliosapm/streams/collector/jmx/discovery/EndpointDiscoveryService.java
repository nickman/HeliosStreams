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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.management.remote.JMXServiceURL;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.cliffc.high_scale_lib.NonBlockingHashSet;
//import org.springframework.beans.BeansException;
//import org.springframework.context.ApplicationContext;
//import org.springframework.context.ApplicationContextAware;
//import org.springframework.context.ApplicationListener;
//import org.springframework.context.event.ApplicationContextEvent;
//import org.springframework.context.event.ContextRefreshedEvent;
//import org.springframework.context.event.ContextStoppedEvent;
//import org.springframework.stereotype.Component;

import com.google.common.io.Files;
import com.heliosapm.streams.collector.groovy.ManagedScriptFactory;
import com.heliosapm.streams.collector.jmx.JMXClient;
import com.heliosapm.streams.discovery.AdvertisedEndpoint;
import com.heliosapm.streams.discovery.AdvertisedEndpointListener;
import com.heliosapm.streams.discovery.EndpointListener;
import com.heliosapm.utils.enums.TimeUnitSymbol;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.tuples.NVP;
import com.heliosapm.utils.url.URLHelper;

/**
 * <p>Title: EndpointDiscoveryService</p>
 * <p>Description: Listens on zookeeper events for registered JMX monitoring endpoints and starts a dynamic monitor for them</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.jmx.discovery.EndpointDiscoveryService</code></p>
 */
//@Component
public class EndpointDiscoveryService implements AdvertisedEndpointListener { //, ApplicationContextAware, ApplicationListener<ApplicationContextEvent> {
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** The endpoint listener */
	protected EndpointListener endpointListener = null;
	
//	/** The injected application context */
//	protected ApplicationContext appCtx = null;
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
	/** The deployed file keyed by endpoint id */
	protected final NonBlockingHashMap<String, File> deployedFiles = new NonBlockingHashMap<String, File>();
	
	/** The format for building a pool config */
	public static final String POOL_CONFIG = 
			"factory=%s\n" +												// e.g. JMXClientPoolBuilder 
			"name=%s\n" +													// e.g. TSDB-4245 
			"jmx.serviceurl=%s" +											// e.g. service:jmx:jmxmp://localhost:4245 
			"maxtotal=%s\n" +												// e.g. 4 (the number of endpoints)
			"maxidle=%s\n" +												// same as maxtotal
			"minidle=%s\n";													// same as maxtotal
	
	/** Regex pattern to determine if a schedule directive is built into the endpoint type name */
	public static final Pattern PERIOD_PATTERN = Pattern.compile(".*\\-(\\d++[s|m|h|d]){1}$", Pattern.CASE_INSENSITIVE);


			
	
	/**
	 * Creates a new EndpointDiscoveryService
	 */
	public EndpointDiscoveryService() {
		log.info("Created EndpointDiscoveryService");
	}
	
	/**
	 * Starts the listener
	 */
	public void start() {
		// TODO: clear the dynamic dir
		if(started.compareAndSet(false, true)) {
			log.info(">>>>> Starting EndpointDiscoveryService...");
			endpointListener = EndpointListener.getInstance();
			endpointListener.addEndpointListener(this);			
			scriptFactory = ManagedScriptFactory.getInstance(); //appCtx.getBean(ManagedScriptFactory.class);
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
			deployEndpointMonitors(endpoint);
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
	
	protected void undeployEndpointMonitors(final AdvertisedEndpoint endpoint) {		
		deployments.remove(endpoint.getId());
		for(String endpointType: endpoint.getEndPoints()) {
			final File script = this.deployedFiles.remove(endpoint.getId() + endpointType);
			if(script!=null) {
				try {
					scriptFactory.onDelete(script);
				} catch (Exception ex) {
					log.warn("Failed to undeploy [{}]", script, ex);
				}
			}
		}
	}
	
	/** The default endpoint collection schedule */
	private static final NVP<Long, TimeUnitSymbol> DEFAULT_SCHEDULE = new NVP<Long, TimeUnitSymbol>(15L, TimeUnitSymbol.SECONDS); 
	
	/**
	 * @param endpoint
	 * TODO: add execution schedule configuration options
	 * TODO: add jmxconnection timeout
	 */
	protected void deployEndpointMonitors(final AdvertisedEndpoint endpoint) {
		final File appDirectory = createEndpointMonitorDirectory(endpoint);
		final Set<File> deployedFiles = new NonBlockingHashSet<File>();
		final Map<String, NVP<Long, TimeUnitSymbol>> deployableEndpointTypes = new NonBlockingHashMap<String, NVP<Long, TimeUnitSymbol>>();
		for(final String endpointType: endpoint.getEndPoints()) {
			final NVP<Long, TimeUnitSymbol> schedule;
			final String endpointTypeName;
			final Matcher m = PERIOD_PATTERN.matcher(endpointType);
			if(m.matches()) {
				final String sch = m.group(1);
				schedule = TimeUnitSymbol.period(sch);
				scheduledPeriod = schedule.getKey();
				scheduledPeriodUnit = schedule.getValue().unit;			
				
			} 			
		}
		
		final Set<String> deployableEndpointTypes = Arrays.stream(endpoint.getEndPoints())
			.parallel()
			.filter(endpointType -> {
				File templateScript = new File(endpointTemplateDirectory, endpointType + ".groovy");
				if(!templateScript.canRead() || templateScript.length() == 0) {
					log.warn("No endpoint script found for endpoint type [{}]",  endpointType);
					return false;
				}
				if(ManagedScriptFactory.isDisabled(templateScript)) {
					log.warn("Endpoint script type [{}] is disabled",  endpointType);
					return false;
				}
				return true;
			}).collect(Collectors.toSet());
		
		final int poolSize;
		if(deployableEndpointTypes.isEmpty()) {
			log.warn("No active endppoint types for AdvertisedEndpoint [{}]", endpoint);
			poolSize = 1;
		} else {
			poolSize = deployableEndpointTypes.size();
		}
		
		final String poolName = deployJMXConnectionPool(endpoint, poolSize);
		
		for(String endpointType: deployableEndpointTypes) {			
			File templateScript = new File(endpointTemplateDirectory, endpointType + ".groovy");

			final Map<String, Object> bindings = new HashMap<String, Object>();
			bindings.put("pool", "pool/" + poolName);
			bindings.put("endpoint", endpoint);
			bindings.put("host", endpoint.getHost());
			bindings.put("app", endpoint.getApp());
			bindings.put("jmxurl", endpoint.getJmxUrl());
			bindings.put("jmxHelper", JMXHelper.class);
			
			File deployedScript = new File(appDirectory, endpointType + "-15s.groovy");
			if(deployedScript.exists()) deployedScript.delete();
			try {
				Files.copy(templateScript, deployedScript);
			} catch (IOException iex) {
				log.error("Failed to copy template [{}} to deployment [{}]", templateScript, deployedScript, iex);
				continue;
			}
			log.info("Activating [{}].....", deployedScript);
			scriptFactory.compileScript(deployedScript, bindings);
			deployedFiles.add(deployedScript);
			this.deployedFiles.put(endpoint.getId() + endpointType, deployedScript);
		}
		final NVP<AdvertisedEndpoint, Set<File>> deploys = new NVP<AdvertisedEndpoint, Set<File>>(endpoint, deployedFiles);
		this.deployments.put(endpoint.getId(), deploys);
	}
	
	/**
	 * Creates the configuration for a new JMXClient connection pool and writes it to the dynamic data source directory
	 * @param endpoint The endpoint to create the pool for
	 * @param maxSize The max size (and min size) of the pool 
	 * @return The pool name
	 */
	protected String deployJMXConnectionPool(final AdvertisedEndpoint endpoint, final int maxSize) {
		final JMXServiceURL jmxUrl = JMXHelper.serviceUrl(endpoint.getJmxUrl());
		final int port = jmxUrl.getPort();
		final File dynamicDataSourceDir = new File(scriptFactory.getDataSourceDirectory(), "dynamic" + File.separator + endpoint.getHost() + File.separator + endpoint.getApp() + File.separator + port);
		dynamicDataSourceDir.mkdir();
		final File dsFile = new File(dynamicDataSourceDir, String.format("jmxpool-%s-%s-%s-%s.pool", jmxUrl.getProtocol(), endpoint.getHost(), endpoint.getApp(), port));
		final String poolName = String.format("%s-%s-%s", endpoint.getHost(), endpoint.getApp(), port);
		final String dsConfig = String.format(POOL_CONFIG, "JMXClientPoolBuilder", poolName, endpoint.getJmxUrl(), maxSize, maxSize, maxSize);
		if(dsFile.exists()) dsFile.delete();
		log.info("Deploying dynamic JMXClient ObjectPool [{}]\n\tto [{}]....", poolName, dsFile.getAbsolutePath());
		URLHelper.writeToFile(dsConfig, dsFile, false);
		if(!scriptFactory.getDataSourceManager().awaitDeployment(dsFile)) {
			log.warn("Timed out waiting for pool [{}] file: [{}]", poolName, dsFile);
		} else {
			log.info("Deployed dynamic JMXClient ObjectPool [{}]", poolName);
		}
		return poolName;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.discovery.AdvertisedEndpointListener#onOfflineAdvertisedEndpoint(com.heliosapm.streams.discovery.AdvertisedEndpoint)
	 */
	@Override
	public void onOfflineAdvertisedEndpoint(final AdvertisedEndpoint endpoint) {
		log.info("Endpoint DOWN [{}]", endpoint);
		undeployEndpointMonitors(endpoint);		
	}

//	/**
//	 * @param appCtx
//	 * @throws BeansException
//	 */
//	@Override
//	public void setApplicationContext(final ApplicationContext appCtx) throws BeansException {
//		this.appCtx = appCtx;		
//	}
//
//
//	/**
//	 * {@inheritDoc}
//	 * @see org.springframework.context.ApplicationListener#onApplicationEvent(org.springframework.context.ApplicationEvent)
//	 */
//	@Override
//	public void onApplicationEvent(final ApplicationContextEvent event) {
//		if(event instanceof ContextRefreshedEvent) {
//			start();
//		} else if(event instanceof ContextStoppedEvent) {
//			stop();
//		}		
//	}

	

}
