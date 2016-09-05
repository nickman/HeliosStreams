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

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ApplicationContextEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.ContextStoppedEvent;
import org.springframework.stereotype.Component;

import com.heliosapm.streams.discovery.AdvertisedEndpoint;
import com.heliosapm.streams.discovery.AdvertisedEndpointListener;
import com.heliosapm.streams.discovery.EndpointListener;

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
	
	public EndpointDiscoveryService() {
		log.info("Created EndpointDiscoveryService");
	}
	
	/**
	 * Starts the listener
	 */
	protected void start() {
		if(started.compareAndSet(false, true)) {
			log.info(">>>>> Starting EndpointDiscoveryService...");
			endpointListener = EndpointListener.getInstance();
			endpointListener.addEndpointListener(this);			
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
			
		} catch (Exception ex) {
			log.error("Failed to activate monitoring for endpoint [{}]", endpoint, ex);
		}
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
