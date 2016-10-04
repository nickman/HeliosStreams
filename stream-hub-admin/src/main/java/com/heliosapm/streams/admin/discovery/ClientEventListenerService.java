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
package com.heliosapm.streams.admin.discovery;

import java.net.URL;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.heliosapm.streams.json.JSONOps;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.url.URLHelper;
import com.sun.jdmk.remote.cascading.CascadingService;

import de.codecentric.boot.admin.event.ClientApplicationDeregisteredEvent;
import de.codecentric.boot.admin.event.ClientApplicationEvent;
import de.codecentric.boot.admin.event.ClientApplicationRegisteredEvent;
import de.codecentric.boot.admin.event.ClientApplicationStatusChangedEvent;
import de.codecentric.boot.admin.registry.ApplicationRegistry;

/**
 * <p>Title: ClientEventListenerService</p>
 * <p>Description: Service to spin up a client event listener</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.admin.discovery.ClientEventListenerService</code></p>
 */
//@Service
//@EnableAutoConfiguration
public class ClientEventListenerService implements InitializingBean {
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** The JMX cascade mount point ids */
	protected final ConcurrentHashMap<String, String> mounts = new ConcurrentHashMap<String, String>();
	
	@Autowired
	private DiscoveryClient discoveryClient;

	@Autowired
	private ApplicationRegistry registry;
	
		
	
	/**
	 * Creates a new ClientEventListenerService
	 */
	public ClientEventListenerService() {
		log.info("Created ClientEventListenerService");
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() throws Exception {
		log.info("\n\t======================================================\n\tStarting ClientEventListenerService\n\tDiscoveryClient:{}\n\tAppRegistry:{}\n\t======================================================\n", discoveryClient, registry);
	}


	/**
	 * Listens on new application registration events
	 * @param event The client registration event
	 */
	@EventListener
	public void onClientApplicationRegistered(final ClientApplicationRegisteredEvent event) {
		log.info("Client Registration", event);
//		registerCascade(event);
	}
	
	protected void registerCascade(final ClientApplicationEvent event) {
//		final String managementUrl = event.getApplication().getManagementUrl();
//		final URL manUrl = URLHelper.toURL(managementUrl);
//		final String jmxUrlJson = URLHelper.getTextFromURL(managementUrl + "/jmxmp");
//		final JsonNode node = JSONOps.parseToNode(jmxUrlJson);
//		final String jmxUrl = node.get("jmx").asText();
//		final String key = event.getApplication().getName() + "/" + manUrl.getHost() + "-" + manUrl.getPort();
//		final String mountId = JMXHelper.invoke(CascadingService.CASCADING_SERVICE_DEFAULT_NAME, "mount", new Object[]{
//			jmxUrl, "*:*", key
//				
//		}, new String[]{String.class.getName(), String.class.getName(), String.class.getName()}).toString();
//		mounts.put(key, mountId);
	}
	
	/**
	 * Handles a client status change event
	 * @param event the client status change event
	 */
	@EventListener
	public void onClientApplicationStatusChange(final ClientApplicationStatusChangedEvent event) {
		final String newStatus = event.getTo().getStatus();
		if("UP".equals(newStatus)) {
//			registerCascade(event);
		}
	}

	/**
	 * Handles a client deregistration event
	 * @param event the client deregistration event
	 */
	@EventListener
	public void onClientApplicationDeregistered(final ClientApplicationDeregisteredEvent event) {
//		final URL manUrl = URLHelper.toURL(event.getApplication().getManagementUrl());
//		final String key = manUrl.getHost() + "-" + manUrl.getPort() + "/" + event.getApplication().getName();
//		final String mountId = mounts.remove(key);
//		if(mountId!=null) {
//			JMXHelper.invoke(CascadingService.CASCADING_SERVICE_DEFAULT_NAME, "unmount", new Object[]{mountId}, new String[]{String.class.getName()});
//		}
	}

}
