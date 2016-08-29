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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Service;

import de.codecentric.boot.admin.registry.ApplicationRegistry;

/**
 * <p>Title: ClientEventListenerService</p>
 * <p>Description: Service to spin up a client event listener</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.admin.discovery.ClientEventListenerService</code></p>
 */
@Service
@EnableAutoConfiguration
public class ClientEventListenerService implements InitializingBean, ApplicationListener {
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	
	@Autowired
	private DiscoveryClient discoveryClient;

	@Autowired
	private ApplicationRegistry registry;
	
	private ClientEventListener eventListener = null;
	
	
	/**
	 * Creates a new ClientEventListenerService
	 */
	public ClientEventListenerService() {
		log.info("Created ClientEventListenerService");
	}
	
	
	@Override
	public void afterPropertiesSet() throws Exception {
		log.info("\n\t======================================================\n\tStarting ClientEventListenerService\n\tDiscoveryClient:{}\n\tAppRegistry:{}\n\t======================================================\n", discoveryClient, registry);
		eventListener = new ClientEventListener(discoveryClient, registry);
	}


	@Override
	public void onApplicationEvent(final ApplicationEvent event) {
		log.info("Application Event: {}", event);
		
	}

}
