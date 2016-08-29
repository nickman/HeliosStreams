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
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.cloud.client.discovery.event.InstanceRegisteredEvent;

import de.codecentric.boot.admin.discovery.ApplicationDiscoveryListener;
import de.codecentric.boot.admin.registry.ApplicationRegistry;

/**
 * <p>Title: ClientEventListener</p>
 * <p>Description: Listens on and responds to client node events</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.admin.discovery.ClientEventListener</code></p>
 */

public class ClientEventListener extends ApplicationDiscoveryListener {
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());

	/**
	 * Creates a new ClientEventListener
	 * @param discoveryClient The discovery client
	 * @param registry The application registry
	 */
	public ClientEventListener(final DiscoveryClient discoveryClient, final ApplicationRegistry registry) {
		super(discoveryClient, registry);
	}
	
	@Override
	public void onInstanceRegistered(final InstanceRegisteredEvent<?> event) {
		super.onInstanceRegistered(event);
		final StringBuilder b = new StringBuilder("\n\t####################\n\tNode registration");		
		b.append("\n\tSource:").append(event.getSource());
		b.append("\n\tConfig:").append(event.getConfig());
		b.append("\n\t####################\n");
		log.info(b.toString());
	}

}
