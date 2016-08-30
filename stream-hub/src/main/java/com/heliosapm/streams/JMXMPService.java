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
package com.heliosapm.streams;

import javax.management.remote.jmxmp.JMXMPConnectorServer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.actuate.endpoint.Endpoint;
import org.springframework.boot.context.embedded.ConfigurableEmbeddedServletContainer;
import org.springframework.boot.context.embedded.EmbeddedServletContainerCustomizer;
import org.springframework.stereotype.Service;

import com.heliosapm.streams.common.naming.AgentName;
import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.jmx.JMXHelper;

import org.springframework.boot.context.embedded.*;

/**
 * <p>Title: JMXMPService</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.JMXMPService</code></p>
 */

@Service
public class JMXMPService implements Endpoint<String> {

	protected final String jmxmpUri;
	protected final JMXMPConnectorServer server;
	protected final Logger log = LogManager.getLogger(getClass());
	
	/**
	 * Creates a new JMXMPService
	 */
	public JMXMPService() {
		final String jmxmp = ConfigurationHelper.getSystemThenEnvProperty("jmx.jmxmp.uri", "jmxmp://0.0.0.0:0");
		server = JMXHelper.fireUpJMXMPServer(jmxmp);
		jmxmpUri = "service:jmx:jmxmp://" + AgentName.getInstance().getHostName() + ":" + server.getAddress().getPort();
		if(server!=null) {
			log.info("\n\t######\n\tJMXMP Server enabled on [{}]\n\t######\n", jmxmpUri);
		}
	}
	

	/**
	 * {@inheritDoc}
	 * @see org.springframework.boot.actuate.endpoint.Endpoint#getId()
	 */
	@Override
	public String getId() {
		return "jmxmp";
	}

	/**
	 * {@inheritDoc}
	 * @see org.springframework.boot.actuate.endpoint.Endpoint#isEnabled()
	 */
	@Override
	public boolean isEnabled() {
		return true;
	}

	/**
	 * {@inheritDoc}
	 * @see org.springframework.boot.actuate.endpoint.Endpoint#isSensitive()
	 */
	@Override
	public boolean isSensitive() {
		return false;
	}

	/**
	 * {@inheritDoc}
	 * @see org.springframework.boot.actuate.endpoint.Endpoint#invoke()
	 */
	@Override
	public String invoke() {
		return "{\"jmx\" : \"" + jmxmpUri + "\"}";
	}


	

}
