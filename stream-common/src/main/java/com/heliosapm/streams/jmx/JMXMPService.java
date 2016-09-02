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
package com.heliosapm.streams.jmx;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.management.remote.jmxmp.JMXMPConnectorServer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.actuate.endpoint.Endpoint;
import org.springframework.stereotype.Service;

import com.heliosapm.streams.common.naming.AgentName;
import com.heliosapm.streams.discovery.EndpointPublisher;
import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.jmx.JMXHelper;

/**
 * <p>Title: JMXMPService</p>
 * <p>Description: Activates a JMXMP connector service and optionally publishes
 * a jmx monitorable discovery endpoint to zookeeper.</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.jmx.JMXMPService</code></p>
 */

@Service
public class JMXMPService implements Endpoint<String> {
	
	/** The configuration key for the JMXMP URI */
	public static final String CONF_JMXMP_URI = "jmx.jmxmp.uri";
	/** The JMXMP URI default */
	public static final String DEFAULT_JMXMP_URI = "jmxmp://0.0.0.0:1421";
	/** The configuration key for disabling JMXMP */
	public static final String CONF_JMXMP_DISABLE = "jmx.jmxmp.disable";

	/** The configuration key for the JMXMP discovery endpoint categories */
	public static final String CONF_JMXMP_DISCOVERY = "jmx.jmxmp.discovery.advertised";
	/** The JMXMP discovery categories (always contains default value) */
	public static final String[] DEFAULT_JMXMP_DISCOVERY = {"jvm"};

	/** The configuration key for disabling JMXMP discovery endpoint publishing */
	public static final String CONF_JMXMP_DISCOVERY_DISABLE = "jmx.jmxmp.discovery.disabled";

	
	/** The configured JMXMP URI */
	protected final String jmxmpUri;
	/** The JMXMP Connector Server */
	protected final JMXMPConnectorServer server;
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** JMXMP server enabled flag */
	protected final boolean enabled;
	/** JMXMP discovery enabled flag */
	protected final boolean discoveryEnabled;
	/** The advertised monitoring endpoints */
	protected final Set<String> endpoints = new HashSet<String>(Arrays.asList(DEFAULT_JMXMP_DISCOVERY));
	
	/**
	 * Creates a new JMXMPService
	 */
	public JMXMPService() {
		enabled = !ConfigurationHelper.getBooleanSystemThenEnvProperty(CONF_JMXMP_DISABLE, false);
		discoveryEnabled = !ConfigurationHelper.getBooleanSystemThenEnvProperty(CONF_JMXMP_DISCOVERY_DISABLE, false);
		if(enabled) {
			final String jmxmp = ConfigurationHelper.getSystemThenEnvProperty("jmx.jmxmp.uri", "jmxmp://0.0.0.0:0");
			server = JMXHelper.fireUpJMXMPServer(jmxmp);
			jmxmpUri = "service:jmx:jmxmp://" + AgentName.getInstance().getHostName() + ":" + server.getAddress().getPort();
			if(server!=null) {
				log.info("\n\t######\n\tJMXMP Server enabled on [{}]\n\t######\n", jmxmpUri);
				if(discoveryEnabled) {
					final String[] endpointKeys = ConfigurationHelper.getArraySystemThenEnvProperty(CONF_JMXMP_DISCOVERY, DEFAULT_JMXMP_DISCOVERY);
					Collections.addAll(endpoints, endpointKeys);
					EndpointPublisher.getInstance().register(jmxmpUri, endpoints.toArray(new String[0]));
				}
			}
		} else {
			server = null;
			jmxmpUri = null;
			log.info("JMXMP Service Disabled");
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
		return enabled;
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

