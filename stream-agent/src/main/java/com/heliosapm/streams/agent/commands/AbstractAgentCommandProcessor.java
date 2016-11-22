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
package com.heliosapm.streams.agent.commands;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.cliffc.high_scale_lib.NonBlockingHashMap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.heliosapm.streams.agent.endpoint.Endpoint;
import com.heliosapm.streams.agent.publisher.AgentCommandProcessor;
import com.heliosapm.streams.agent.services.EndpointPublisher;
import com.heliosapm.utils.jmx.JMXHelper;

/**
 * <p>Title: AbstractAgentCommandProcessor</p>
 * <p>Description: Base class for {@link AgentCommandProcessor} implementations</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.agent.commands.AbstractAgentCommandProcessor</code></p>
 */

public abstract class AbstractAgentCommandProcessor implements AgentCommandProcessor {
	/** A map of advertised endpoints keyed by the JMX Url the endpoints are accessed by */
	protected static final NonBlockingHashMap<String, Endpoint[]> endpoints = new NonBlockingHashMap<String, Endpoint[]>();
	/** Placeholder for endpoints update */
	private static final Endpoint[] PLACEHOLDER = {Endpoint.fromString(AbstractAgentCommandProcessor.class.getName())};
	/** Shared ObjectMapper instance */
	private static final ObjectMapper jsonMapper = new ObjectMapper();
	
	/**
	 * Updates the JMXMP installed URLs system and agent properties
	 */
	protected static synchronized void updateJMXMPSystemProps() {		
		final Set<String> endpointUrls = new HashSet<String>(endpoints.keySet());
		if(endpointUrls.isEmpty()) return;
		final StringBuilder urls = new StringBuilder();
		for(String s: endpointUrls) {
			if(s==null || s.trim().isEmpty()) continue;
			urls.append(s.trim()).append(",");
		}
		if(urls.length()==0) return;
		urls.deleteCharAt(urls.length()-1);
		final String jmxUrls = urls.toString();
		System.setProperty(JMXMP_CONNECTORS_PROP, jmxUrls);
		JMXHelper.getAgentProperties().setProperty(JMXMP_CONNECTORS_PROP, jmxUrls);
		try {
			final String ends = jsonMapper.writeValueAsString(endpoints);
			System.setProperty(JMXMP_ENDPOINTS_PROP, ends);
			JMXHelper.getAgentProperties().setProperty(JMXMP_ENDPOINTS_PROP, ends);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		for(Map.Entry<String, Endpoint[]> entry: endpoints.entrySet()) {
			EndpointPublisher.getInstance().register(entry.getKey(), entry.getValue());
		}
	}
	
	/**
	 * Returns a set of installed JMXMP connector URLs
	 * @return a set of installed JMXMP connector URLs
	 */
	protected static Set<String> getConnectorURLs() {
		return new HashSet<String>(endpoints.keySet());
	}
	
	/**
	 * Returns a map of installed endpoints keyed by the JMXMP connector URL
	 * @return a map of installed endpoints keyed by the JMXMP connector URL
	 */
	protected static Map<String, Endpoint[]> getEndpoints() {
		return new HashMap<String, Endpoint[]>(endpoints);
	}
	
	
	/**
	 * Adds an array of endpoint names for the passed JMX url
	 * @param jmxUrl The JMX url through which access to the endpoints is enabled 
	 * @param endpointNames The advertised endpoint names
	 */
	protected static void addEndpoints(final String jmxUrl, final Endpoint...epoints) {
		if(jmxUrl==null || jmxUrl.trim().isEmpty()) throw new IllegalArgumentException("The passed JMX URL was null or empty");
		final String key = jmxUrl.trim();
		if(endpoints==null || epoints.length==0) return;
		Endpoint[] endps = endpoints.putIfAbsent(key, PLACEHOLDER);
		if(endps!=null) {
			if(PLACEHOLDER==endps) {
				endpoints.replace(key, epoints);
			} else {
				final Set<Endpoint> joined = new LinkedHashSet<Endpoint>(Arrays.asList(endps));
				Collections.addAll(joined, epoints);
				endpoints.replace(key, joined.toArray(new Endpoint[joined.size()]));				
			}
			updateJMXMPSystemProps();
		}
	}
	
	/**
	 * Sets the specified property in system and agent properties
	 * @param key The property key
	 * @param value The property value
	 */
	protected static void setProperty(final String key, final String value) {
		System.setProperty(key, value);
		JMXHelper.getAgentProperties().setProperty(key, value);		
	}
	
	/**
	 * Retrieves the named property value from the agent and if null, then system properties.
	 * @param key The property key
	 * @param defaultValue The default value if both sources return null
	 * @param setIfNull If both sources return null, set the default value in both if this is true
	 * @return the named property value or the default value if not found
	 */
	protected static String getProperty(final String key, final String defaultValue, final boolean setIfNull) {
		String value = JMXHelper.getAgentProperties().getProperty(key);
		if(value==null) {
			value = System.getProperty(key);
			if(value==null && setIfNull) {
				setProperty(key, defaultValue);
			}
		}
		return value==null ? defaultValue : value;
	}
	
	/**
	 * Retrieves the named property value from the agent and if null, then system properties.
	 * @param key The property key
	 * @param defaultValue The default value if both sources return null
	 * @return the named property value or the default value if not found
	 */
	protected static String getProperty(final String key, final String defaultValue) {
		return getProperty(key, defaultValue, false);
	}
	
	
}
