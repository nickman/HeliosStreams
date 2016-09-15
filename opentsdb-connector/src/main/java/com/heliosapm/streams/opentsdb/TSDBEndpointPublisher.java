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
package com.heliosapm.streams.opentsdb;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.heliosapm.streams.common.naming.AgentName;
import com.heliosapm.streams.discovery.EndpointPublisher;
import com.heliosapm.utils.config.ConfigurationHelper;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.tsd.RpcPlugin;

/**
 * <p>Title: TSDBEndpointPublisher</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.opentsdb.TSDBEndpointPublisher</code></p>
 */

public class TSDBEndpointPublisher extends RpcPlugin {
	
	/** The template for the JMX service URL */
	public static final String DEFAULT_JMX_URLS  = "service:jmx:jmxmp://%s:4245";
	
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());

	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RpcPlugin#initialize(net.opentsdb.core.TSDB)
	 */
	@Override
	public void initialize(final TSDB tsdb) {
		log.info(">>>>> Initializing TSDBEndpointPublisher...");
		final Properties properties = new Properties();
		properties.putAll(tsdb.getConfig().getMap());
		final String zkConnect = properties.getProperty("tsd.storage.hbase.zk_quorum", "localhost:2181");
		log.info("ZK Connect: [{}]", zkConnect);
		System.setProperty("streamhub.discovery.zookeeper.connect", zkConnect);
		final String[] rpcPlugins = ConfigurationHelper.getArraySystemThenEnvProperty("tsd.rpc.plugins", new String[]{}, properties);
		Arrays.sort(rpcPlugins);
		if(Arrays.binarySearch(rpcPlugins, "com.heliosapm.opentsdb.jmx.JMXRPC") >= 0) {
			final String host = AgentName.getInstance().getHostName();
			String jmxmpUri = String.format(DEFAULT_JMX_URLS, host);
			Set<String> endpoints = new HashSet<String>();
			endpoints.add("jvm");
			endpoints.add("tsd");
			if(Arrays.binarySearch(rpcPlugins, "com.heliosapm.streams.opentsdb.KafkaRPC") >= 0) {
				endpoints.add("kafka-consumer");
			}
			if(Arrays.binarySearch(rpcPlugins, "com.heliosapm.streams.opentsdb.KafkaRTPublisher") >= 0) {
				endpoints.add("kafka-producer");
			}			
			EndpointPublisher.getInstance().register(jmxmpUri, endpoints.toArray(new String[endpoints.size()]));
		}
		log.info(">>>>> TSDBEndpointPublisher initialized.");
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RpcPlugin#shutdown()
	 */
	@Override
	public Deferred<Object> shutdown() {
		return Deferred.fromResult(null);
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RpcPlugin#version()
	 */
	@Override
	public String version() {
		return "2.1";
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RpcPlugin#collectStats(net.opentsdb.stats.StatsCollector)
	 */
	@Override
	public void collectStats(final StatsCollector collector) {

	}

}
