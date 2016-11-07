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
package com.heliosapm.streams.opentsdb.websock;

import java.io.IOException;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.heliosapm.streams.opentsdb.plugin.PluginMetricManager;
import com.heliosapm.webrpc.jsonservice.services.SystemJSONServices;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.tsd.HttpRpcPlugin;
import net.opentsdb.tsd.HttpRpcPluginQuery;
import net.opentsdb.tsd.TSDBJSONService;
import net.opentsdb.utils.Config;

/**
 * <p>Title: WebSocketRPC</p>
 * <p>Description: Provides access to the OpenTSDB server API via websockets</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.opentsdb.websock.WebSocketRPC</code></p>
 */

public class WebSocketRPC extends HttpRpcPlugin {
	/** The config key name for the websocket rpc path */
	public static final String CONFIG_RPC_PATH = "websock.rpc.path";
	/** The default websocket rpc path */
	public static final String DEFAULT_RPC_PATH = "/ws";

	
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** The TSDB instance */
	protected TSDB tsdb = null;
	/** The metric manager for this plugin */
	protected final PluginMetricManager metricManager = new PluginMetricManager(getClass().getSimpleName());
	/** The configured path for thw websocket rpc service */
	protected String path = null;
	
	
	
	/**
	 * Creates a new WebSocketRPC
	 */
	public WebSocketRPC() {
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.HttpRpcPlugin#initialize(net.opentsdb.core.TSDB)
	 */
	@Override
	public void initialize(final TSDB tsdb) {
		log.info(">>>>> Initializing WebSocketRPC service....");
		this.tsdb = tsdb;
		final Properties properties = new Properties();
		final Config cfg = tsdb.getConfig();
		properties.putAll(cfg.getMap());		
		path = metricManager.getAndSetConfig(CONFIG_RPC_PATH, DEFAULT_RPC_PATH, properties, cfg);
		JSONRequestRouter.getInstance().registerJSONService(new SystemJSONServices());
		JSONRequestRouter.getInstance().registerJSONService(new TSDBJSONService(tsdb));

		log.info("<<<<< WebSocketRPC service Initialized.");
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.HttpRpcPlugin#shutdown()
	 */
	@Override
	public Deferred<Object> shutdown() {
		log.info(">>>>> Stopping WebSocketRPC service....");

		log.info("<<<<< WebSocketRPC service Stopped.");
		return Deferred.fromResult(null);
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.HttpRpcPlugin#version()
	 */
	@Override
	public String version() {
		return "2.2";
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.HttpRpcPlugin#collectStats(net.opentsdb.stats.StatsCollector)
	 */
	@Override
	public void collectStats(final StatsCollector collector) {
		metricManager.collectStats(collector);

	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.HttpRpcPlugin#getPath()
	 */
	@Override
	public String getPath() {
		return path;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.HttpRpcPlugin#execute(net.opentsdb.core.TSDB, net.opentsdb.tsd.HttpRpcPluginQuery)
	 */
	@Override
	public void execute(final TSDB tsdb, final HttpRpcPluginQuery query) throws IOException {


	}

}
