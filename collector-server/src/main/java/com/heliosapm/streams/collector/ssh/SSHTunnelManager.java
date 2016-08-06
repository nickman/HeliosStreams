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
package com.heliosapm.streams.collector.ssh;

import java.io.InputStream;
import java.net.URL;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.lang.StringHelper;
import com.heliosapm.utils.url.URLHelper;

import ch.ethz.ssh2.LocalPortForwarder;

/**
 * <p>Title: SSHTunnelManager</p>
 * <p>Description: Deployment and management of SSH port tunnels</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.ssh.SSHTunnelManager</code></p>
 */

public class SSHTunnelManager implements SSHConnectionListener, SSHTunnelManagerMBean {
	/** The singleton instance */
	private static volatile SSHTunnelManager instance = null;
	/** The singleton instance ctor lock */
	private static final Object lock = new Object();
	
	/** The shareable json object mapper */
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	
	/** The config key for the URL locations to load the SSH configuration JSON resources */
	public static final String CONFIG_JSON_URLS = "ssh.configs";
	/** The default URL locations to load the SSH configuration JSON resources */
	public static final String[] DEFAULT_JSON_URLS = {};
	
	/** An empty connection array const */
	public static final SSHConnection[] EMPTY_CONN_ARR = {};
	
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** The JSON config sources */
	protected final Set<String> jsonConfigs = new LinkedHashSet<String>();
	/** Connected connections */
	protected final NonBlockingHashMap<String, SSHConnection> connectedConnections = new NonBlockingHashMap<String, SSHConnection>(); 
	/** Disconnected connections */
	protected final NonBlockingHashMap<String, SSHConnection> disconnectedConnections = new NonBlockingHashMap<String, SSHConnection>(); 
	/** A map of local port forwards  */
	protected final NonBlockingHashMap<String, LocalPortForwarder> portForwards = new NonBlockingHashMap<String, LocalPortForwarder>();
	
	/**
	 * Acquires and returns the SSHTunnelManager singleton instance
	 * @return the SSHTunnelManager singleton instance
	 */
	public static SSHTunnelManager getInstance() {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					instance = new SSHTunnelManager();					
				}
			}
		}
		return instance;
	}
	
	/**
	 * Creates a new SSHTunnelManager
	 */
	private SSHTunnelManager() {
		int connectionCount = 0;
		final String[] _jsonConfigs = ConfigurationHelper.getArraySystemThenEnvProperty(CONFIG_JSON_URLS, DEFAULT_JSON_URLS);
		if(_jsonConfigs.length > 0) {
			for(String s: _jsonConfigs) {
				try {
					final URL url = URLHelper.toURL(s);
					final JsonNode node = OBJECT_MAPPER.readTree(url);
					final SSHConnection[] connections = getConnections(node);
					connectionCount += connections.length;
				} catch (Exception ex) {
					log.warn("Failed to read SSH configs from URL [{}], cause: [{}]", s, ex.toString());
				}
			}
		}
		log.info("Loaded [{}] SSHConnections", connectionCount);
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.ssh.SSHTunnelManagerMBean#loadSSHConfigJson(java.net.URL)
	 */
	@Override
	public int loadSSHConfigJson(final URL jsonUrl) {
		if(jsonUrl==null) throw new IllegalArgumentException("The passed URL was null");
		try {
			final JsonNode node = OBJECT_MAPPER.readTree(jsonUrl);
			final SSHConnection[] connections = getConnections(node);
			return connections.length;
		} catch (Exception ex) {
			throw new RuntimeException("Failed to process SSH config JSON from [" + jsonUrl + "]", ex);
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.ssh.SSHTunnelManagerMBean#loadSSHConfigJson(java.lang.String)
	 */
	@Override
	public int loadSSHConfigJson(final String jsonUrl) {
		return loadSSHConfigJson(URLHelper.toURL(jsonUrl));
	}
	
	/**
	 * Returns the local port bound to the requested remote host and port
	 * @param connectHost The host to connect to
	 * @param connectPort The port on the connect host to connect to
	 * @return the local port to bind to if one wants to connect there
	 */
	public int getPortForward(final String connectHost, final int connectPort) {
		if(connectHost==null || connectHost.trim().isEmpty()) throw new IllegalArgumentException("The passed connect host was null or empty");
		if(connectPort < 1 || connectPort > 65535) throw new IllegalArgumentException("The requested port number [" + connectPort + "] is invalid");
		final String key = connectHost.trim() + ":" + connectPort;
		final LocalPortForwarder lpf = portForwards.get(key);
		if(lpf==null || lpf == LocalPortForwarder.PLACEHOLDER) throw new RuntimeException("No portforward established to [" + key + "]");
		return lpf.getLocalPort();
	}
	
	/**
	 * Creates a new portforward and returns the local port to bind to it on
	 * @param forwardingHost The host to connect through
	 * @param sshPort The ssh port on the forwarding host to connect through
	 * @param user The user to connect as
	 * @param connectHost The endpoint host to connect to
	 * @param connectPort The endpoint port to connect to
	 * @return the local port to bind to
	 */
	public int createPortForward(final String forwardingHost, final int sshPort, final String user, final String connectHost, final int connectPort) {
		if(connectHost==null || connectHost.trim().isEmpty()) throw new IllegalArgumentException("The passed connect host was null or empty");
		if(connectPort < 1 || connectPort > 65535) throw new IllegalArgumentException("The requested port number [" + connectPort + "] is invalid");
		
		final String lpfKey = connectHost.trim() + ":" + connectPort;
		LocalPortForwarder lpf = portForwards.putIfAbsent(lpfKey, LocalPortForwarder.PLACEHOLDER);
		if(lpf==null || lpf == LocalPortForwarder.PLACEHOLDER) {
			if(forwardingHost==null || forwardingHost.trim().isEmpty()) throw new IllegalArgumentException("The passed forwarding host was null or empty");
			if(sshPort < 1 || sshPort > 65535) throw new IllegalArgumentException("The requested forwarding ssh port number [" + sshPort + "] is invalid");
			if(user==null || user.trim().isEmpty()) throw new IllegalArgumentException("The passed forwarding host user was null or empty");
			final String forwardKey = String.format(SSHConnection.KEY_TEMPLATE, user, forwardingHost, sshPort);
			final SSHConnection conn = connectedConnections.get(forwardKey);
			if(conn==null) throw new IllegalStateException("No connected connection for forwarder [" + forwardKey + "]");
			lpf = conn.createPortForward(0, forwardingHost, sshPort);
			portForwards.replace(lpfKey, lpf);
		}
		return lpf.getLocalPort();		
	}
	
	/**
	 * Parses SSHConnections from the passed json node
	 * @param rootNode the node to read from
	 * @return an array of SSHConnections
	 */
	public static SSHConnection[] getConnections(final JsonNode rootNode) {
		try {
			final ArrayNode an = (ArrayNode)rootNode.get("connections");
			if(an.size()==0) return EMPTY_CONN_ARR;
			return OBJECT_MAPPER.convertValue(an, SSHConnection[].class);	
		} catch (Exception ex) {
			throw new RuntimeException("Failed to load SSHConnections", ex);
		}
	}
	
	/**
	 * Parses SSHConnections from the JSON read from the passed URL
	 * @param jsonUrl the URL the json is read from
	 * @return an array of SSHConnections
	 */
	public static SSHConnection[] parseConnections(final URL jsonUrl) {
		if(jsonUrl==null) throw new IllegalArgumentException("The passed URL was null");		
		final String jsonText = StringHelper.resolveTokens(
				URLHelper.getStrBuffFromURL(jsonUrl)
		);
		try {			
			final JsonNode rootNode = OBJECT_MAPPER.readTree(jsonText);
			final ArrayNode an = (ArrayNode)rootNode.get("connections");
			if(an.size()==0) return EMPTY_CONN_ARR;
			return OBJECT_MAPPER.convertValue(an, SSHConnection[].class);	
		} catch (Exception ex) {
			throw new RuntimeException("Failed to load SSHConnections from [" + jsonUrl + "]", ex);
		}		
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.ssh.SSHConnectionListener#onStarted(java.lang.String)
	 */
	@Override
	public void onStarted(final String connectionKey) {
		disconnectedConnections.put(connectionKey, SSHConnection.getConnection(connectionKey));
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.ssh.SSHConnectionListener#onStopped(java.lang.String)
	 */
	@Override
	public void onStopped(final String connectionKey) {
		connectedConnections.remove(connectionKey);
		disconnectedConnections.remove(connectionKey);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.ssh.SSHConnectionListener#onConnected(java.lang.String)
	 */
	@Override
	public void onConnected(final String connectionKey) {
		connectedConnections.put(connectionKey, SSHConnection.getConnection(connectionKey));
		disconnectedConnections.remove(connectionKey);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.ssh.SSHConnectionListener#onDisconnected(java.lang.String)
	 */
	@Override
	public void onDisconnected(final String connectionKey) {
		final SSHConnection conn = SSHConnection.getConnection(connectionKey);
		if(conn!=null) disconnectedConnections.put(connectionKey, SSHConnection.getConnection(connectionKey));
		connectedConnections.remove(connectionKey);		
	}

}
