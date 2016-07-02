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
package com.heliosapm.streams.admin.zookeep;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.heliosapm.utils.config.ConfigurationHelper;

/**
 * <p>Title: ZooKeepPublisher</p>
 * <p>Description: Publishes the Admin Server HTTP URL to Zookeeper</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.admin.zookeep.ZooKeepPublisher</code></p>
 */

public class ZooKeepPublisher implements Watcher {

	/** The config key for the name of the zookeeper node the admin server will publish its http URL into */
	public static final String CONFIG_NODE_NAME = "streamhub.config.zookeeper.admin.nodename";
	/** The default name of the zookeeper node the admin server will publish its http URL into */
	public static final String DEFAULT_NODE_NAME = "streamhub-admin-url";
	/** The config key for the zookeeper connect string */
	public static final String CONFIG_CONNECT = "streamhub.config.zookeeperconnect";
	/** The default zookeeper connect string */
	public static final String DEFAULT_CONNECT = "localhost:2181";
	/** The config key for the zookeeper chroot */
	public static final String CONFIG_CHROOT = "streamhub.config.zookeeper.chroot";
	/** The default zookeeper chroot */
	public static final String DEFAULT_CHROOT = "/streamhub";

	/** The config key for the zookeeper session timeout */
	public static final String CONFIG_TIMEOUT = "streamhub.config.zookeeper.timeout";
	/** The default zookeeper session timeout */
	public static final int DEFAULT_TIMEOUT = 60000;

	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** The configured connect string */	
	protected final String connect;
	/** The configured chroot string */
	protected final String chroot;
	/** The configured node name */
	protected final String nodeName;
	/** The session timeout in ms. */
	protected final int timeout;
	/** The zookeeper client instance */
	protected final ZooKeeper zk;
	
	/**
	 * Creates a new ZooKeepPublisher
	 * @throws IOException thrown if we can't create the zk
	 */
	public ZooKeepPublisher() throws IOException {
		connect = ConfigurationHelper.getSystemThenEnvProperty(CONFIG_CONNECT, DEFAULT_CONNECT);
		chroot = ConfigurationHelper.getSystemThenEnvProperty(CONFIG_CHROOT, DEFAULT_CHROOT);
		nodeName = ConfigurationHelper.getSystemThenEnvProperty(CONFIG_NODE_NAME, DEFAULT_NODE_NAME);
		timeout = ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_TIMEOUT, DEFAULT_TIMEOUT);
		zk = new ZooKeeper(connect + chroot, timeout, this);
	}
	
	public static void main(String[] args) {
		try {
			ZooKeepPublisher pub = new ZooKeepPublisher();
			pub.log.info("Created Zk");
			Thread.currentThread().join(60000);
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(-1);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
	 */
	@Override
	public void process(final WatchedEvent event) {
		log.info("ZooKeep Event: [{}]", event);		
	}

	/**
	 * Returns the connect string
	 * @return the connect
	 */
	public String getConnect() {
		return connect;
	}

	/**
	 * Returns the connect chroot
	 * @return the chroot
	 */
	public String getChroot() {
		return chroot;
	}

	/**
	 * Returns the binding node name
	 * @return the nodeName
	 */
	public String getNodeName() {
		return nodeName;
	}

	/**
	 * Returns the session timeout in ms.
	 * @return the timeout
	 */
	public int getTimeout() {
		return timeout;
	}

	/**
	 * Returns the zk instance
	 * @return the zk
	 */
	public ZooKeeper getZk() {
		return zk;
	}

}
