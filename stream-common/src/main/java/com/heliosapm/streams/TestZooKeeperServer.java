/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.heliosapm.streams;

import java.io.File;
import java.io.IOException;

import javax.management.JMException;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.zookeeper.jmx.ManagedUtil;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

/**
 * <p>Title: TestZooKeeperServer</p>
 * <p>Description: A simple zookeep server</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.TestZooKeeperServer</code></p>
 */

public class TestZooKeeperServer {
	private ServerCnxnFactory cnxnFactory;
	private ZooKeeperServer zkServer;
	private final Logger LOG = LogManager.getLogger(TestZooKeeperServer.class);

	
	/**
	 * Returns the listening port
	 * @return the listening port
	 */
	public int getPort() {
		return cnxnFactory.getLocalPort();
	}
	
	
    /**
     * Run from a ServerConfig.
     * @param config ServerConfig to use.
     * @throws IOException on any error
     */
    public void runFromConfig(ServerConfig config) throws IOException {
    	LOG.info(">>>>> Starting Test ZooKeep Server...");
        FileTxnSnapLog txnLog = null;
        try {
            // Note that this thread isn't going to be doing anything else,
            // so rather than spawning another thread, we will just call
            // run() in this thread.
            // create a file logger url from the command line args
            zkServer = new ZooKeeperServer();

            txnLog = new FileTxnSnapLog(new File(config.getDataLogDir()), new File(
                    config.getDataDir()));
            zkServer.setTxnLogFactory(txnLog);
            zkServer.setTickTime(config.getTickTime());
            zkServer.setMinSessionTimeout(config.getMinSessionTimeout());
            zkServer.setMaxSessionTimeout(config.getMaxSessionTimeout());
            cnxnFactory = ServerCnxnFactory.createFactory();
            cnxnFactory.configure(config.getClientPortAddress(),
                    config.getMaxClientCnxns());
            cnxnFactory.startup(zkServer);
            LOG.info("<<<<< Test ZooKeep Server Started.");
//            cnxnFactory.join();
//            if (zkServer.isRunning()) {
//                zkServer.shutdown();
//            }            
        } catch (InterruptedException e) {
            // warn, but generally this is ok
            LOG.warn("Server interrupted", e);
        } finally {
//            if (txnLog != null) {
//                txnLog.close();
//            }
        }
    }

    /**
     * Shutdown the serving instance
     */
    protected void shutdown() {
    	LOG.info(">>>>> Stopping Test ZooKeep Server...");
    	try { zkServer.shutdown(); } catch (Exception x) {/* No Op */}
    	try { cnxnFactory.shutdown(); } catch (Exception x) {/* No Op */}
    	LOG.info("<<<<< Test ZooKeep Server Stopped.");
        
    }
	
	
}
