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
package com.heliosapm.streams.kafka;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;

import com.heliosapm.utils.concurrency.ExtendedThreadManager;
import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.reflect.PrivateAccessor;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.SystemTime$;
import kafka.utils.ZKStringSerializer$;
import kafka.admin.RackAwareMode$;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;

/**
 * <p>Title: KafkaTestServer</p>
 * <p>Description: An embedded kafka server for testing</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.kafka.KafkaTestServer</code></p>
 */

public class KafkaTestServer {
	/** The config key for the embedded kafka log directory */
	public static final String CONFIG_LOG_DIR = "test.kafka.logdir";	
	/** The default embedded kafka log directory */
	public static final String DEFAULT_LOG_DIR = new File(new File(System.getProperty("java.io.tmpdir")) + "/embedded/kafka").getAbsolutePath(); 
	/** The config key for the embedded kafka listening port */
	public static final String CONFIG_PORT = "test.kafka.port";
	/** The default embedded kafka listening port */
	public static final int DEFAULT_PORT = 9092;
	/** The config key for the embedded kafka broker id */
	public static final String CONFIG_BROKERID = "test.kafka.brokerid";
	/** The default embedded kafka broker id */
	public static final int DEFAULT_BROKERID = 1;	
	/** The config key for the embedded kafka zookeeper enablement */
	public static final String CONFIG_ZOOKEEP = "test.kafka.zookeep.enabled";
	/** The default embedded kafka zookeeper enablement */
	public static final boolean DEFAULT_ZOOKEEP = true;
	/** The config key for the embedded kafka zookeeper connect uri */
	public static final String CONFIG_ZOOKEEP_URI = "test.kafka.zookeep.uri";
	/** The default embedded kafka zookeeper connect uri */
	public static final String DEFAULT_ZOOKEEP_URI = "localhost:2181";
	
	
	/** The prefix for zookeeper config properties */
	public static final String ZK_PREFIX = "test.kafka.zk.";

	/** The config key for the embedded zookeeper data directory */
	public static final String CONFIG_ZK_DATA_DIR = ZK_PREFIX  + "dataDir";	
	/** The default embedded zookeeper data directory */
	public static final String DEFAULT_ZK_DATA_DIR = new File(new File(System.getProperty("java.io.tmpdir")) + "/embedded/zookeeper/data").getAbsolutePath();
	/** The config key for the embedded zookeeper log directory */
	public static final String CONFIG_ZK_LOG_DIR = ZK_PREFIX  + "dataLogDir";	
	/** The default embedded zookeeper log directory */
	public static final String DEFAULT_ZK_LOG_DIR = new File(new File(System.getProperty("java.io.tmpdir")) + "/embedded/zookeeper/log").getAbsolutePath();	
	/** The config key for the embedded zookeeper listening port */
	public static final String CONFIG_ZK_PORT = ZK_PREFIX  + "clientPort";	
	/** The default embedded zookeeper listening port */
	public static final int DEFAULT_ZK_PORT = 2181;	
	/** The config key for the embedded zookeeper listener binding interface */
	public static final String CONFIG_ZK_IFACE = ZK_PREFIX  + "clientPortAddress";	
	/** The default embedded zookeeper listener binding interface */
	public static final String DEFAULT_ZK_IFACE = "0.0.0.0";
	/** The config key for the embedded zookeeper maximum number of client connections */
	public static final String CONFIG_ZK_MAXCONNS = ZK_PREFIX  + "maxClientCnxns";	
	/** The default embedded zookeeper maximum number of client connections */
	public static final int DEFAULT_ZK_MAXCONNS = 50;
	/** The config key for the embedded zookeeper minimum session timeout */
	public static final String CONFIG_ZK_MINTO = ZK_PREFIX  + "minSessionTimeout";	
	/** The default embedded zookeeper minimum session timeout */
	public static final int DEFAULT_ZK_MINTO = -1;
	/** The config key for the embedded zookeeper maximum session timeout */
	public static final String CONFIG_ZK_MAXTO = ZK_PREFIX  + "maxSessionTimeout";	
	/** The default embedded zookeeper maximum session timeout */
	public static final int DEFAULT_ZK_MAXTO = -1;
	
	
	/** The default embedded kafka listening URI */
	public static final String DEFAULT_LISTENER = "0:localhost:9092";
	
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** The embedded server config properties  */
	protected final Properties configProperties = new Properties();
	/** The kafka configuration */
	protected KafkaConfig kafkaConfig = null;
	/** The embedded kafka server instance */
	protected KafkaServer kafkaServer = null;
	/** The up and running flag */
	protected final AtomicBoolean running = new AtomicBoolean(false);
	
	/** The embedded zookeeper config properties  */
	protected final Properties zkConfigProperties = new Properties();
	/** The embedded zookeeper configurator */
	protected QuorumPeerConfig zkConfig = null;
	/** The embedded zookeeper server */
	protected QuorumPeerMain zkServer = null;
	/** The standalone zookeeper config */
	protected ServerConfig sc = null;
	/** The standalone zookeeper server */
	protected ZooKeeperServerMain zkSoServer = null;
	/** The standalone flag */
	protected final AtomicBoolean standalone = new AtomicBoolean(true);
	/** The zookeep connect string as derrived from the zookeep configuration */
	protected String zookeepConnect = "localhost:2181";
	/** A Zookeeper util client for admin ops */
	protected volatile ZkUtils zkUtils = null;
	/** A zookeeper client for admin ops */
	protected ZkClient zkClient  = null;
	/** A zookeeper connection for admin ops */
	protected ZkConnection zkConnection = null;
	
	
	/**
	 * Creates a new KafkaTestServer
	 */
	public KafkaTestServer() {
		 System.setProperty("zookeeper.jmx.log4j.disable", "true");
	}
	
	/**
	 * Starts the test server
	 * @throws Exception thrown on any error
	 */
	public void start() throws Exception {
		if(running.compareAndSet(false, true)) {
			try {
				zkConfigProperties.clear();
				zkConfigProperties.setProperty("tickTime", "2000");
				zkConfigProperties.setProperty("syncEnabled", "false");
				zkConfigProperties.setProperty("dataDir", ConfigurationHelper.getSystemThenEnvProperty(CONFIG_ZK_DATA_DIR, DEFAULT_ZK_DATA_DIR));
				zkConfigProperties.setProperty("dataLogDir", ConfigurationHelper.getSystemThenEnvProperty(CONFIG_ZK_LOG_DIR, DEFAULT_ZK_LOG_DIR));
				final int clientPort = ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_ZK_PORT, DEFAULT_ZK_PORT);
				final String clientPortAddress = ConfigurationHelper.getSystemThenEnvProperty(CONFIG_ZK_IFACE, DEFAULT_ZK_IFACE);				
				zookeepConnect = clientPortAddress + ":" + clientPort;
				
				zkConfigProperties.setProperty("clientPort", "" + clientPort);
				zkConfigProperties.setProperty("clientPortAddress", clientPortAddress);
				zkConfigProperties.setProperty("maxClientCnxns", "" + ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_ZK_MAXCONNS, DEFAULT_ZK_MAXCONNS));
				zkConfigProperties.setProperty("minSessionTimeout", "" + ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_ZK_MINTO, DEFAULT_ZK_MINTO));
				zkConfigProperties.setProperty("maxSessionTimeout", "" + ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_ZK_MAXTO, DEFAULT_ZK_MAXTO));
//				zkConfigProperties.setProperty("server.0", "PP-DT-NWHI-01:" + clientPort + ":" + (clientPort+1)); //  + ":PARTICIPANT");
				configProperties.clear();
				
				configProperties.setProperty("log.dir", ConfigurationHelper.getSystemThenEnvProperty(CONFIG_LOG_DIR, DEFAULT_LOG_DIR));
				configProperties.setProperty("port", "" + ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_PORT, DEFAULT_PORT));
				configProperties.setProperty("enable.zookeeper", "" + ConfigurationHelper.getBooleanSystemThenEnvProperty(CONFIG_ZOOKEEP, DEFAULT_ZOOKEEP));
				configProperties.setProperty("zookeeper.connect", zookeepConnect);
				configProperties.setProperty("brokerid", "" + ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_BROKERID, DEFAULT_BROKERID));
				log.info("Embedded Kafka ZooKeeper Config: {}",  zkConfigProperties);
				log.info("Embedded Kafka Broker Config: {}",  configProperties);
				log.info(">>>>> Starting Embedded ZooKeeper...");
				zkConfig = new QuorumPeerConfig();
//				zkConfig.parse(System.getenv("ZOOKEEPER_HOME") + File.separator + "conf" + File.separator + "zoo.cfg");
				zkConfig.parseProperties(zkConfigProperties);
				final Thread zkRunThread;
				final Throwable[] t = new Throwable[1];
				if(zkConfig.getServers().size() > 1) {
					standalone.set(false);
					zkServer = new QuorumPeerMain();
					zkRunThread = new Thread("ZooKeeperRunThread") {
						public void run() {
							try {
								zkServer.runFromConfig(zkConfig);
							} catch (IOException ex) {
								log.error("Failed to start ZooKeeper", ex);
								t[0] = ex;
							}
						}
					};
				} else {
					standalone.set(true);
					sc = new ServerConfig();
					sc.readFrom(zkConfig);
					zkSoServer = new ZooKeeperServerMain();					
					zkRunThread = new Thread("ZooKeeperStandaloneRunThread") {
						public void run() {
							try {
								zkSoServer.runFromConfig(sc);
							} catch (IOException ex) {
								log.error("Failed to start standalone ZooKeeper", ex);
								t[0] = ex;
							}
						}
					};
				}
				zkRunThread.setDaemon(true);
				zkRunThread.start();
				
				
//				ZooKeeperServer zkServer  = new ZooKeeperServer(new File(ConfigurationHelper.getSystemThenEnvProperty(CONFIG_ZK_DATA_DIR, DEFAULT_ZK_DATA_DIR)), new File(ConfigurationHelper.getSystemThenEnvProperty(CONFIG_ZK_LOG_DIR, DEFAULT_ZK_LOG_DIR)), 200);
				
				log.info("<<<<< Embedded ZooKeeper started.");
				log.info(">>>>> Starting Embedded Kafka...");
				kafkaConfig = new KafkaConfig(configProperties);
				kafkaServer = new KafkaServer(kafkaConfig, SystemTime$.MODULE$, null);
				kafkaServer.startup();				
				log.info("<<<<< Embedded Kafka started.");
			} catch (Exception ex) {
				running.set(false);
				configProperties.clear();
				kafkaConfig = null;
				try { kafkaServer.shutdown(); } catch (Exception x) {/* No Op */}
				kafkaServer = null;
				log.error("Failed to start embedded kafka server", ex);
				throw ex;
			}
		} else {
			log.warn("Embedded Kafka Broker already running");
		}
	}
	

	private ZkUtils getZkUtils() {
		if(!running.get()) throw new IllegalStateException("The KafkaTestServer is not running");
		if(zkUtils==null) {
			synchronized(this) {
				if(zkUtils==null) {
					zkClient = new ZkClient(
						zookeepConnect,
					    5000,
					    5000,
					    ZKStringSerializer$.MODULE$);
					zkConnection = new ZkConnection(zookeepConnect);
					zkUtils = new ZkUtils(zkClient, zkConnection, false);					
				}
			}
		}
		return zkUtils;
	}
	
	/**
	 * Stops the server
	 */
	public void stop() {
		if(running.compareAndSet(true, false)) {
			kafkaServer.shutdown();
			kafkaServer = null;
			kafkaConfig = null;
			configProperties.clear();
			if(zkUtils!=null) {
				try { zkUtils.close(); } catch (Exception x) {}
				zkUtils = null;
				try { zkConnection.close(); } catch (Exception x) {}
				zkConnection = null;
				try { zkClient.close(); } catch (Exception x) {}
				zkClient = null;				
			}			
			if(standalone.get()) {
				PrivateAccessor.invoke(zkSoServer, "shutdown");
			} else {
				PrivateAccessor.invoke(zkServer, "shutdown");
			}
			zkSoServer = null;
			zkServer = null;
			sc = null;
			zkConfig = null;
			zkConfigProperties.clear();
		} else {
			log.warn("Embedded Kafka Broker is not running");
		}
	}
	
	public void createTopic(final String topicName, final int partitionCount, final int replicaCount, final Properties topicProperties) {
		if(!running.get()) throw new IllegalStateException("The KafkaTestServer is not running");
		final ZkUtils z = getZkUtils();
		AdminUtils.createTopic(zkUtils, topicName, partitionCount, replicaCount, topicProperties==null ? new Properties() : topicProperties, new RackAwareMode.Disabled$());
		// kafka.common.TopicExistsException: Topic "xxx" already exists.
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.setProperty("java.net.preferIPv4Stack", "true");
		JMXHelper.fireUpJMXMPServer(3339);
		ExtendedThreadManager.install();
		final KafkaTestServer kts = new KafkaTestServer();
		try {
			kts.start();
			kts.log.info("Creating topic [{}]",  "xxx");
			kts.createTopic("xxx", 5, 1, null);
			kts.log.info("Topic created [{}]",  "xxx");
			StdInCommandHandler.getInstance().registerCommand("shutdown", new Runnable(){
				public void run() {
					if(kts.running.get()) {
						kts.stop();
					}
					System.exit(-1);
				}
			}).run();
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(-1);
		}
 
	}

}


//import org.I0Itec.zkclient.ZkClient;
//import org.I0Itec.zkclient.ZkConnection;
//
//import java.util.Properties;
//
//import kafka.admin.AdminUtils;
//import kafka.utils.ZKStringSerializer$;
//import kafka.utils.ZkUtils;
//
//public class KafkaJavaExample {
//
//  public static void main(String[] args) {
//    String zookeeperConnect = "zkserver1:2181,zkserver2:2181";
//    int sessionTimeoutMs = 10 * 1000;
//    int connectionTimeoutMs = 8 * 1000;
//    // Note: You must initialize the ZkClient with ZKStringSerializer.  If you don't, then
//    // createTopic() will only seem to work (it will return without error).  The topic will exist in
//    // only ZooKeeper and will be returned when listing topics, but Kafka itself does not create the
//    // topic.
//    ZkClient zkClient = new ZkClient(
//        zookeeperConnect,
//        sessionTimeoutMs,
//        connectionTimeoutMs,
//        ZKStringSerializer$.MODULE$);
//
//    // Security for Kafka was added in Kafka 0.9.0.0
//    boolean isSecureKafkaCluster = false;
//    ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), isSecureKafkaCluster);
//
//    String topic = "my-topic";
//    int partitions = 2;
//    int replication = 3;
//    Properties topicConfig = new Properties(); // add per-topic configurations settings here
//    AdminUtils.createTopic(zkUtils, topic, partitions, replication, topicConfig);
//    zkClient.close();
//  }
//
//}

