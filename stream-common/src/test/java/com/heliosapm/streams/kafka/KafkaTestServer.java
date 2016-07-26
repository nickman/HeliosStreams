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

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.heliosapm.utils.config.ConfigurationHelper;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;

import kafka.utils.SystemTime$;

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
	public static final String DEFAULT_LOG_DIR = System.getProperty("java.io.tmpdir") + "/embedded/kafka/";
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
	
	
	/** The default embedded kafka listening URI */
	public static final String DEFAULT_LISTENER = "0:localhost:9092";
	
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** The embedded server  */
	protected final Properties configProperties = new Properties();
	/** The kafka configuration */
	protected KafkaConfig kafkaConfig = null;
	/** The embedded kafka server instance */
	protected KafkaServer kafkaServer = null;
	/** The up and running flag */
	protected final AtomicBoolean running = new AtomicBoolean(false);
	
	/**
	 * Creates a new KafkaTestServer
	 */
	public KafkaTestServer() {
		// TODO Auto-generated constructor stub
	}
	
	public void start() throws Exception {
		if(running.compareAndSet(false, true)) {
			try {
				configProperties.clear();
				configProperties.setProperty("log.dir", ConfigurationHelper.getSystemThenEnvProperty(CONFIG_LOG_DIR, DEFAULT_LOG_DIR));
				configProperties.setProperty("port", "" + ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_PORT, DEFAULT_PORT));
				configProperties.setProperty("enable.zookeeper", "" + ConfigurationHelper.getBooleanSystemThenEnvProperty(CONFIG_ZOOKEEP, DEFAULT_ZOOKEEP));
				configProperties.setProperty("zookeeper.connect", ConfigurationHelper.getSystemThenEnvProperty(CONFIG_ZOOKEEP_URI, DEFAULT_ZOOKEEP_URI));
				configProperties.setProperty("brokerid", "" + ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_BROKERID, DEFAULT_BROKERID));
				log.info("Embedded Kafka Broker Config: {}",  configProperties);
				kafkaConfig = new KafkaConfig(configProperties);
				kafkaServer = new KafkaServer(kafkaConfig, SystemTime$.MODULE$, null);
				kafkaServer.startup();				
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

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		KafkaTestServer kts = new KafkaTestServer();
		try {
			kts.start();
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(-1);
		}
 
	}

}
