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
package com.heliosapm.streams.hub.base;

import java.io.Closeable;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Ignore;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import com.heliosapm.streams.hub.BaseTest;
import com.heliosapm.streams.kafka.KafkaTestServer;
import com.heliosapm.streams.kafka.TopicDefinition;
import com.heliosapm.utils.collections.Props;
import com.heliosapm.utils.lang.StringHelper;
import com.heliosapm.utils.url.URLHelper;

/**
 * <p>Title: BaseHubStreamTest</p>
 * <p>Description: Base hub stream test</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.hub.base.BaseHubStreamTest</code></p>
 */
@Ignore
public class BaseHubStreamTest extends BaseTest {
	
	/** Static class logger */
	protected static final Logger log = LogManager.getLogger(BaseHubStreamTest.class);
	/** The current kafka test server instance */
	protected static KafkaTestServer KTS = null; 
	/** The zookeeper port */
	protected static int zooKeepPort = -1; 
	/** The kafka port */
	protected static int kafkaPort = -1;
	/** The current app context instance */
	protected static ApplicationContext APPCTX = null; 
	
	/** The system prop where the zookeeper port is published */
	public static final String ZOOKEEP_PORT_PROP = "test.zookeeper.port";
	/** The system prop where the kafka port is published */
	public static final String KAFKA_PORT_PROP = "test.kafka.port";
	/** The system prop where the zookeeper connect is published */
	public static final String ZOOKEEP_CONNECT_PROP = "test.zookeeper.connect";
	/** The system prop where the kafka connect is published */
	public static final String KAFKA_CONNECT_PROP = "test.kafka.connect";
	
	private static final List<Closeable> closeables = new CopyOnWriteArrayList<Closeable>();
	
	/**
	 * Starts a test kafka server
	 * @param topicConfigName The topic configuration name
	 * @return an array of topic definitions for the created topics
	 */	
	public static TopicDefinition[] startKafkaServer(final String topicConfigName) {
		if(KTS!=null) {
			stopKafkaServer();
		}
		KTS = new KafkaTestServer();
		try {
			KTS.start();
			zooKeepPort = KTS.getZooKeeperPort(); 
			kafkaPort = KTS.getKafkaPort(); 
			System.setProperty(ZOOKEEP_PORT_PROP, "" + zooKeepPort);
			System.setProperty(ZOOKEEP_CONNECT_PROP, "localhost:" + zooKeepPort);
			System.setProperty(KAFKA_PORT_PROP, "" + kafkaPort);
			System.setProperty(KAFKA_CONNECT_PROP, "localhost:" + kafkaPort);
			final TopicDefinition[] tds = loadTopicConfig(KTS, topicConfigName);
//			startAppContext(topicConfigName);
			return tds;
		} catch (Exception ex) {
			try { KTS.stop(); } catch (Exception x) {/* No Op */}
			throw new RuntimeException(ex);
		}
	}
	
	/**
	 * Stops the test kafka server
	 */
	@AfterClass
	public static void stopKafkaServer() {
		for(Closeable c: closeables) {
			try { c.close(); } catch (Exception x) {/* No Op */}
		}
		closeables.clear();
//		stopAppContext();
		if(KTS!=null) {
			try { KTS.stop(); } catch (Exception x) {/* No Op */}			
		}
		KTS = null;
		zooKeepPort = -1;
		kafkaPort = -1;
		System.clearProperty(ZOOKEEP_PORT_PROP);
		System.clearProperty(KAFKA_PORT_PROP);
	}
	
	
	/**
	 * Loads the specified topic configuration and loads it into the passed kafka server
	 * @param kts The kafka test server
	 * @param topicConfigName The name of the topic configuration
	 * @return an array of topic definitions for the created topics
	 */
	protected static TopicDefinition[] loadTopicConfig(final KafkaTestServer kts, final String topicConfigName) {
		final TopicDefinition[] topicDefs = TopicDefinition.topics(BaseHubStreamTest.class.getClassLoader().getResourceAsStream("tests/" + topicConfigName + "/topics.json"));
		kts.createTopics(topicDefs);
		for(TopicDefinition td: topicDefs) {
			Assert.assertTrue("Topic [" + td.getTopicName() + "] was not found", kts.topicExists(td.getTopicName()));
		}
		return topicDefs;
	}
	
	
	/**
	 * Starts the named spring application context
	 * @param appContextName the spring config to load 
	 */
	protected static void startAppContext(final String appContextName) {
		final Resource configResource = new ClassPathResource("tests/" + appContextName + "/spring.xml");
		final GenericXmlApplicationContext appCtx = new GenericXmlApplicationContext(configResource);
		appCtx.setDisplayName(appContextName);		
		APPCTX = appCtx;		
	}
	
	/**
	 * Stops the application context
	 */
	protected static void stopAppContext() {
		if(APPCTX!=null) {
			try { ((GenericXmlApplicationContext)APPCTX).close(); } catch (Exception x) {/* No Op */}
		}
		APPCTX = null;
	}
	
	/**
	 * Creates a new kafka producer
	 * @param name The test name this producer is for
	 * @param keyType The key type
	 * @param valueType The value type
	 * @return the producer
	 * @param <K> The key type
	 * @param <V> The value type
	 */
	protected <K,V> KafkaProducer<K,V> newProducer(final String name, final Class<K> keyType, final Class<V> valueType) {
		final Properties p = Props.strToProps(
				StringHelper.resolveTokens(
						URLHelper.getStrBuffFromURL(getClass().getClassLoader().getResource("tests/" + name + "/producer.properties"))
				)
		);
		final KafkaProducer<K,V> producer =  new KafkaProducer<K,V>(p);
		closeables.add(producer);
		return producer;
	}
	
	/**
	 * Creates a new kafka consumer
	 * @param name The test name this consumer is for
	 * @param consumerGroup An optional consumer group to assign. Defaults to the test name plus a prefix.
	 * @param keyType The key type
	 * @param valueType The value type
	 * @return the producer
	 * @param <K> The key type
	 * @param <V> The value type
	 */
	protected <K,V> KafkaConsumer<K,V> newConsumer(final String name, final String consumerGroup, final Class<K> keyType, final Class<V> valueType) {
		final Properties p = Props.strToProps(
				StringHelper.resolveTokens(
						URLHelper.getStrBuffFromURL(getClass().getClassLoader().getResource("tests/" + name + "/consumer.properties"))
				)
		);
		p.setProperty("group.id", consumerGroup==null ? (name + "TestGroup") : consumerGroup.trim());
		final KafkaConsumer<K,V> consumer = new KafkaConsumer<K,V>(p);
		closeables.add(consumer);
		return consumer;		
	}
	
	/**
	 * Creates a new kafka consumer
	 * @param name The test name this consumer is for
	 * @param keyType The key type
	 * @param valueType The value type
	 * @return the producer
	 * @param <K> The key type
	 * @param <V> The value type
	 */
	protected <K,V> KafkaConsumer<K,V> newConsumer(final String name, final Class<K> keyType, final Class<V> valueType) {
		return newConsumer(name, null, keyType, valueType);
	}
	
	
			
	
//	<property name="clientId">
//	<util:constant static-field="com.heliosapm.streams.metrics.router.config.StreamsConfigBuilder.DEFAULT_CLIENT_ID" />
//</property>
//<property name="pollWaitMs" value="${streamhub.config.pollwaitms:10}"/>
//<property name="stateStoreDir">
//	<util:constant static-field="com.heliosapm.streams.metrics.router.config.StreamsConfigBuilder.DEFAULT_STATE_STORE_NAME" />
//</property>
//<property name="timeExtractor" value="${streamhub.config.timeextractor:com.heliosapm.streams.metrics.TextLineTimestampExtractor}"/>
	
	
}
