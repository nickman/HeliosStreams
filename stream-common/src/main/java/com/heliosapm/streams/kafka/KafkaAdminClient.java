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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.common.requests.MetadataResponse.TopicMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import com.heliosapm.utils.io.StdInCommandHandler;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.common.TopicExistsException;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;

/**
 * <p>Title: KafkaAdminClient</p>
 * <p>Description: Pure java wrapper for the scala based KafkaUtils client for issuing administrative operations against a Kafka broker or cluster</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.kafka.KafkaAdminClient</code></p>
 */

public class KafkaAdminClient implements Watcher, Closeable {
	
	/** A map of connected clients keyed by the zk-connect string */
	private static final NonBlockingHashMap<String, KafkaAdminClient> clients = new NonBlockingHashMap<String, KafkaAdminClient>(); 
	/** A place holder admin client */
	private static final KafkaAdminClient PLACEHOLDER = new KafkaAdminClient();
	
	/** It is recommended to use quite large sessions timeouts for ZooKeeper. */
    private static final int DEFAULT_SESSION_TIMEOUT = 30000;	
	/** It is recommended to use quite large connection timeouts for ZooKeeper. */
    private static final int DEFAULT_CONNECTION_TIMEOUT = 60000;	

	/** An empty topicDef array const */
	private static final TopicDefinition[] EMPTY_TOPIC_ARR = {};

	static {
		System.setProperty("zookeeper.jmx.log4j.disable", "true");
	}

	
	/** The zk connect string */
	private final String zkConnect;
	/** The zk session timeout */
	private final int sessionTimeout;
	/** The zk connection timeout */
	private final int connectionTimeout;
	/** A Zookeeper util client for admin ops */
	protected final ZkUtils zkUtils;
	/** A zookeeper client for admin ops */
	protected final ZkClient zkClient;
	/** A zookeeper connection for admin ops */
	protected final ZkConnection zkConnection;
	/** The connected flag */
	protected final AtomicBoolean connected = new AtomicBoolean(false);
	
	
	
	/** The current zk state */
	private final AtomicReference<Watcher.Event.KeeperState> state = new AtomicReference<Watcher.Event.KeeperState>(null); 
	/** Instance logger */
	private final Logger log = LogManager.getLogger(getClass());
	
	/**
	 * Acquires a KafkaAdminClient for the specified zk connect string
	 * @param zkConnect The zk connect string
	 * @param sessionTimeout The session timeout
	 * @param connectionTimeout The connection timeout
	 * @return the client
	 */
	public static KafkaAdminClient getClient(final String zkConnect, final int sessionTimeout, final int connectionTimeout) {
		if(zkConnect==null || zkConnect.trim().isEmpty()) throw new IllegalArgumentException("The passed zk connect string was null or empty");
		if(sessionTimeout < 0) throw new IllegalArgumentException("The passed session timeout [" + sessionTimeout + "] was invalid");
		if(connectionTimeout < 0) throw new IllegalArgumentException("The passed connection timeout [" + connectionTimeout + "] was invalid");
		final String _zkConnect = zkConnect.trim();
		KafkaAdminClient client = clients.putIfAbsent(_zkConnect, PLACEHOLDER);
		if(client==null || client==PLACEHOLDER) {
			client = new KafkaAdminClient(_zkConnect, sessionTimeout, connectionTimeout);
			clients.replace(_zkConnect, client);
		}
		return client;		
	}

	/**
	 * Acquires a KafkaAdminClient for the specified zk connect string using the default session and connect timeouts
	 * @param zkConnect The zk connect string
	 * @return the client
	 */
	public static KafkaAdminClient getClient(final String zkConnect) {
		return getClient(zkConnect, DEFAULT_SESSION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT);
	}
	
	/**
	 * Acquires a KafkaAdminClient for <b><code>localhost:2181</code></b> using the default session and connect timeouts
	 * @return the client
	 */
	public static KafkaAdminClient getClient() {
		return getClient("localhost:2181", DEFAULT_SESSION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT);
	}
	
	/**
	 * Creates and connects a new KafkaAdminClient
	 * @param zkConnect The zk connect string
	 * @param sessionTimeout The session timeout
	 * @param connectionTimeout The connection timeout
	 */
	private KafkaAdminClient(final String zkConnect, final int sessionTimeout, final int connectionTimeout) {
		this.zkConnect = zkConnect;
		this.sessionTimeout = sessionTimeout;
		this.connectionTimeout = connectionTimeout;
		zkClient = new ZkClient(
				zkConnect,
				this.sessionTimeout,
				this.connectionTimeout,
			    ZKStringSerializer$.MODULE$);
		zkConnection = new ZkConnection(zkConnect, DEFAULT_SESSION_TIMEOUT);
		zkConnection.connect(this);
		zkUtils = new ZkUtils(zkClient, zkConnection, false);					
		
	}
	
	private KafkaAdminClient() {
		zkConnect = null;
		sessionTimeout = -1;
		connectionTimeout = -1;
		zkClient = null;
		zkConnection = null;
		zkUtils = null;									
	}

	/**
	 * Quickie test
	 * @param args None
	 */
	public static void main(String[] args) {
		final KafkaAdminClient client = getClient();
		StdInCommandHandler.getInstance()
			.registerCommand("close", new Runnable(){
				public void run() {
					client.doClose();
					System.exit(0);
				}
			})
			.registerCommand("clients", new Runnable(){
				public void run() {
					final StringBuilder b = new StringBuilder("Connected Clients [").append(clients.size()).append("]:");
					for(KafkaAdminClient client: clients.values()) {
						b.append("\n\tZKClient:").append(client.zkConnect);
					}
					System.err.println(b);
				}
			})			
			.run();
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
	 */
	@Override
	public void process(final WatchedEvent event) {
		if(event==null) return;
		final Watcher.Event.KeeperState newState = event.getState();
		if(newState==null) return;
		final Watcher.Event.KeeperState priorState = state.getAndSet(event.getState());
		if(priorState==null || newState != priorState) {
			switch(event.getState()) {
				case AuthFailed:
					connected.set(false);
					log.warn("[{}] : Authentication Failed", zkConnect);
					doClose();
					break;
				case ConnectedReadOnly:
					connected.set(true);
					log.info("[{}] : Connected ReadOnly", zkConnect);
					break;
				case Disconnected:		
					connected.set(false);
					log.warn("[{}] : Disconnected", zkConnect);
					doClose();
				case Expired:
					connected.set(false);
					log.warn("[{}] : Expired", zkConnect);
					doClose();					
					break;
				case SaslAuthenticated:
					connected.set(true);
					log.info("[{}] : SaslAuthenticated", zkConnect);
					break;
				case SyncConnected:
					connected.set(true);
					log.info("[{}] : Connected", zkConnect);
					break;
				default:
					break;
			
			}
			
		}
	}

	private void doClose() {
		try {
			close();
		} catch (Exception x) {/* No Op */}
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() throws IOException {
		connected.set(false);
		clients.remove(zkConnect);
		try { zkClient.close(); } catch (Exception x) { /* No Op */ }
		try { zkConnection.close(); } catch (Exception x) { /* No Op */ }
		try { zkUtils.close(); } catch (Exception x) { /* No Op */ }
	}
	
	/**
	 * Creates a new topic
	 * @param topicName The topic name
	 * @param partitionCount The partition count
	 * @param replicaCount The replica count
	 * @param topicProperties The optional topic properties
	 * @throws TopicExistsException thrown if the requested topic already exists
	 */
	public void createTopic(final String topicName, final int partitionCount, final int replicaCount, final Properties topicProperties) throws TopicExistsException {
		if(!connected.get()) throw new IllegalStateException("The KafkaTestServer is not running");
		if(topicName==null || topicName.trim().isEmpty()) throw new IllegalArgumentException("The passed topic name was null or empty");
		if(partitionCount < 1) throw new IllegalArgumentException("Invalid topic partition count: " + partitionCount);
		if(replicaCount < 1) throw new IllegalArgumentException("Invalid topic replica count: " + replicaCount);
		AdminUtils.createTopic(zkUtils, topicName, partitionCount, replicaCount, topicProperties==null ? new Properties() : topicProperties, new RackAwareMode.Disabled$());
	}
	
	/**
	 * Determines if the passed topic name represents an existing topic
	 * @param topicName the topic name to test for 
	 * @return true if the passed topic name represents an existing topic, false otherwise
	 */
	public boolean topicExists(final String topicName) {
		if(!connected.get()) throw new IllegalStateException("The KafkaTestServer is not running");
		if(topicName==null || topicName.trim().isEmpty()) throw new IllegalArgumentException("The passed topic name was null or empty");
		return AdminUtils.topicExists(zkUtils, topicName.trim());
	}
	
	/**
	 * Determines if the passed consumer group is active
	 * @param consumerGroup the consumer group to test for 
	 * @return true if if the passed consumer group is active, false otherwise
	 */
	public boolean consumerGroupActive(final String consumerGroup) {
		if(!connected.get()) throw new IllegalStateException("The KafkaTestServer is not running");
		if(consumerGroup==null || consumerGroup.trim().isEmpty()) throw new IllegalArgumentException("The passed consumer group was null or empty");
		return AdminUtils.isConsumerGroupActive(zkUtils, consumerGroup.trim());
	}	
	
	/**
	 * Returns a map of topic properties keyed by the topic name for all topics installed into this server
	 * @return a map of topic properties keyed by the topic name 
	 */
	public Map<String, Properties> topicProperties() {
		if(!connected.get()) throw new IllegalStateException("The KafkaTestServer is not running");
		final scala.collection.Map<String, Properties> scmap = AdminUtils.fetchAllTopicConfigs(zkUtils);
		return JavaConversions.mapAsJavaMap(scmap);
	}
	
	/**
	 * Returns a set of the names of all topics installed into this server
	 * @return a set of topic names 
	 */
	public Set<String> topicNames() {
		if(!connected.get()) throw new IllegalStateException("The KafkaTestServer is not running");
		final Set<String> set = new LinkedHashSet<String>();
		set.addAll(topicProperties().keySet());
		return set;
	}
	
	
	/**
	 * Returns the metadata for the specified topic names
	 * @param topicNames The topic names to retrieve metadata for, or null/empty array for all topics
	 * @return a map of TopicMetadatas keyed by topic name 
	 */
	public Map<String, TopicMetadata> topicMetaData(final String...topicNames) {
		if(!connected.get()) throw new IllegalStateException("The KafkaTestServer is not running");
		final Set<String> set = (topicNames==null || topicNames.length==0) ? topicNames() : new LinkedHashSet<String>(Arrays.asList(topicNames));		
		final Set<TopicMetadata> meta = JavaConversions.setAsJavaSet(AdminUtils.fetchTopicMetadataFromZk(JavaConverters.asScalaSetConverter(set).asScala(), zkUtils));
		final Map<String, TopicMetadata> map = new HashMap<String, TopicMetadata>(meta.size());
		for(TopicMetadata tm: meta) {
			map.put(tm.topic(), tm);
		}
		return map;
	}
	
	/**
	 * Retrieves topic definitions 
	 * @param topicNames The names of the topics to retrieve definitions for, or an empty/null array for all of them
	 * @return a map of topic definitons keyed by the topic name
	 */
	public Map<String, TopicDefinition> topicDefinitions(final String...topicNames) {
		if(!connected.get()) throw new IllegalStateException("The KafkaTestServer is not running");
		final Map<String, Properties> topicProps = topicProperties();
		if(topicProps.isEmpty()) return Collections.emptyMap();
		final String[] targetTopics = (topicNames==null || topicNames.length==0) ? topicProps.keySet().toArray(new String[topicProps.size()]) : topicNames;
		final Map<String, TopicDefinition> map = new HashMap<String, TopicDefinition>(targetTopics.length);
		final Map<String, TopicMetadata> meta = topicMetaData(targetTopics);
		for(Map.Entry<String, TopicMetadata> entry: meta.entrySet()) {
			final String name = entry.getKey();
			final TopicMetadata tmeta = entry.getValue();
			final Properties p = topicProps.get(name);
			final int partitions = tmeta.partitionMetadata().size();
			final int replicas = tmeta.partitionMetadata().get(0).replicas().size();
			final TopicDefinition topicDef = new TopicDefinition(name, partitions, replicas, p);
			map.put(name, topicDef);
		}
		return map;
	}
	
	/**
	 * Retrieves topic definitions as a JSON array document 
	 * @param topicNames The names of the topics to retrieve definitions for, or an empty/null array for all of them
	 * @return a JSON array document
	 */
	public String topicDefinitionsJSON(final String...topicNames) {
		final Map<String, TopicDefinition> defs = topicDefinitions(topicNames);
		try {
			return TopicDefinition.OBJ_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(defs.values().toArray(new TopicDefinition[defs.size()]));
		} catch (Exception ex) {
			throw new RuntimeException("Failed to serialize topic definitions to JSON", ex);
		}
		
	}
	
	/**
	 * Deletes the named topics
	 * @param topicNames The names of the topics to delete
	 * @return A set of the names of the topics that were successfully deleted
	 */
	public String[] deleteTopics(final String...topicNames) {
		if(!connected.get()) throw new IllegalStateException("The KafkaTestServer is not running");
		if(topicNames==null || topicNames.length==0) return new String[0];
		final Set<String> deleted = new LinkedHashSet<String>();
		for(String topicName: topicNames) {
			if(topicName==null || topicName.trim().isEmpty()) {
				try {
					AdminUtils.deleteTopic(zkUtils, topicName.trim());
					deleted.add(topicName.trim());
				} catch (Exception ex) {
					log.warn("Failed to delete topic [" + topicName.trim() + "]", ex);
				}
			}
		}
		return deleted.toArray(new String[deleted.size()]);
	}
	
	
	
	/**
	 * Creates topics in this kafka server, one for each passed topic definition
	 * @param topicDefs The TopicDefinitions to create topics from
	 * @return an array of TopicDefinitions, one for each successfully created topic
	 */
	public TopicDefinition[] createTopics(final TopicDefinition...topicDefs) {
		if(topicDefs==null || topicDefs.length==0) return EMPTY_TOPIC_ARR;
		final Set<TopicDefinition> created = new LinkedHashSet<TopicDefinition>();
		for(TopicDefinition td : topicDefs) {
			if(td==null) continue;
			try {
				createTopic(td.getTopicName(), td.getPartitionCount(), td.getReplicaCount(), td.getTopicProperties());
				created.add(td);
			} catch (Exception ex) {
				log.warn("Failed to create topic [{}]", td, ex);
			}
		}
		return created.toArray(new TopicDefinition[created.size()]);
	}
	
	/**
	 * Creates topics in this kafka server, one for each topic definition parsed out of the passed json
	 * @param json The TopicDefinitions JSON to create topics from
	 * @return an array of TopicDefinitions, one for each successfully created topic
	 */
	public TopicDefinition[] createTopics(final String json) {
		return createTopics(TopicDefinition.topics(json));
	}
	
	/**
	 * Creates topics in this kafka server, one for each topic definition parsed out of the passed json file
	 * @param json The TopicDefinitions JSON to create topics from
	 * @return an array of TopicDefinitions, one for each successfully created topic
	 */
	public TopicDefinition[] createTopics(final File json) {
		return createTopics(TopicDefinition.topics(json));
	}
	
	/**
	 * Creates topics in this kafka server, one for each topic definition parsed out of the JSON read from the passed URL
	 * @param json The URL to read the JSON TopicDefinitions from
	 * @return an array of TopicDefinitions, one for each successfully created topic
	 */
	public TopicDefinition[] createTopics(final URL json) {
		return createTopics(TopicDefinition.topics(json));
	}
	
	/**
	 * Creates topics in this kafka server, one for each topic definition parsed out of the JSON read from the passed input stream
	 * @param json The input stream to read the JSON TopicDefinitions from
	 * @return an array of TopicDefinitions, one for each successfully created topic
	 */
	public TopicDefinition[] createTopics(final InputStream json) {
		return createTopics(TopicDefinition.topics(json));
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "KafkaAdminClient [" + zkConnect + "]:" + getState();
	}

	/**
	 * Returns the session timeout in ms.
	 * @return the session timeout in ms.
	 */
	public int getSessionTimeout() {
		return sessionTimeout;
	}
	
	/**
	 * Returns the connection timeout in ms.
	 * @return the connection timeout in ms.
	 */
	public int getConnectionTimeout() {
		return connectionTimeout;
	}
	

	/**
	 * Indicates if the client is connected
	 * @return true if the client is connected, false otherwise
	 */
	public boolean getConnected() {
		return connected.get();
	}

	/**
	 * Returns the client state
	 * @return the state
	 */
	public Watcher.Event.KeeperState getState() {
		return state.get();
	}
	
	

}
