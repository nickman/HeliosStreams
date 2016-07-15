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

import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

//import org.springframework.kafka.test.rule.KafkaEmbedded;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.heliosapm.utils.time.SystemClock;



/**
 * <p>Title: EmbeddedKafkaDefinition</p>
 * <p>Description: Definition for an embedded kafka cluster and topics</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.opentsdb.EmbeddedKafkaDefinition</code></p>
 */

public class EmbeddedKafkaDefinition {
	/** The number of broker instances */
	@JsonProperty
	protected int brokers = 1;
	/** Indicates if the brokers should have a controlled shutdown */
	@JsonProperty
	protected boolean controlled = true;
	/** The number of partitions for each topic */
	@JsonProperty
	protected int partitions = 1;
	/** The names of the topics to create */
	@JsonProperty
	protected String[] topicNames = {};
	
	/** The kafka cluster instance */
//	protected KafkaEmbedded cluster = null;
	/** Indicates if the kafka cluster instance  is started */
	protected final AtomicBoolean started = new AtomicBoolean(false);
	
//	private static final Method shutdownMethod;
//	private static final Method startMethod;
	
	static {
		try {
//			shutdownMethod = KafkaEmbedded.class.getDeclaredMethod("after");
//			shutdownMethod.setAccessible(true);
//			startMethod = KafkaEmbedded.class.getDeclaredMethod("before");
//			startMethod.setAccessible(true);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
	
	
	
	private static final ObjectMapper OM = new ObjectMapper();
	
	/**
	 * Creates a new EmbeddedKafkaDefinition
	 * @param brokers The number of broker instances
	 * @param controlled Indicates if the brokers should have a controlled shutdown
	 * @param partitions The number of partitions for each topic
	 * @param topicNames The names of the topics to create
	 */	
	public EmbeddedKafkaDefinition(final int brokers, final boolean controlled, final int partitions, final String...topicNames) {
		super();
		this.brokers = brokers;
		this.controlled = controlled;
		this.partitions = partitions;
		this.topicNames = topicNames;
	}
	
	/**
	 * Indicates if this cluster is started
	 * @return true if this cluster is started, false otherwise
	 */
	public boolean isStarted() {
		return started.get();
	}
	
//	protected void _stop() {
//		try {
//			shutdownMethod.invoke(cluster);
//		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
//			throw new RuntimeException(e);
//		}
//	}
//	
//	protected void _start() {
//		try {
//			startMethod.invoke(cluster);
//		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
//			throw new RuntimeException(e);
//		}
//	}
	
	
	public void start() {
//		if(started.compareAndSet(false, true)) {
//			try {
//				cluster = new KafkaEmbedded(brokers, controlled, partitions, topicNames);
//				_start();
//				System.out.println("Kafka Cluster Started");
//			} catch (Exception ex) {
//				if(cluster!=null) {
//					try { _stop(); } catch (Exception x) {/* No Op */}
//					cluster = null;
//				}
//				started.set(false);
//				throw new RuntimeException(ex);
//			}
//		}
	}
	
	public void stop() {
//		if(started.compareAndSet(true, false)) {
//			if(cluster!=null) {
//				try {
//					_stop();
//					cluster = null;
//					System.out.println("Kafka Cluster Stopped");
//				} catch (Exception ex) {
//					throw new RuntimeException(ex);
//				}
//			}
//		}
	}
	
	public static void main(String[] args) {		
		try {
			EmbeddedKafkaDefinition def = new EmbeddedKafkaDefinition(4, true, 3, "my-topic-1", "my-topic-2");
			System.out.println(OM.writeValueAsString(def));
			def = fromResource("configs/brokers/default.json");
			System.out.println(def);
			def.start();
			SystemClock.sleep(10000);
			def.stop();
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
		}
	}
	
	@Override
	public String toString() {
		return "EmbeddedKafkaDefinition [brokers=" + brokers + ", controlled=" + controlled + ", partitions="
				+ partitions + ", topicNames=" + Arrays.toString(topicNames) + "], started:" + started;
	}

	/**
	 * Creates a new empty EmbeddedKafkaDefinition.
	 * Intended for Json marshalling
	 */
	public EmbeddedKafkaDefinition() {
		
	}

	/**
	 * Creates a new EmbeddedKafkaDefinition from the content read from the passed resource name
	 * @param resource The resource that is read from this class's class loader
	 * @return a new EmbeddedKafkaDefinition
	 */
	public static EmbeddedKafkaDefinition fromResource(final String resource) {
		return from(EmbeddedKafkaDefinition.class.getClassLoader().getResource(resource));
	}
	
	
	/**
	 * Creates a new EmbeddedKafkaDefinition from the content read from the passed URL
	 * @param url The url to read from
	 * @return a new EmbeddedKafkaDefinition
	 */
	public static EmbeddedKafkaDefinition from(final URL url) {
		InputStream is = null;
		try {
			is =url.openStream();
			return OM.readValue(is, EmbeddedKafkaDefinition.class);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		} finally {
			if(is!=null) try { is.close(); } catch (Exception x) {/* No Op */}
		}
	}
	
	
	
}
