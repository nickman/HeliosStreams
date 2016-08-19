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

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Properties;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.heliosapm.utils.url.URLHelper;

/**
 * <p>Title: TopicDefinition</p>
 * <p>Description: Defines a topic that can be created in the {@link KafkaTestServer}</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.kafka.TopicDefinition</code></p>
 */

public class TopicDefinition {
	/** The topic name */
	@JsonProperty(value="name", required=true)
	protected String topicName = null;
	/** The topic partition count */
	@JsonProperty(value="partitions", required=false, defaultValue="1")
	protected int partitionCount = 1;
	/** The topic replica count */
	@JsonProperty(value="replicas", required=false, defaultValue="1")
	protected int replicaCount = 1;
	/** The topic properties. See <a href="http://kafka.apache.org/documentation.html#topic-config">Topic-level configuration </a> */
	@JsonProperty(value="properties", required=false)
	protected final Properties topicProperties = new Properties();
	
	
	/** A sharable object mapper */
	public static final ObjectMapper OBJ_MAPPER = new ObjectMapper();
	/** The UTF character set */
	public static final Charset UTF8 = Charset.forName("UTF8");
	
	
	public static void main(String[] args) {
		final String json = URLHelper.getTextFromFile(new File("./src/test/resources/brokers/test/topics/topics.json"));
		try {
			TopicDefinition[] td = OBJ_MAPPER.readValue(json, TopicDefinition[].class);
			log(Arrays.toString(td));
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
		}
	}
	
	public static void log(Object msg) {
		System.out.println(msg);
	}
	/**
	 * Creates a new TopicDefinition
	 * @param topicName The topic name
	 * @param partitionCount The partition count
	 * @param replicaCount The replica count
	 * @param topicProperties The optional topic properties
	 */
	public TopicDefinition(final String topicName, final int partitionCount, final int replicaCount, final Properties topicProperties) {
		if(topicName==null || topicName.trim().isEmpty()) throw new IllegalArgumentException("The passed topic name was null or empty");
		if(partitionCount < 1) throw new IllegalArgumentException("Invalid topic partition count: " + partitionCount);
		if(replicaCount < 1) throw new IllegalArgumentException("Invalid topic replica count: " + replicaCount);
		this.topicName = topicName.trim();
		this.partitionCount = partitionCount;
		this.replicaCount = replicaCount;
		if(topicProperties!=null) {
			this.topicProperties.putAll(topicProperties);
		}
	}
	
	/**
	 * Unmarshalls the passed JSON to an array of topic definitions
	 * @param json The JSON to unmarshall
	 * @return an array of topic definitions
	 */
	public static TopicDefinition[] topics(final String json) {
		if(json==null || json.trim().isEmpty()) throw new IllegalArgumentException("The passed json was null or empty");
		final String _json = json.trim();		
		final char first = _json.charAt(0);		
		if(first=='[') {
			try {
				return OBJ_MAPPER.readValue(json, TopicDefinition[].class);
			} catch (Exception ex) {
				throw new RuntimeException("Failed to parse JSON array [" + _json + "]", ex);
			}
		} else if(first=='{') {
			try {
				return new TopicDefinition[]{OBJ_MAPPER.readValue(json, TopicDefinition.class)};
			} catch (Exception ex) {
				throw new RuntimeException("Failed to parse JSON object [" + _json + "]", ex);
			}			
		} else {
			throw new RuntimeException("Passed JSON had invalid first character [" + first + "]. JSON was [" + _json + "]");
		}
	}
	
	/**
	 * Unmarshalls the passed JSON to an array of topic definitions
	 * @param json The JSON to unmarshall
	 * @return an array of topic definitions
	 */
	public static TopicDefinition[] topics(final File json) {
		if(json==null) throw new IllegalArgumentException("The passed file was null");
		if(!json.canRead()) throw new IllegalArgumentException("The passed file [" + json + "] could not be read");
		return topics(URLHelper.getTextFromFile(json));
	}
	
	/**
	 * Unmarshalls the passed JSON URL to an array of topic definitions
	 * @param json The URL from which to read the JSON to unmarshall
	 * @return an array of topic definitions
	 */
	public static TopicDefinition[] topics(final URL json) {
		if(json==null) throw new IllegalArgumentException("The passed URL was null");		
		return topics(URLHelper.getTextFromURL(json));
	}
	
	/**
	 * Unmarshalls the passed JSON input stream to an array of topic definitions
	 * @param json The JSON to unmarshall
	 * @return an array of topic definitions
	 */
	public static TopicDefinition[] topics(final InputStream json) {
		if(json==null) throw new IllegalArgumentException("The passed input stream was null");		
		InputStreamReader isr = null;
		BufferedReader br = null;
		final StringBuilder b = new StringBuilder();
		String line = null;
		try {
			isr = new InputStreamReader(json);
			br = new BufferedReader(isr);
			while((line = br.readLine())!=null) {
				b.append(line);
			}
		} catch (Exception ex) {
			throw new RuntimeException("Failed to read a string from the passed input stream", ex);
		} finally {
			if(isr!=null) try { isr.close(); } catch (Exception x) {/* No Op */}
			if(br!=null) try { br.close(); } catch (Exception x) {/* No Op */}
		}
		return topics(b.toString());
	}
	

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		final StringBuilder b = new StringBuilder("TopicDefinition [");
		b.append("\n\tName:").append(topicName);
		b.append("\n\tPartitionCount:").append(partitionCount);
		b.append("\n\tReplicaCount:").append(replicaCount);
		if(topicProperties.isEmpty()) {
			b.append("\n\tProperties: []");
		} else {
			b.append("\n\tProperties: [");
			for(final String key: topicProperties.stringPropertyNames()) {
				b.append("\n\t\t").append(key).append(":").append(topicProperties.getProperty(key));
			}
			b.append("\n\t]");
		}
		return b.append("\n]").toString();
	}


	/**
	 * Creates a new TopicDefinition
	 */
	@SuppressWarnings("unused")
	private TopicDefinition() {
		/* No Op */
	}

	/**
	 * Returns the topic name
	 * @return the topic name
	 */
	public String getTopicName() {
		return topicName;
	}

	/**
	 * Returns the partition count
	 * @return the partition count
	 */
	public int getPartitionCount() {
		return partitionCount;
	}

	/**
	 * Returns the replica count
	 * @return the replica count
	 */
	public int getReplicaCount() {
		return replicaCount;
	}

	/**
	 * Returns the topic properties
	 * @return the topic properties
	 */
	public Properties getTopicProperties() {
		return topicProperties;
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((topicName == null) ? 0 : topicName.hashCode());
		return result;
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TopicDefinition other = (TopicDefinition) obj;
		if (topicName == null) {
			if (other.topicName != null)
				return false;
		} else if (!topicName.equals(other.topicName))
			return false;
		return true;
	}
	
	

}
