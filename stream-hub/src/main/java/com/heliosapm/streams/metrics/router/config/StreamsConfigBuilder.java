/**
 * Helios, OpenSource Monitoring
 * Brought to you by the Helios Development Group
 *
 * Copyright 2016, Helios Development Group and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org. 
 *
 */
package com.heliosapm.streams.metrics.router.config;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.DefaultPartitionGrouper;
import org.apache.kafka.streams.processor.PartitionGrouper;
import org.apache.kafka.streams.processor.TimestampExtractor;

import com.heliosapm.streams.common.kafka.interceptor.SwitchableMonitoringInterceptor;
import com.heliosapm.streams.metrics.TextLineTimestampExtractor;

/**
 * <p>Title: StreamsConfigBuilder</p>
 * <p>Description: Fluent style {@link StreamsConfig} builder</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.router.config.StreamsConfigBuilder</code></p>
 */

public class StreamsConfigBuilder {
	
	/** The default client id which is the runtime name */
	public static final String DEFAULT_CLIENT_ID = "StreamHubNode." + ManagementFactory.getRuntimeMXBean().getName().replace('@', '.');
	/** The default state store directory */
	public static final File DEFAULT_STATE_STORE = new File(new File(System.getProperty("java.io.tmpdir")), "kafka-streams-state-store");	
	/** The default state store directory name */
	public static final String DEFAULT_STATE_STORE_NAME = new File(new File(System.getProperty("java.io.tmpdir")), "kafka-streams-state-store").getAbsolutePath();	
	
	// ============================
	// Mandatory Config
	// ============================
	/** A list of host/port pairs to use for establishing the initial connection to the Kafka cluster: <b><code>bootstrap.servers</code></b> */
	protected String bootstrapServers = "localhost:9092";
	/** An identifier for the stream processing application. Must be unique within the Kafka cluster: <b><code>application.id</code></b> */
	protected String applicationId = "StreamApp";
 	/** Zookeeper connect string for Kafka topic management: <b><code>zookeeper.connect</code></b>  */
 	protected String zookeeperConnect = "localhost:2181";

	// ============================
	// Optional Config
	// ============================
	/** The default (de)serializer for record keys: <b><code>key.serd</code></b>  */
 	protected Serde<?> keySerde = Serdes.String();  
	/** The default (de)serializer for record values: <b><code>value.serd</code></b>  */
 	protected Serde<?> valueSerde = Serdes.String();   	
	/** The maximum number of records to buffer per partition : <b><code>buffered.records.per.partition</code></b> */
	protected int maxBufferedPerPartition = 1000;
	/** An id string to pass to the server when making requests. (This setting is passed to the consumer/producer clients used internally by Kafka Streams.) : <b><code>buffered.records.per.partition</code>client.id</b> */
	protected String clientId = DEFAULT_CLIENT_ID;
	/** The frequency with which to save the position (offsets in source topics) of tasks : <b><code>commit.interval.ms</code></b> */
	protected long commitIntervalMs = 30000;
	/** A list of classes to use as metrics reporters : <b><code>metric.reporters</code></b> */
	protected Set<String> metricReportingClasses = new HashSet<String>();
	/** The number of standby replicas for each task : <b><code>num.standby.replicas</code></b> */
	protected int standbyReplicas = 0;
	/** The number of threads to execute stream processing: <b><code>num.stream.threads</code></b> */
	protected int streamThreads = 1;
	/** Partition grouper class that implements the PartitionGrouper interface : <b><code>partition.grouper</code></b> */
	protected String partitionGrouper = DefaultPartitionGrouper.class.getName();
	/** The amount of time in milliseconds to block waiting for input : <b><code>poll.ms</code></b> */
	protected long pollWaitMs = 100;
	/** The replication factor for changelog topics and repartition topics created by the application : <b><code>replication.factor</code></b> */
	protected int replicationFactor = 1;
	/** The amount of time in milliseconds to wait before deleting state when a partition has migrated : <b><code>state.cleanup.delay.ms</code></b> */
	protected long stateCleanupDelayMs = 60000;
	/** Directory location for state stores	/tmp/kafka-streams : <b><code>state.dir</code></b> */
	protected File stateStoreDir = DEFAULT_STATE_STORE;	
	/** Timestamp extractor class that implements the TimestampExtractor interface : <b><code>timestamp.extractor</code></b> */
	protected String timeExtractor = TextLineTimestampExtractor.class.getName();
	
	/** The number of samples maintained to compute metrics : <b><code>metrics.num.samples</code></b> */
	protected int metricSampleCount = 2;
	/** The window of time a metrics sample is computed over : <b><code>metrics.sample.window.ms</code></b> */
	protected long metricSampleWindow = 30000;
	/** A monitoring interceptor that will install as both a producer and consumer interceptor in a streams client */
	protected boolean enableMonitoringInterceptor = false;
	
	
	/**
	 * Creates a new StreamsConfigBuilder
	 */
	public StreamsConfigBuilder() {

	}
	
	/**
	 * Resets this builder
	 * @return this builder in its reset mode
	 */
	public StreamsConfigBuilder reset() {
		bootstrapServers = "localhost:9092";
		applicationId = "StreamApp" + DEFAULT_CLIENT_ID;
	 	keySerde = Serdes.String();  
	 	valueSerde = Serdes.String();  
	 	zookeeperConnect = "localhost:2181";
		maxBufferedPerPartition = 1000;
		clientId = DEFAULT_CLIENT_ID;
		commitIntervalMs = 30000;
		metricReportingClasses = new HashSet<String>();
		standbyReplicas = 0;
		streamThreads = 1;
		partitionGrouper = DefaultPartitionGrouper.class.getName();
		pollWaitMs = 100;
		replicationFactor = 1;
		stateCleanupDelayMs = 60000;
		stateStoreDir = DEFAULT_STATE_STORE;	
		timeExtractor = TextLineTimestampExtractor.class.getName();
		metricSampleCount = 2;
		metricSampleWindow = 30000;
		enableMonitoringInterceptor = false; 
		return this;
	}
	
	/**
	 * Builds the raw properties for this builder 
	 * @return the built properties
	 */
	public Properties buildProperties() {
		final Properties p = new Properties();
		p.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
		p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		p.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, keySerde.getClass().getName());
		p.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, valueSerde.getClass().getName());
		p.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeperConnect);
		p.put(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, maxBufferedPerPartition);
		p.put(StreamsConfig.CLIENT_ID_CONFIG, clientId);
		p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, commitIntervalMs);
		p.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, standbyReplicas);
		p.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, streamThreads);
		p.put(StreamsConfig.PARTITION_GROUPER_CLASS_CONFIG, partitionGrouper);
		p.put(StreamsConfig.POLL_MS_CONFIG, pollWaitMs);
		p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, replicationFactor);
		p.put(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, stateCleanupDelayMs);
		p.put(StreamsConfig.STATE_DIR_CONFIG, stateStoreDir.getAbsolutePath());
		p.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, timeExtractor);
		p.put(StreamsConfig.METRICS_NUM_SAMPLES_CONFIG, metricSampleCount);
		p.put(StreamsConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, metricSampleWindow);
		p.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
		p.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, metricReportingClasses.toString().replace("[", "").replace("]", "").replace(" ", ""));
		if(enableMonitoringInterceptor) {
			p.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, SwitchableMonitoringInterceptor.class.getName());
		}
		return p;
	}
	
	/**
	 * Builds and returns the currently configured streams confiug
	 * @return the built StreamsConfig
	 */
	public StreamsConfig build() {
		final Properties p = buildProperties();
		return new StreamsConfig(p);
	}
	
	

	/**
	 * Sets the kafka broker bootstrap servers string
	 * @param bootstrapServers the bootstrapServers to set
	 * @return this builder
	 */	
	public StreamsConfigBuilder setBootstrapServerStr(final String bootstrapServers) {
		if(bootstrapServers==null || bootstrapServers.trim().isEmpty()) throw new IllegalArgumentException("The passed bootstrap servers string was null or empty");
		this.bootstrapServers = bootstrapServers;
		return this;
	}
	
	/**
	 * Sets the kafka broker bootstrap servers 
	 * @param bootstrapServers the bootstrapServers to set
	 * @return this builder
	 */
	public StreamsConfigBuilder setBootstrapServers(final String...bootstrapServers) {
		final StringBuilder b = new StringBuilder();
		for(String s : bootstrapServers) {
			if(s==null || s.trim().isEmpty()) continue;
			if(b.length() > 0) b.append(",");
			b.append(s.trim());
		}
		return setBootstrapServerStr(b.toString());
	}
	
	/**
	 * Sets the enabled state of the {@link SwitchableMonitoringInterceptor} in the streams client
	 * @param enable true to enable, false otherwise
	 * @return this builder
	 */
	public StreamsConfigBuilder setMonitoringInterceptorEnabled(final boolean enable) {
		enableMonitoringInterceptor = enable;
		return this;
	}
	

	/**
	 * Sets the application id
	 * @param applicationId the applicationId to set
	 * @return this builder
	 */
	public StreamsConfigBuilder setApplicationId(final String applicationId) {
		if(applicationId==null || applicationId.trim().isEmpty()) throw new IllegalArgumentException("The passed applicationId was null or empty");
		this.applicationId = applicationId;
		return this;
	}

	/**
	 * Sets the key serializer/deserializer
	 * @param keySerde the keySerde to set
	 * @return this builder
	 */
	public StreamsConfigBuilder setKeySerde(final Serde<?> keySerde) {
		if(keySerde==null) throw new IllegalArgumentException("The passed Key Serde was null or empty");
		this.keySerde = keySerde;
		return this;
	}

	/**
	 * Sets the value serializer/deserializer
	 * @param valueSerde the valueSerde to set
	 * @return this builder
	 */
	public StreamsConfigBuilder setValueSerde(final Serde<?> valueSerde) {
		if(valueSerde==null) throw new IllegalArgumentException("The passed Value Serde was null or empty");
		this.valueSerde = valueSerde;
		return this;
	}

	/**
	 * Sets the zookeeper connect string
	 * @param zookeeperConnect the zookeeperConnect to set
	 * @return this builder
	 */
	public StreamsConfigBuilder setZookeeperConnect(final String zookeeperConnect) {
		if(zookeeperConnect==null) throw new IllegalArgumentException("The passed zookeeper connect string was null or empty");
		this.zookeeperConnect = zookeeperConnect;
		return this;
	}


	/**
	 * Sets the maximum number of buffered records per partition
	 * @param maxBufferedPerPartition the maxBufferedPerPartition to set
	 * @return this builder
	 */
	public StreamsConfigBuilder setMaxBufferedPerPartition(final int maxBufferedPerPartition) {
		this.maxBufferedPerPartition = maxBufferedPerPartition;
		return this;
	}

	/**
	 * Sets an id string to pass to the server when making requests. (This setting is passed to the consumer/producer clients used internally by Kafka Streams.)
	 * @param clientId the clientId to set
	 * @return this builder
	 */
	public StreamsConfigBuilder setClientId(final String clientId) {
		if(clientId==null) throw new IllegalArgumentException("The passed clientId was null or empty");
		this.clientId = clientId;
		return this;
	}

	/**
	 * Sets the frequency with which to save the position (offsets in source topics) of tasks
	 * @param commitIntervalMs the commitIntervalMs to set
	 * @return this builder
	 */
	public StreamsConfigBuilder setCommitIntervalMs(final long commitIntervalMs) {
		this.commitIntervalMs = commitIntervalMs;
		return this;
	}

	/**
	 * Sets a list of metric reporting class names
	 * @param metricReportingClasses the metricReportingClasses to set
	 * @return this builder
	 */
	public StreamsConfigBuilder setMetricReportingClasses(final List<String> metricReportingClasses) {
		if(metricReportingClasses != null && !metricReportingClasses.isEmpty()) {
			for(String s: metricReportingClasses) {
				if(s==null || s.trim().isEmpty()) continue;
				this.metricReportingClasses.add(s.trim());
			}
		}
		return this;
	}

	/**
	 * Sets the number of standby replicas for each task
	 * @param standbyReplicas the standbyReplicas to set
	 * @return this builder
	 */
	public StreamsConfigBuilder setStandbyReplicas(final int standbyReplicas) {
		this.standbyReplicas = standbyReplicas;
		return this;
	}

	/**
	 * Sets the number of threads to execute stream processing 
	 * @param streamThreads the streamThreads to set
	 * @return this builder
	 */
	public StreamsConfigBuilder setStreamThreads(final int streamThreads) {
		this.streamThreads = streamThreads;
		return this;
	}

	/**
	 * Sets the partition grouper that implements the {@link PartitionGrouper} interface
	 * @param partitionGrouperClassName the partitionGrouper class name to set
	 * @return this builder
	 */
	public StreamsConfigBuilder setPartitionGrouper(final String partitionGrouperClassName) {
		if(partitionGrouperClassName==null) throw new IllegalArgumentException("The passed partition grouper was null");
		this.partitionGrouper = partitionGrouperClassName;
		return this;
	}

	/**
	 * Sets the amount of time in milliseconds to block waiting for input
	 * @param pollWaitMs the pollWaitMs to set
	 * @return this builder
	 */
	public StreamsConfigBuilder setPollWaitMs(final long pollWaitMs) {
		this.pollWaitMs = pollWaitMs;
		return this;
	}

	/**
	 * Sets the replication factor for changelog topics and repartition topics created by the application
	 * @param replicationFactor the replicationFactor to set
	 * @return this builder
	 */
	public StreamsConfigBuilder setReplicationFactor(final int replicationFactor) {
		this.replicationFactor = replicationFactor;
		return this;
	}

	/**
	 * Sets the amount of time in milliseconds to wait before deleting state when a partition has migrated
	 * @param stateCleanupDelayMs the stateCleanupDelayMs to set
	 * @return this builder
	 */
	public StreamsConfigBuilder setStateCleanupDelayMs(final long stateCleanupDelayMs) {
		this.stateCleanupDelayMs = stateCleanupDelayMs;
		return this;
	}

	/**
	 * Sets the directory location for state stores
	 * @param stateStoreDir the stateStoreDir to set
	 * @return this builder
	 */
	public StreamsConfigBuilder setStateStoreDir(final File stateStoreDir) {
		if(stateStoreDir==null) throw new IllegalArgumentException("The passed state store directory was null");
		this.stateStoreDir = stateStoreDir;
		return this;
	}

	/**
	 * Sets the Timestamp extractor class that implements the {@link TimestampExtractor} interface
	 * @param timeExtractorClassName the timeExtractor class name to set
	 * @return this builder
	 */
	public StreamsConfigBuilder setTimeExtractor(final String timeExtractorClassName) {
		this.timeExtractor = timeExtractorClassName;
		return this;
	}

	/**
	 * Sets the number of samples maintained to compute metrics
	 * @param metricSampleCount the metricSampleCount to set
	 * @return this builder
	 */
	public StreamsConfigBuilder setMetricSampleCount(final int metricSampleCount) {
		this.metricSampleCount = metricSampleCount;
		return this;
	}

	/**
	 * Sets the window of time a metrics sample is computed over in ms.
	 * @param metricSampleWindow the metricSampleWindow to set
	 * @return this builder
	 */
	public StreamsConfigBuilder setMetricSampleWindow(final long metricSampleWindow) {
		this.metricSampleWindow = metricSampleWindow;
		return this;
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("StreamsConfigBuilder [");
		if (bootstrapServers != null) {
			builder.append("bootstrapServers=");
			builder.append(bootstrapServers);
			builder.append(", ");
		}
		if (applicationId != null) {
			builder.append("applicationId=");
			builder.append(applicationId);
			builder.append(", ");
		}
		if (zookeeperConnect != null) {
			builder.append("zookeeperConnect=");
			builder.append(zookeeperConnect);
			builder.append(", ");
		}
		if (keySerde != null) {
			builder.append("keySerde=");
			builder.append(keySerde);
			builder.append(", ");
		}
		if (valueSerde != null) {
			builder.append("valueSerde=");
			builder.append(valueSerde);
			builder.append(", ");
		}
		builder.append("maxBufferedPerPartition=");
		builder.append(maxBufferedPerPartition);
		builder.append(", ");
		if (clientId != null) {
			builder.append("clientId=");
			builder.append(clientId);
			builder.append(", ");
		}
		builder.append("commitIntervalMs=");
		builder.append(commitIntervalMs);
		builder.append(", ");
		if (metricReportingClasses != null) {
			builder.append("metricReportingClasses=");
			builder.append(metricReportingClasses);
			builder.append(", ");
		}
		builder.append("standbyReplicas=");
		builder.append(standbyReplicas);
		builder.append(", streamThreads=");
		builder.append(streamThreads);
		builder.append(", ");
		if (partitionGrouper != null) {
			builder.append("partitionGrouper=");
			builder.append(partitionGrouper);
			builder.append(", ");
		}
		builder.append("pollWaitMs=");
		builder.append(pollWaitMs);
		builder.append(", replicationFactor=");
		builder.append(replicationFactor);
		builder.append(", stateCleanupDelayMs=");
		builder.append(stateCleanupDelayMs);
		builder.append(", ");
		if (stateStoreDir != null) {
			builder.append("stateStoreDir=");
			builder.append(stateStoreDir);
			builder.append(", ");
		}
		if (timeExtractor != null) {
			builder.append("timeExtractor=");
			builder.append(timeExtractor);
			builder.append(", ");
		}
		builder.append("metricSampleCount=");
		builder.append(metricSampleCount);
		builder.append(", metricSampleWindow=");
		builder.append(metricSampleWindow);
		builder.append("]");
		return builder.toString();
	}
	
	

}
