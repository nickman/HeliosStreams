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
package com.heliosapm.streams.metrics.router.text;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.router.config.StreamsConfigBuilder;
import com.heliosapm.streams.serialization.HeliosSerdes;
import com.heliosapm.utils.io.StdInCommandHandler;

/**
 * <p>Title: TextMeteredMetricsRouter</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.router.text.TextMeteredMetricsRouter</code></p>
 */

public class TextMeteredMetricsRouter {
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** The Streams config builder */
	protected StreamsConfigBuilder config = null;
	/** The stream builder */
	protected final KStreamBuilder builder = new KStreamBuilder();
	/** The stream engine */
	protected KafkaStreams streams = null; 

	/**
	 * Creates a new TextMeteredMetricsRouter
	 */
	public TextMeteredMetricsRouter() {

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		StreamsConfigBuilder config = new StreamsConfigBuilder();
		config.setApplicationId("TextMeteredMetricsRouter");
		config.setBootstrapServers("localhost:9092");
		config.setValueSerde(HeliosSerdes.STREAMED_METRIC_SERDE_THROUGH_STRING);
		config.setKeySerde(HeliosSerdes.STRING_SERDE);
		final TextMeteredMetricsRouter router = new TextMeteredMetricsRouter();
		router.setConfig(config);
		router.start();
		StdInCommandHandler.getInstance().registerCommand("stop", new Runnable() {
			public void run() {
				router.streams.close();
			}
		})
		.run();
	}

	public void start() {
		final StreamsConfig sc = config.build();
		KStream<String, StreamedMetric> kstr = builder.stream("tsdb.metrics.text.meter");
		kstr.mapValues(metric -> new KeyValue<String, StreamedMetric>(metric.getMetricName(), metric));
		kstr.to("tsdb.metrics.meter");			
		KafkaStreams streams = new KafkaStreams(builder, sc);
	    streams.start();
	}
	
	public void setConfig(StreamsConfigBuilder config) {
		this.config = config;
	}
	
}
