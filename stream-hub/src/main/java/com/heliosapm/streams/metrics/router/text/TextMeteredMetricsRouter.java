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

import java.lang.Thread.UncaughtExceptionHandler;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.router.config.StreamsConfigBuilder;
import com.heliosapm.streams.metrics.router.util.StreamedMetricLongSumReducer;
import com.heliosapm.streams.serialization.HeliosSerdes;
import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.jmx.JMXHelper;

/**
 * <p>Title: TextMeteredMetricsRouter</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.router.text.TextMeteredMetricsRouter</code></p>
 */

public class TextMeteredMetricsRouter implements UncaughtExceptionHandler {
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
		JMXHelper.fireUpJMXMPServer(1234);
		StreamsConfigBuilder config = new StreamsConfigBuilder();
		config.setApplicationId("TextMeteredMetricsRouter");
		config.setBootstrapServers("localhost:9092");
		config.setMonitoringInterceptorEnabled(true);
		config.setValueSerde(HeliosSerdes.STREAMED_METRIC_SERDE_THROUGH_STRING);
		config.setKeySerde(HeliosSerdes.STRING_SERDE);
		config.setTimeExtractor("com.heliosapm.streams.metrics.StreamedMetricTimestampExtractor");
		
		final TextMeteredMetricsRouter router = new TextMeteredMetricsRouter();
		router.setConfig(config);
		router.start();
		StdInCommandHandler.getInstance().registerCommand("stop", new Runnable() {
			public void run() {
				router.streams.close();
				System.err.println("Exiting ....");
			}
		})
		.run();
	}
	
	protected final StreamedMetricLongSumReducer sumReducer = new StreamedMetricLongSumReducer();

	public void start() {
		log.info("Starting TextMetered Router...");
		final StreamsConfig sc = config.build();
		builder.stream(HeliosSerdes.STRING_SERDE, HeliosSerdes.STRING_SERDE, "tsdb.metrics.text.meter")
				.map(new KeyValueMapper<String, String, KeyValue<String, StreamedMetric>>(){
					@Override
					public KeyValue<String, StreamedMetric> apply(String key, String value) {
						final StreamedMetric sm = StreamedMetric.fromString(value);
						return new KeyValue<String, StreamedMetric>(sm.metricKey(), sm);
					}

				}).to(HeliosSerdes.STRING_SERDE, HeliosSerdes.STREAMED_METRIC_SERDE, "tsdb.metrics.meter");
		
		
		
		KTable<Windowed<String>, StreamedMetric> meteredWindow = 
			builder.stream(HeliosSerdes.STRING_SERDE, HeliosSerdes.STREAMED_METRIC_SERDE, "tsdb.metrics.meter")
			.reduceByKey(sumReducer, TimeWindows.of("MeteringWindowAccumulator", 5000), HeliosSerdes.STRING_SERDE, HeliosSerdes.STREAMED_METRIC_SERDE);
		
		meteredWindow.toStream()
		.map(new KeyValueMapper<Windowed<String>, StreamedMetric, KeyValue<String,StreamedMetric>>() {
			@Override
			public KeyValue<String, StreamedMetric> apply(final Windowed<String> key, final StreamedMetric sm) {
				return new KeyValue<String, StreamedMetric>(sm.metricKey(), sm.forValue(1L).update(key.window().end()));
			}
		}).to(HeliosSerdes.STRING_SERDE, HeliosSerdes.STREAMED_METRIC_SERDE, "tsdb.metrics.binary");
		
		builder.addStateStore(new StateStoreSupplier(){
			@Override
			public StateStore get() {				
				return Stores.create("MeteringWindowAccumulator")
					.withKeys(HeliosSerdes.STRING_SERDE)
					.withValues(HeliosSerdes.STREAMED_METRIC_SERDE)
					.inMemory()
					.build().get();
			}

			@Override
			public String name() {
				return "MeteringWindowAccumulatorSupplier";
			}
		});
		
		builder.connectProcessorAndStateStores("KSTREAM-MAP-0000000001", "MeteringWindowAccumulator");
		
		
		//KSTREAM-MAP-0000000001
		
//		
//		final KStream<String, Long> smstream = builder.stream(HeliosSerdes.STRING_SERDE, HeliosSerdes.STREAMED_METRIC_SERDE, "tsdb.metrics.meter")
//			.map(new KeyValueMapper<String, StreamedMetric, KeyValue<String, Long>>(){
//				@Override
//				public KeyValue<String, Long> apply(final String key, final StreamedMetric sm) {					
//					return new KeyValue<String, Long>(sm.metricKey(), sm.forValue(1L).getLongValue());
//				}
//			});
//		
//		final Initializer<Long> accumInit = new Initializer<Long>() {
//			public Long apply() { return 0L; }
//		};
//		
//		
//		KTable<Windowed<String>, Long> timeWindowSum = smstream
//				.aggregateByKey(accumInit, new Aggregator<String, Long, Long>(){
//					/**
//					 * {@inheritDoc}
//					 * @see org.apache.kafka.streams.kstream.Aggregator#apply(java.lang.Object, java.lang.Object, java.lang.Object)
//					 */
//					@Override
//					public Long apply(final String aggKey, final Long value, final Long aggregate) {
//						return aggregate + value;
//					}
//				}, TimeWindows.of("MeteringWindowAccumulator", 5000), HeliosSerdes.STRING_SERDE, HeliosSerdes.LONG_SERDE);
		
		
				
			
			//			.toStream(new KeyValueMapper<Windowed<String>, Long, KeyValue<String, Long>>() {
//				/**
//				 * {@inheritDoc}
//				 * @see org.apache.kafka.streams.kstream.KeyValueMapper#apply(java.lang.Object, java.lang.Object)
//				 */
//				@Override
//				public KeyValue<String, Long> apply(Windowed<String> key, Long value) {
//					// TODO Auto-generated method stub
//					return null;
//				}
//			})
			
			
			
//			.reduceByKey(new Reducer<KeyValue<String, Long>>(){
//				/**
//				 * {@inheritDoc}
//				 * @see org.apache.kafka.streams.kstream.Reducer#apply(java.lang.Object, java.lang.Object)
//				 */
//				@Override
//				public KeyValue<String, Long> apply(KeyValue<String, Long> value1, KeyValue<String, Long> value2) {
//					// TODO Auto-generated method stub
//					return null;
//				}
//			}, TimeWindows.of("MeterAccumlator", 5000))

		
		streams = new KafkaStreams(builder, sc);
	    streams.start();
	    log.info("TextMetered Router Started.");
	}
	
	public void setConfig(final StreamsConfigBuilder config) {
		this.config = config;
	}
	
	public static double calcRate(final double windowSize, final double count) {
		if(count==0D || windowSize==0D) return 0D;
		return count/windowSize;
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Thread.UncaughtExceptionHandler#uncaughtException(java.lang.Thread, java.lang.Throwable)
	 */
	@Override
	public void uncaughtException(final Thread t, final Throwable e) {
		log.error("Uncaught exception on thread [{}]", t, e);
		
	}
	
}
