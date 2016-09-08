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
package com.heliosapm.streams.metrics.router.nodes;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.StreamedMetricValue;
import com.heliosapm.streams.serialization.HeliosSerdes;
import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.tuples.NVP;


/**
 * <p>Title: KMetricAggreagator</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.router.nodes.KMetricAggreagator</code></p>
 */

public class KMetricAggreagator {
	static final Logger log = LogManager.getLogger(KMetricAggreagator.class);
	/**
	 * Creates a new KMetricAggreagator
	 */
	public KMetricAggreagator() {
		// TODO Auto-generated constructor stub
	}
	
	protected static final NonBlockingHashMap<String, NVP<Window, StreamedMetricValue>> lastEntry = new NonBlockingHashMap<String, NVP<Window, StreamedMetricValue>>(); 

	protected static final List<KeyValue<String, StreamedMetricValue>> EMPTY_KVS = Collections.unmodifiableList(Collections.emptyList());
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		log.info("KMetricAggreagator Test");
		  Properties streamsConfiguration = new Properties();
		    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
		    // against which the application is run.
		    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "KMetricAggreagator");
		    // Where to find Kafka broker(s).
		    //streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "pdk-pt-cltsdb-02.intcx.net:9092,pdk-pt-cltsdb-04.intcx.net:9092,pdk-pt-cltsdb-03.intcx.net:9092");
		    //streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
		    //streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.5.202.251:9092,10.5.202.251:9093,10.5.202.251:9094");
		    //streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.22.114.37:9092,10.22.114.37:9093,10.22.114.37:9094");
		    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		    
		    // Where to find the corresponding ZooKeeper ensemble.
		    //streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "pdk-pt-cltsdb-02.intcx.net:2181,pdk-pt-cltsdb-04.intcx.net:2181,pdk-pt-cltsdb-03.intcx.net:2181");
		    streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
		    // Specify default (de)serializers for record keys and for record values.
		    streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		    streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		    streamsConfiguration.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "com.heliosapm.streams.metrics.StreamedMetricTimestampExtractor");
		    streamsConfiguration.put("auto.offset.reset", "earliest");
		    final AtomicLong currentWindow = new AtomicLong(0L);
		    
		    KStreamBuilder builder = new KStreamBuilder();
		    KStream<String, StreamedMetric> rawMetrics = builder.stream(HeliosSerdes.STRING_SERDE, HeliosSerdes.STREAMED_METRIC_SERDE, "tsdb.metrics.meter");
		    KTable<Windowed<String>, StreamedMetricValue> window = rawMetrics.aggregateByKey(new SMAggInit(), new SMAgg(), TimeWindows.of("StreamedMetricAggWindow", 10 * 1000L), HeliosSerdes.STRING_SERDE, HeliosSerdes.STREAMED_METRIC_VALUE_SERDE);
		    
		    window.toStream()
		  //NonBlockingHashMap<String, NVP<Window, StreamedMetricValue>>
		    .flatMap(new KeyValueMapper<Windowed<String>, StreamedMetricValue, Iterable<KeyValue<String,StreamedMetricValue>>>() {
		    	@Override
		    	public Iterable<KeyValue<String, StreamedMetricValue>> apply(final Windowed<String> key, final StreamedMetricValue value) {
		    		final NVP<Window, StreamedMetricValue> prior = lastEntry.put(key.key(), new NVP<Window, StreamedMetricValue>(key.window(), value));
		    		if(prior!=null && !prior.getKey().equals(key.window())) {
		    			return Collections.singletonList(new KeyValue<String, StreamedMetricValue>(key.key(), prior.getValue()));
		    		}
		    		return EMPTY_KVS;
		    	}
			}).foreach((k,v) -> log.info("W: {}",v));
		    
//		    .foreach((w, smv) -> {
//		    	final long winStart = w.window().start();
//		    	log.info("W: [{}]:{} -- [{}]", new Date(winStart), smv.getValueNumber(), smv.metricKey());
//		    })
		    ;
		    
		    //lastEntry = new NonBlockingHashMap<String, Window>();
		    
//		    .foreach((w, smv) -> {
//		    	final long winStart = w.window().start();
//		    	log.info("W: [{}]:{}", new Date(winStart), smv.getValueNumber());
//		    	if(!currentWindow.compareAndSet(0L, winStart)) {
//		    		
//		    	}
//		    	final long prior = currentWindow.get();
//		    	if(prior==0L) {
//		    		
//		    	}
//		    	final long win = currentWindow.getAndSet(w.window().start());
//		    	final long prior = currentWindow.get();
//		    	if(win!=0L && win!=prior) {
//		    		//log.info("W: [{}]:[{}] ---> [{}]", w.key(), w.window().start(), smv.toString());
//		    		log.info("W: [{}]", win);
//		    	}
//		    });
		    
		    KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
		    streams.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
				@Override
				public void uncaughtException(final Thread t, final Throwable e) {
					log.error("Uncaught exception on [{}]",  t, e);
					
				}
			});
		    streams.start();

		    StdInCommandHandler.getInstance().registerCommand("stop", new Runnable(){
		    	@Override
		    	public void run() {
		    		log.info("\n\tSTOPPING....");
		    		try { streams.close(); } catch (Exception x) {/* No Op */}
		    		System.exit(0);
		    	}
		    }).run();
		    
		    
		    
		    

	}
	
    static class SMAggInit implements Initializer<StreamedMetricValue> {
    @Override
    	public StreamedMetricValue apply() {
    		return null;
    	}	
    }
    
    
    static class SMAgg implements org.apache.kafka.streams.kstream.Aggregator<String, StreamedMetric, StreamedMetricValue> {
		@Override
		public StreamedMetricValue apply(final String aggKey, final StreamedMetric value, final StreamedMetricValue aggregate) {
			if(aggregate==null) {
//				log.info("Initializing [{}]", aggKey);
				return value.forValue(1L);
			}
			return aggregate.forceToLong().increment(
					value.forValue(1L).getValueNumber().longValue()
			);
		}
    	
    }
	

}
