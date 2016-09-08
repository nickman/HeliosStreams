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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
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
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.cliffc.high_scale_lib.NonBlockingHashSet;
import org.springframework.jmx.export.annotation.ManagedAttribute;

import com.heliosapm.streams.common.kafka.interceptor.SwitchableMonitoringInterceptor;
import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.StreamedMetricValue;
import com.heliosapm.streams.metrics.router.StreamHubKafkaClientSupplier;
import com.heliosapm.streams.serialization.HeliosSerdes;
import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.tuples.NVP;


/**
 * <p>Title: KMetricAggreagator</p>
 * <p>Description: Aggregates metered metrics into windows of a defined period</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.router.nodes.KMetricAggreagator</code></p>
 */

public class KMetricAggreagator extends AbstractMetricStreamNode implements Runnable   {
	
	/** The aggregation window duration */
	protected long windowDuration = 1000 * 5;
	/** The rate divisor to get TPS */
	protected double rateDivisor = TimeUnit.MILLISECONDS.toSeconds(windowDuration);
	/** The idle time during which we send zero values for formerly active metrics */
	protected long idleDuration = 1000 * 60 * 5;
	/** The aggregation table */
	protected KTable<Windowed<String>, StreamedMetricValue> window = null;
	/** The last delivered metric by key */
	protected final Set<NonBlockingHashMap<String, NVP<Window, StreamedMetricValue>>> lastEntries = new NonBlockingHashSet<NonBlockingHashMap<String, NVP<Window, StreamedMetricValue>>>();	
	/** The producer used to send messages from the background tasks */
	protected Producer<String, StreamedMetricValue> producer = null;
	
	/** The background task scheduler */
	protected ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2, new ThreadFactory(){
		final AtomicInteger serial = new AtomicInteger();
		@Override
		public Thread newThread(final Runnable r) {
			final Thread t = new Thread(r, "KMetricAggregatorScheduler#" + serial.incrementAndGet());
			t.setDaemon(true);
			return t;
		}
	});
	
	
	
	/** The store name */
	public static final String STORE_NAME = "StreamedMetricAggWindow";
	/** The internal topic name where idle metrics are published */
	public static final String IDLE_TOPIC_NAME = "__idle_metrics";
	
	/** An empty key/value list const */
	public static final List<KeyValue<String, StreamedMetricValue>> EMPTY_KVS = Collections.unmodifiableList(Collections.emptyList());
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		JMXHelper.fireUpJMXMPServer(1423);
		System.setProperty("streams.debug", "true");
		Properties streamsConfiguration = new Properties();
	    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "KMetricAggreagatorX");
	    streamsConfiguration.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, SwitchableMonitoringInterceptor.class.getName());
	    //streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "pdk-pt-cltsdb-02.intcx.net:9092,pdk-pt-cltsdb-04.intcx.net:9092,pdk-pt-cltsdb-03.intcx.net:9092");
	    //streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
	    //streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.5.202.251:9092,10.5.202.251:9093,10.5.202.251:9094");
	    //streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.22.114.37:9092,10.22.114.37:9093,10.22.114.37:9094");
	    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
	    //streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "pdk-pt-cltsdb-02.intcx.net:2181,pdk-pt-cltsdb-04.intcx.net:2181,pdk-pt-cltsdb-03.intcx.net:2181");
	    streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
	    
	    streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);
	    
	    // Specify default (de)serializers for record keys and for record values.
	    streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
	    streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
	    streamsConfiguration.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "com.heliosapm.streams.metrics.StreamedMetricTimestampExtractor");
	    //streamsConfiguration.put("auto.offset.reset", "earliest");
	    KStreamBuilder builder = new KStreamBuilder();
	    KMetricAggreagator aggregator = new KMetricAggreagator();
	    aggregator.sinkTopic = "tsdb.metrics.binary";
	    aggregator.sourceTopics = new String[]{"tsdb.metrics.meter"};
	    
	    aggregator.configure(builder);
	    KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
	    streams.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
			@Override
			public void uncaughtException(final Thread t, final Throwable e) {
				System.err.println("Uncaught exception on [" + t + "]");
				e.printStackTrace(System.err);				
			}
		});
	    streams.start();
	    aggregator.onStart(new StreamHubKafkaClientSupplier(new StreamsConfig(streamsConfiguration), "KMetricAggreagatorX"), streams);
	    StdInCommandHandler.getInstance().registerCommand("stop", new Runnable(){
	    	@Override
	    	public void run() {
	    		System.err.println("\n\tSTOPPING....");
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
    
    
    class SMAgg implements org.apache.kafka.streams.kstream.Aggregator<String, StreamedMetric, StreamedMetricValue> {
		@Override
		public StreamedMetricValue apply(final String aggKey, final StreamedMetric value, final StreamedMetricValue aggregate) {
			if(aggregate==null) {
				//log.info("Initializing [{}]", aggKey);
				return value.forValue(1L);
			}
			return aggregate.forceToLong().increment(
					value.forValue(1L).getValueNumber().longValue()
			);
		}    	
    }
    
    @Override
    public void onStart(final StreamHubKafkaClientSupplier clientSupplier, final KafkaStreams kafkaStreams) {
    	super.onStart(clientSupplier, kafkaStreams);
    	producer = clientSupplier.getProducer(HeliosSerdes.STRING_SERDE, HeliosSerdes.STREAMED_METRIC_VALUE_SERDE);
    	scheduler.scheduleAtFixedRate(this, windowDuration, windowDuration, TimeUnit.MILLISECONDS);
    	log.info("Scheduled background reaper every [{}] ms.", windowDuration);
    }


	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.router.nodes.AbstractMetricStreamNode#close()
	 */
	@Override
	public void close() {
		super.close();	
		scheduler.shutdownNow();
		try { producer.close(); } catch (Exception x) {/* No Op */}
	}
	
	private NonBlockingHashMap<String, NVP<Window, StreamedMetricValue>> newlastEntry() {
		NonBlockingHashMap<String, NVP<Window, StreamedMetricValue>> acc = new NonBlockingHashMap<String, NVP<Window, StreamedMetricValue>>();
		lastEntries.add(acc);
		
		return acc;
	}


	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.router.nodes.MetricStreamNode#configure(org.apache.kafka.streams.kstream.KStreamBuilder)
	 */
	@Override
	public void configure(final KStreamBuilder streamBuilder) {
		rateDivisor = TimeUnit.MILLISECONDS.toSeconds(windowDuration);
	    KStream<String, StreamedMetric> rawMetrics = streamBuilder.stream(HeliosSerdes.STRING_SERDE, HeliosSerdes.STREAMED_METRIC_SERDE, sourceTopics);
	    window = rawMetrics.aggregateByKey(new SMAggInit(), new SMAgg(), TimeWindows.of(STORE_NAME, windowDuration), HeliosSerdes.STRING_SERDE, HeliosSerdes.STREAMED_METRIC_VALUE_SERDE);
	    window.toStream()
	    .flatMap(new KeyValueMapper<Windowed<String>, StreamedMetricValue, Iterable<KeyValue<String,StreamedMetricValue>>>() {
	    	protected final NonBlockingHashMap<String, NVP<Window, StreamedMetricValue>> lastEntry = newlastEntry();	    	
	    	@Override
	    	public Iterable<KeyValue<String, StreamedMetricValue>> apply(final Windowed<String> key, final StreamedMetricValue value) {
	    		final NVP<Window, StreamedMetricValue> prior = lastEntry.put(key.key(), new NVP<Window, StreamedMetricValue>(key.window(), value));
	    		if(prior!=null && !prior.getKey().equals(key.window())) {
	    			final StreamedMetricValue smv = prior.getValue();
//	    			log.info("[{}]-[{}]:{}", new Date(smv.getTimestamp()), smv.metricKey(), smv.getValueNumber());
	    			return Collections.singletonList(new KeyValue<String, StreamedMetricValue>(key.key(), adjust(smv, key.window())));
	    		}
	    		return EMPTY_KVS;
	    	}
		}).to(HeliosSerdes.STRING_SERDE, HeliosSerdes.STREAMED_METRIC_VALUE_SERDE, sinkTopic);
	    if(System.getProperties().containsKey("streams.debug")) {
		    streamBuilder.stream(HeliosSerdes.STRING_SERDE, HeliosSerdes.STREAMED_METRIC_SERDE, sinkTopic)
		    	.foreach((k,v) -> System.err.println("[" + new Date() + "<" + Thread.currentThread() + ">] WWWW: [" + new Date(v.getTimestamp()) + "] [" + v.metricKey() + "]:" + v.forValue(0D).getValueNumber()));
	    }
	    
	    
	}
	
	private StreamedMetricValue adjust(final StreamedMetricValue smv, final Window window) {
		return StreamedMetricValue.fromKey(window.end(), smv.metricKey(), calcTps(smv.getValueNumber().doubleValue(), rateDivisor));
	}
	
	private static double calcTps(final double count, final double time) {
		if(count==0D || time==0D) return 0D;
		return count/time;
	}
	
	/**
	 * <p>Runs the background task to send closing values, fill in zeroes, when the metric is idle and purges the last entry when they expire.</p>
	 * {@inheritDoc}
	 * @see java.lang.Runnable#run()
	 */
	public void run() {
		for(NonBlockingHashMap<String, NVP<Window, StreamedMetricValue>> lastEntry: lastEntries) {
			try {
				final long now = System.currentTimeMillis();
				for(Map.Entry<String, NVP<Window, StreamedMetricValue>> entry: lastEntry.entrySet()) {
					final String key = entry.getKey();
					final NVP<Window, StreamedMetricValue> value = entry.getValue();
					final Window win = value.getKey();
					final StreamedMetricValue smv = value.getValue();				
					final long diff = now - win.end(); 
					if(diff > windowDuration) {
						if(smv.getValueNumber().doubleValue() > 0D) {
							producer.send(new ProducerRecord<String, StreamedMetricValue>(sinkTopic, key, adjust(smv, win)));
							entry.setValue(new NVP<Window, StreamedMetricValue>(win, StreamedMetricValue.fromKey(now, smv.metricKey(), 0D)));
							log.debug("Sent closing value for [{}]", smv.metricKey());
							continue;
						}
						
						if(diff > idleDuration) {
							lastEntry.remove(key);
							log.info("Removed expired key [{}]", key);
						} else {
							producer.send(new ProducerRecord<String, StreamedMetricValue>(sinkTopic, key, StreamedMetricValue.fromKey(now, smv.metricKey(), 0L)));
							log.debug("Filling in for key [{}] until [{}]", key, new Date(win.end() + idleDuration));
						}
					}
				}
			} catch (Exception ex) {
				log.error("Background task failure", ex);
			}
		}
	}
	

	/**
	 * Returns the aggregation window duration in ms.
	 * @return the aggregation window duration in ms.
	 */
	@ManagedAttribute(description="The aggregation window duration in ms.")
	public long getWindowDuration() {
		return windowDuration;
	}

	/**
	 * Sets the aggregation window duration in ms.
	 * @param windowDuration the aggregation window duration in ms.
	 */
	public void setWindowDuration(final long windowDuration) {
		if(windowDuration < 0) throw new IllegalArgumentException("The aggregation window duration [" + windowDuration + "] is invalid");
		this.windowDuration = windowDuration;
	}

	/**
	 * Returns the idle duration in ms.
	 * @return the idle duration in ms.
	 */
	@ManagedAttribute(description="The idle duration in ms.")
	public long getIdleDuration() {
		return idleDuration;
	}

	/**
	 * Sets the time in ms. that a metric can be idle (with zeroes being sent every period)
	 * before it is purged from {@link #lastEntry}
	 * @param idleDuration the idle duration in ms.
	 */
	public void setIdleDuration(final long idleDuration) {
		if(idleDuration < 0) throw new IllegalArgumentException("The idle duration [" + idleDuration + "] is invalid");
		this.idleDuration = idleDuration;
	}

	/**
	 * Returns the number of windows in the last entry cache
	 * @return the number of windows in the last entry cache
	 */
	@ManagedAttribute(description="The number of windows in the last entry cache.")
	public int getStateEntryCount() {
		return lastEntries.stream().mapToInt(NonBlockingHashMap::size).sum();
	}

	/**
	 * Returns the number of accumulators created
	 * @return the number of accumulators created
	 */
	@ManagedAttribute(description="The the number of accumulators created.")
	public int getAccumulatorCount() {
		return lastEntries.size();
	}
	
}
