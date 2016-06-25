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
package com.heliosapm.streams.metrics.router.text;

import static com.heliosapm.streams.metrics.Utils.EMPTY_STR_ARR;
import static com.heliosapm.streams.metrics.Utils.getArrayProperty;

import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.StreamedMetricValue;

/**
 * <p>Title: TextMetricStreamRouter</p>
 * <p>Description: Subscribes to topics that supply {@link StreamedMetric}s in string format,
 * transforms them to {@link StreamedMetric}s in binary format and publishes them to binary metric endpooints.</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.router.text.TextMetricStreamRouter</code></p>
 */

public class TextMetricStreamRouter implements ProcessorSupplier<String, String>, Processor<String, String>, InitializingBean, DisposableBean, BeanNameAware {
	
	/** The configuration key for the names of the topics to listen on */
	public static final String CONFIG_TOPICS = "processor.topics";
	/** The configuration key for the names of the state stores the processor will use */
	public static final String CONFIG_STORES = "processor.stores";
	
	/** Instance logger */
	protected Logger log = LogManager.getLogger(getClass());
	/** The bean name */
	protected String beanName = null;
	/** The streams configuration properties */
	protected final Properties config;
	/** The streams config */
	protected final StreamsConfig streamsConfig;
	/** The names of the topics to listen on */
	protected final String[] listenTopics;
	/** The names of the state stores used by the processor */
	protected final String[] stateStoreNames;
	
	/** The value type metric router */
//	protected ValueTypeMetricRouter router = null;
	
	/** The processor context */
	protected ProcessorContext context = null;
	/** A string serializer */
	protected final Serializer<String> stringSerializer = new StringSerializer();
	/** A string deserializer */
	protected final Deserializer<String> stringDeserializer = new StringDeserializer();
	/** The incoming text line stream */
	protected final KStream<String, String> textMetricsIn;
	/** The outgoinf binary metrics stream */
	protected final KStream<String, StreamedMetricValue> binaryMetricsOut;
	
	/** The string serializer/deserializer */
	protected final Serde<String> stringSerde = Serdes.String();
	
	/** The streams instance handling the continuous input */
	protected final KafkaStreams kafkaStreams;
	 
	/**
	 * Creates a new TextMetricStreamRouter
	 */
	public TextMetricStreamRouter(final Properties config) {
		if(config == null || config.isEmpty()) throw new IllegalArgumentException("The passed config properties was null or empty");		
		this.config = config;
		listenTopics = getArrayProperty(config, CONFIG_TOPICS, EMPTY_STR_ARR);
		if(listenTopics.length==0) throw new IllegalArgumentException("No topic names found in the passed config properties");
		stateStoreNames = getArrayProperty(config, CONFIG_STORES, EMPTY_STR_ARR);
		for(int i = 0; i < listenTopics.length; i++) {
			if(listenTopics[i]==null || listenTopics[i].trim().isEmpty()) throw new IllegalArgumentException("The passed topic array " + Arrays.toString(listenTopics) + " had null or empty entries");
		}
		log.info("Starting TextMetricStreamRouter for topics {}", listenTopics);
		streamsConfig = new StreamsConfig(this.config);
		
		final KStreamBuilder builder = new KStreamBuilder();
		final StreamedMetric SM = null;
		final StreamedMetricValue SMV = null;
		textMetricsIn = null; 
		KStream<String, StreamedMetricValue> metricStream = 
				builder.stream(stringSerde, stringSerde, listenTopics)
			.map(new KeyValueMapper<String, String, KeyValue<String, StreamedMetricValue>>() {
				/**
				 * {@inheritDoc}
				 * @see org.apache.kafka.streams.kstream.KeyValueMapper#apply(java.lang.Object, java.lang.Object)
				 */
				@Override
				public KeyValue<String, StreamedMetricValue> apply(String key, String value) {
					final StreamedMetricValue smv = StreamedMetric.fromString(value).forValue(1L);
					return new KeyValue<String, StreamedMetricValue>(smv.metricKey(), smv);
				}
			});
		
		
		
		
			
		binaryMetricsOut = null;
//		binaryMetricsOut = textMetricsIn.map((key, value) -> )
//		textLinesStream.process(this, stateStoreNames);
		
		kafkaStreams = new KafkaStreams(builder, config);
		
		
		final TopologyBuilder topBuilder = new TopologyBuilder();
		topBuilder
			.addSource("TextMetricStreamRouter", listenTopics);
			
		
		
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.springframework.beans.factory.BeanNameAware#setBeanName(java.lang.String)
	 */
	@Override
	public void setBeanName(final String name) {
		this.beanName = name;
		
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() throws Exception {
		log.info("\n\t=================================================\n\tStarting {} ...\n\t=================================================", beanName);
		kafkaStreams.start(); 		
		log.info("\n\t=================================================\n\t{} Started\n\t=================================================", beanName);
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.springframework.beans.factory.DisposableBean#destroy()
	 */
	@Override
	public void destroy() throws Exception {
		log.info("\n\t=================================================\n\tStopping {} ...\n\t=================================================", beanName);
		kafkaStreams.close();
		close();		
		log.info("\n\t=================================================\n\t{} Stopped\n\t=================================================", beanName);
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.processor.Processor#init(org.apache.kafka.streams.processor.ProcessorContext)
	 */
	@Override
	public void init(final ProcessorContext context) {
		this.context = context;
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.processor.Processor#process(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void process(final String key, final String value) {
		if(log.isDebugEnabled()) log.debug("Processing TextLine [{}]: {}", key, value);
		
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.processor.Processor#punctuate(long)
	 */
	@Override
	public void punctuate(final long timestamp) {
		/* No Op ? */
		log.info("Punctuate: {}", new Date(timestamp));
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.processor.Processor#close()
	 */
	@Override
	public void close() {
		/* No Op ? */		
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.processor.ProcessorSupplier#get()
	 */
	@Override
	public Processor<String, String> get() {		
		return this;
	}

}
