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
import java.util.Map;
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
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.StreamedMetricDeserializer;
import com.heliosapm.streams.metrics.StreamedMetricSerializer;
import com.heliosapm.streams.metrics.ValueType;
import com.heliosapm.streams.metrics.processor.StreamedMetricProcessor;
import com.heliosapm.streams.metrics.router.DefaultValueTypeMetricRouter;
import com.heliosapm.streams.metrics.router.ValueTypeMetricRouter;
import com.heliosapm.streams.metrics.router.config.StreamsConfigBuilder;
import com.heliosapm.utils.io.StdInCommandHandler;

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
	
	/** The value type metric router */
	protected ValueTypeMetricRouter router = new DefaultValueTypeMetricRouter();
	
	/** The processor context */
	protected ProcessorContext context = null;
	/** A string serializer */
	protected final Serializer<String> stringSerializer = new StringSerializer();
	/** A string deserializer */
	protected final Deserializer<String> stringDeserializer = new StringDeserializer();
	/** The incoming text line stream */
	final KStream<String, StreamedMetric> metricStream;
	
	/** The string serializer/deserializer */
	protected final Serde<String> stringSerde = Serdes.String();
	/** The StreamedMetric serializer/deserializer */
	protected final Serde<StreamedMetric> metricSerde = new Serde<StreamedMetric>() {
		final StreamedMetricSerializer ser = new StreamedMetricSerializer();
		final StreamedMetricDeserializer de = new StreamedMetricDeserializer();
		@Override
		public Deserializer<StreamedMetric> deserializer() {
			return de;
		}
		@Override
		public Serializer<StreamedMetric> serializer() {			
			return ser;
		}
		@Override
		public void close() {
			/* No Op */			
		}
		@Override
		public void configure(final Map<String, ?> configs, final boolean isKey) {
			/* No Op */			
		}
	};
	
	
	/** The streams instance handling the continuous input */
	protected final KafkaStreams kafkaStreams;
	
	public static void main(String[] args) {
		final StreamsConfigBuilder conf = new StreamsConfigBuilder();
		conf.setBootstrapServers("localhost:9093", "localhost:9094");
		final Properties p = conf.buildProperties();
		p.put("processor.topics", "tsdb.metrics.accumulator,tsdb.metrics.st");
		final TextMetricStreamRouter router = new TextMetricStreamRouter(p);
		router.setBeanName("Hallo");
		try {
			router.afterPropertiesSet();
			StdInCommandHandler.getInstance().registerCommand("stop", new Runnable(){
				public void run() {
					try {
						router.destroy();
					} catch (Exception ex) {
						ex.printStackTrace(System.err);
					} finally {
						Runtime.getRuntime().halt(1);
					}
				}
			});
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			Runtime.getRuntime().halt(-1);
		}
		
		
	}
	 
	/**
	 * Creates a new TextMetricStreamRouter
	 */
	public TextMetricStreamRouter(final Properties config) {
		if(config == null || config.isEmpty()) throw new IllegalArgumentException("The passed config properties was null or empty");		
		this.config = config;
		listenTopics = getArrayProperty(config, CONFIG_TOPICS, EMPTY_STR_ARR);
		if(listenTopics.length==0) throw new IllegalArgumentException("No topic names found in the passed config properties");
		
		for(int i = 0; i < listenTopics.length; i++) {
			if(listenTopics[i]==null || listenTopics[i].trim().isEmpty()) throw new IllegalArgumentException("The passed topic array " + Arrays.toString(listenTopics) + " had null or empty entries");
		}
		log.info("Starting TextMetricStreamRouter for topics [{}]", listenTopics);
		streamsConfig = new StreamsConfig(this.config);
		final KStreamBuilder builder = new KStreamBuilder();
		metricStream = null;
		for(ValueType v: ValueType.values()) {
			final StreamedMetricProcessor p = router.route(v);
			if(p!=null) {
				for(StateStoreSupplier ss: p.getStateStores()) {
					builder.addStateStore(ss);
				}
				k.to(stringSerde, metricSerde, p.getSink());
			}
		}
		
		builder.stream(stringSerde, stringSerde, listenTopics)
			.foreach(new ForeachAction<String, String>() {
				@Override
				public void apply(final String key, final String value) {
					StreamedMetric sm = StreamedMetric.fromString(value);
					final StreamedMetricProcessor p = router.route(sm.getValueType());
					p.process(key, sm);					
				}
			});
			
//			.map(new KeyValueMapper<String, String, KeyValue<String, StreamedMetric>>() {
//				/**
//				 * {@inheritDoc}
//				 * @see org.apache.kafka.streams.kstream.KeyValueMapper#apply(java.lang.Object, java.lang.Object)
//				 */
//				@Override
//				public KeyValue<String, StreamedMetric> apply(String key, String value) {
//					final StreamedMetric smv = StreamedMetric.fromString(value);
//					return new KeyValue<String, StreamedMetric>(smv.metricKey(), smv);
//				}
//		});
		
//		final KStreamBuilder builder = new KStreamBuilder();
//		metricStream = 
//			builder.stream(stringSerde, stringSerde, listenTopics)
//			.map(new KeyValueMapper<String, String, KeyValue<String, StreamedMetric>>() {
//				/**
//				 * {@inheritDoc}
//				 * @see org.apache.kafka.streams.kstream.KeyValueMapper#apply(java.lang.Object, java.lang.Object)
//				 */
//				@Override
//				public KeyValue<String, StreamedMetric> apply(String key, String value) {
//					final StreamedMetric smv = StreamedMetric.fromString(value);
//					return new KeyValue<String, StreamedMetric>(smv.metricKey(), smv);
//				}
//		});
//		for(ValueType v: ValueType.values()) {
//			final StreamedMetricProcessor p = router.route(v);
//			if(p!=null) {
//				for(StateStoreSupplier ss: p.getStateStores()) {
//					builder.addStateStore(ss);
//				}
//				KStream<String, StreamedMetric> k = metricStream.filter(p);				
//				k.process(p, p.getDataStoreNames());
//				k.to(stringSerde, metricSerde, p.getSink());
//			}
//		}
		kafkaStreams = new KafkaStreams(builder, config);
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
