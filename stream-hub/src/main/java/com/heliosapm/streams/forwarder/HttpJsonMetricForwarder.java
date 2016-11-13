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
package com.heliosapm.streams.forwarder;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.management.ObjectName;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.springframework.beans.factory.BeanNameAware;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ApplicationContextEvent;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;

import com.heliosapm.streams.metrics.StreamedMetricValue;
import com.heliosapm.streams.serialization.HeliosSerdes;
import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.jmx.JMXManagedThreadPool;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4J2LoggerFactory;

/**
 * <p>Title: HttpJsonMetricForwarder</p>
 * <p>Description: Listens on a kafka topic for metrics and forwards them to an HTTP/JSON endpoint</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.forwarder.HttpJsonMetricForwarder</code></p>
 */

public class HttpJsonMetricForwarder extends ChannelInitializer<SocketChannel> implements BeanNameAware, ApplicationContextAware, ApplicationListener<ApplicationContextEvent>, ConsumerRebalanceListener, Runnable {
	/** The injected spring application context */
	protected ApplicationContext appCtx = null;
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());

	/** The netty client thread pool */
	protected JMXManagedThreadPool threadPool = null;
	/** The netty client event loop group */
	protected NioEventLoopGroup eventLoopGroup = null;
	/** The number of worker threads to allocate to the event loop group */
	protected int workerThreads = Runtime.getRuntime().availableProcessors() * 2;	
	/** The end point host to post metrics JSON to */
	protected String endpointHost = "localhost";
	/** The end point port to post metrics JSON to */
	protected int endpointPort = 8070;
	/** The end point http uri to post metrics JSON to */
	protected String endpointUri = "/api/put";
	/** The netty bootstrap */
	protected final Bootstrap bootstrap = new Bootstrap();
	/** The forwarder bean name */
	protected String beanName = "MetricForwarder";
	/** The outbound handler that converts the received kafka messages to an http post */
	protected HttpJsonOutboundHandler outboundHandler = null;
	/** The sender channel */
	protected Channel senderChannel = null;

	/** The kafka bootstrap servers */
	protected String kafkaBootstrapServers = "localhost:9093,localhost:9094";
	/** The kafka consumer group id */
	protected String kafkaGroupId = "HttpJsonMetricForwarder";
	/** The kafka consumer auto-commit enablement */
	protected boolean kafkaAutoCommit = true;
	/** The kafka consumer auto-commit interval in ms */
	protected int kafkaAutoCommitInterval = 1000;
	/** The kafka consumer session timeout in ms */
	protected int kafkaSessionTimeout = 30000;
	/** The kafka consumer max poll time */
	protected int kafkaMaxPollTime = 1000;
	/** The kafka consumer subscriber thread */
	protected Thread subscriberThread = null;
	/** The kafka consumer instance */
	protected KafkaConsumer<String, StreamedMetricValue> consumer = null;
	/** The kafka consumer properties */
	protected final Properties consumerProperties = new Properties();
	/** The kafka topics to subscribe to */
	protected String[] topicNames = {"tsdb.metrics.binary"};
	/** The flag indicating if the subscriber thread should keep running */
	protected final AtomicBoolean subThreadActive = new AtomicBoolean(false);

	/** The forwarder's JMX ObjectName */
	protected ObjectName OBJECT_NAME;
	/** The forwarder's thread pool JMX ObjectName */
	protected ObjectName EXECUTOR_OBJECT_NAME;
	
	
	
	/**
	 * Starts the forwarder
	 * @throws Exception thrown on any error
	 */
	public void start() throws Exception {
		log.info(">>>>> Starting HttpJsonMetricForwarder [{}]....", beanName);
		threadPool = new JMXManagedThreadPool(EXECUTOR_OBJECT_NAME, beanName, workerThreads, workerThreads * 2, 1, 60000, 100, 99, true);
		eventLoopGroup = new NioEventLoopGroup(workerThreads, (Executor)threadPool);
		outboundHandler = new HttpJsonOutboundHandler(1500, endpointHost, endpointUri);
		bootstrap.group(eventLoopGroup)
			.channel(NioSocketChannel.class)
			.handler(this)
			.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)   	// FIXME: config
			.option(ChannelOption.SO_SNDBUF, 64000)  				// FIXME: config
			.option(ChannelOption.SO_KEEPALIVE, true);
		senderChannel = bootstrap.connect(endpointHost, endpointPort).sync().channel();  // FIXME: short timeout, reconnect loop
		consumerProperties.put("bootstrap.servers", kafkaBootstrapServers);
		consumerProperties.put("group.id", kafkaGroupId);
		consumerProperties.put("enable.auto.commit", "" + kafkaAutoCommit);
		consumerProperties.put("auto.commit.interval.ms", "" + kafkaAutoCommitInterval);
		consumerProperties.put("session.timeout.ms", "" + 30000);
		consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumerProperties.put("value.deserializer", HeliosSerdes.STREAMED_METRIC_VALUE_DESER.getClass().getName());
		subscriberThread = new Thread(this, "KafkaSubscriberThread-" + beanName);
		subscriberThread.setDaemon(true);
		subThreadActive.set(true);
		log.info("[{}] Subscriber Thread Starting...", beanName);
		subscriberThread.start();
		log.info("<<<<< HttpJsonMetricForwarder [{}] Started.", beanName);
	}
	
	public static void main(String[] args) {
		HttpJsonMetricForwarder h = new HttpJsonMetricForwarder();
		h.setBeanName("Basic");
		try {
			h.start();
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(1);
		}
		StdInCommandHandler.getInstance().run();
	}
	
	/**
	 * Stops the forwarder
	 */
	public void stop() {
		log.info(">>>>> Stopping HttpJsonMetricForwarder....");
		
		log.info("<<<<< HttpJsonMetricForwarder Stopped.");		
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see org.springframework.beans.factory.BeanNameAware#setBeanName(java.lang.String)
	 */
	@Override
	public void setBeanName(final String name) {
		this.beanName = name;
		OBJECT_NAME = JMXHelper.objectName("com.heliosapm.streams.forwarder:service=MetricForwarder,type=" + beanName);
		EXECUTOR_OBJECT_NAME = JMXHelper.objectName("com.heliosapm.streams.forwarder:service=MetricForwarderThreadPool,type=" + beanName);
	}

	
	/**
	 * {@inheritDoc}
	 * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
	 */
	@Override
	public void setApplicationContext(final ApplicationContext appCtx) {
		this.appCtx = appCtx;
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsAssigned(java.util.Collection)
	 */
	@Override
	public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
		final StringBuilder b = new StringBuilder("\n\t=======================================\n\tAssigned Partitions");
		partitions.stream().forEach(t -> b.append("\n\t\t").append(t.topic()).append("-").append(t.partition()));
		b.append("\n\t=======================================\n");
		log.info(b.toString());
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsRevoked(java.util.Collection)
	 */
	@Override
	public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
		final StringBuilder b = new StringBuilder("\n\t=======================================\n\tRevoked Partitions");
		partitions.stream().forEach(t -> b.append("\n\t\t").append(t.topic()).append("-").append(t.partition()));
		b.append("\n\t=======================================\n");
		log.info(b.toString());
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.lang.Runnable#run()
	 */
	public void run() {
		consumer = new KafkaConsumer<>(consumerProperties);
		consumer.subscribe(Arrays.stream(topicNames).collect(Collectors.toList()), this);
		while(subThreadActive.get()) {
			
			final ConsumerRecords<String, StreamedMetricValue> records = consumer.poll(kafkaMaxPollTime);
			final int cnt = records.count();
			if(cnt>0) {
				senderChannel.writeAndFlush(records);
				log.info("Dispatched [{}] Metrics", cnt);
			}
		}
		
	}

	/**
	 * {@inheritDoc}
	 * @see org.springframework.context.ApplicationListener#onApplicationEvent(org.springframework.context.ApplicationEvent)
	 */
	@Override
	public void onApplicationEvent(final ApplicationContextEvent event) {		
		if(event.getApplicationContext()==appCtx) {
			if(event instanceof ContextRefreshedEvent) {
				try {
					start();
				} catch (Exception ex) {
					throw new RuntimeException(ex);
				}
			} else if(event instanceof ContextClosedEvent) {
				stop();
			}
		}
	}
	
	static {
		InternalLoggerFactory.setDefaultFactory(Log4J2LoggerFactory.INSTANCE);
	}
	
	final LoggingHandler lh = new LoggingHandler(getClass(), LogLevel.INFO);
	
	class InHandler extends SimpleChannelInboundHandler<FullHttpResponse> {
		@Override
		protected void channelRead0(final ChannelHandlerContext ctx, final FullHttpResponse msg) throws Exception {
			final StringBuilder b = new StringBuilder("RESPONSE:");
			b.append("\n\tStatus:").append(msg.status());
			final ByteBuf content = msg.content();
			if(content!=null && content.readableBytes()>0) {
				b.append("\n\tMessage:").append(content.toString(Charset.defaultCharset()));
			}
			log.info(b.toString());
		}
	}
	
	final InHandler inboundHandler = new InHandler();
	
	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelInitializer#initChannel(io.netty.channel.Channel)
	 */
	@Override
	protected void initChannel(final SocketChannel ch) throws Exception {
		final ChannelPipeline p = ch.pipeline();
		
		
		
		//p.addLast("compressor", new HttpContentCompressor());
		
		p.addLast("httpCodec", new HttpClientCodec());
		p.addLast("logger", lh);
		p.addLast("inhandler", inboundHandler);
		p.addLast("outhandler", outboundHandler);
		
		
		
		
//		p.addLast("httpcodec",    new HttpClientCodec());
//		p.addLast("inflater",   new HttpContentDecompressor());
//		p.addLast("qdecoder", queryResultDecoder);
		
	}

	/**
	 * Sets 
	 * @param workerThreads the workerThreads to set
	 */
	public void setWorkerThreads(int workerThreads) {
		this.workerThreads = workerThreads;
	}

	/**
	 * Sets 
	 * @param endpointHost the endpointHost to set
	 */
	public void setEndpointHost(String endpointHost) {
		this.endpointHost = endpointHost;
	}

	/**
	 * Sets 
	 * @param endpointPort the endpointPort to set
	 */
	public void setEndpointPort(int endpointPort) {
		this.endpointPort = endpointPort;
	}

	/**
	 * Sets 
	 * @param endpointUri the endpointUri to set
	 */
	public void setEndpointUri(String endpointUri) {
		this.endpointUri = endpointUri;
	}

	/**
	 * Sets 
	 * @param kafkaBootstrapServers the kafkaBootstrapServers to set
	 */
	public void setKafkaBootstrapServers(String kafkaBootstrapServers) {
		this.kafkaBootstrapServers = kafkaBootstrapServers;
	}

	/**
	 * Sets 
	 * @param kafkaGroupId the kafkaGroupId to set
	 */
	public void setKafkaGroupId(String kafkaGroupId) {
		this.kafkaGroupId = kafkaGroupId;
	}

	/**
	 * Sets 
	 * @param kafkaAutoCommit the kafkaAutoCommit to set
	 */
	public void setKafkaAutoCommit(boolean kafkaAutoCommit) {
		this.kafkaAutoCommit = kafkaAutoCommit;
	}

	/**
	 * Sets 
	 * @param kafkaAutoCommitInterval the kafkaAutoCommitInterval to set
	 */
	public void setKafkaAutoCommitInterval(int kafkaAutoCommitInterval) {
		this.kafkaAutoCommitInterval = kafkaAutoCommitInterval;
	}

	/**
	 * Sets 
	 * @param kafkaSessionTimeout the kafkaSessionTimeout to set
	 */
	public void setKafkaSessionTimeout(int kafkaSessionTimeout) {
		this.kafkaSessionTimeout = kafkaSessionTimeout;
	}

	/**
	 * Sets 
	 * @param kafkaMaxPollTime the kafkaMaxPollTime to set
	 */
	public void setKafkaMaxPollTime(int kafkaMaxPollTime) {
		this.kafkaMaxPollTime = kafkaMaxPollTime;
	}
	
}
