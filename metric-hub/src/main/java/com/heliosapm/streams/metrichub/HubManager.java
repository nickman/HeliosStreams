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
package com.heliosapm.streams.metrichub;

import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonGenerator;
import com.heliosapm.streams.buffers.BufferManager;
import com.heliosapm.streams.metrichub.impl.MetricsMetaAPIImpl;
import com.heliosapm.streams.metrichub.results.QueryResultDecoder;
import com.heliosapm.streams.sqlbinder.SQLWorker;
import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.url.URLHelper;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolMap;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.ChannelPoolMap;
import io.netty.channel.pool.SimpleChannelPool;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4J2LoggerFactory;

import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import reactor.core.composable.Promise;
import reactor.core.composable.Stream;
import reactor.function.Consumer;

/**
 * <p>Title: HubManager</p>
 * <p>Description: Container singleton for all the services needed for distributed queries</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrichub.HubManager</code></p>
 */

public class HubManager implements MetricsMetaAPI, ChannelPoolHandler {
	/** The singleton instance */
	private static volatile HubManager instance = null;
	/** The singleton instance ctor guard */
	private static final Object lock = new Object();
	
	/** The metrics metadata lookup service */
	private final MetricsMetaAPIImpl metricMetaService;
	/** The TSDBEndpoint to get updates lists of available endpoints */
	private final TSDBEndpoint tsdbEndpoint;
	/** The netty client event loop group */
	private final EventLoopGroup group;
	/** The netty client bootstrap */
	private final Bootstrap bootstrap;
	/** The netty channel group event executor  */
	private final EventExecutor eventExecutor;
	/** The channel pool map */
	private final ChannelPoolMap<InetSocketAddress, SimpleChannelPool> poolMap;
	/** The channel group containing all connnected channels */
	private final ChannelGroup channelGroup;
	/** The known up tsdb endpoints */
	private final List<InetSocketAddress> tsdbAddresses = new CopyOnWriteArrayList<InetSocketAddress>();
	/** The number of loaded addresses */
	private final int endpointCount;
	/** Sequence for getting random endpoint */
	private final AtomicInteger endpointSequence;
	/** The logging handler */
	private final LoggingHandler loggingHandler = new LoggingHandler(getClass(), LogLevel.ERROR); 
	
	
	
	private final QueryResultDecoder queryResultDecoder = new QueryResultDecoder();
	
	/** Instance logger */
	private final Logger log = LogManager.getLogger(getClass());
	
	static {
		InternalLoggerFactory.setDefaultFactory(Log4J2LoggerFactory.INSTANCE);
	}
	
	private final ChannelInitializer<SocketChannel> channelInitializer = new ChannelInitializer<SocketChannel>() {
		@Override
		public void initChannel(final SocketChannel ch) throws Exception {
			final ChannelPipeline p = ch.pipeline();
			//p.addLast("timeout",    new IdleStateHandler(0, 0, 60));  // TODO: configurable
			
			p.addLast("httpcodec",    new HttpClientCodec());
			p.addLast("inflater",   new HttpContentDecompressor());
			//p.addLast("aggregator", new HttpObjectAggregator(1048576));
			p.addLast("qdecoder", queryResultDecoder);
//			p.addLast("logging", loggingHandler);
		}
	};
	
	
	/**
	 * Initializes the hub manager
	 * @param properties The configuration properties
	 * @return the HubManager singleton instance
	 */
	static HubManager init(final Properties properties) {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					instance = new HubManager(properties);
				}
			}
		}
		return instance;
	}
	
	/**
	 * Acquires the hub manager
	 * @return the HubManager singleton instance
	 */
	public static HubManager getInstance() {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					throw new IllegalStateException("The HubManager has not been initialized yet. Programmer Error.");
				}
			}
		}
		return instance;
	}

	
	private HubManager(final Properties properties) {
		log.info(">>>>> Initializing HubManager...");
		metricMetaService = new MetricsMetaAPIImpl(properties);
		tsdbEndpoint = TSDBEndpoint.getEndpoint(metricMetaService.getSqlWorker());
		for(String url: tsdbEndpoint.getUpServers()) {
			final URL tsdbUrl = URLHelper.toURL(url);
			tsdbAddresses.add(new InetSocketAddress(tsdbUrl.getHost(), tsdbUrl.getPort()));
		}
		endpointCount = tsdbAddresses.size(); 
		endpointSequence = new AtomicInteger(endpointCount);
		group = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2, metricMetaService.getForkJoinPool());
		bootstrap = new Bootstrap();
		bootstrap
			.handler(channelInitializer)
			.group(group)
			.channel(NioSocketChannel.class)
			.option(ChannelOption.RCVBUF_ALLOCATOR, new AdaptiveRecvByteBufAllocator())
			.option(ChannelOption.ALLOCATOR, BufferManager.getInstance());
		final ChannelPoolHandler poolHandler = this;
		poolMap = new AbstractChannelPoolMap<InetSocketAddress, SimpleChannelPool>() {
		    @Override
		    protected SimpleChannelPool newPool(final InetSocketAddress key) {
				final Bootstrap b = new Bootstrap().handler(channelInitializer)
				.group(group)
				.remoteAddress(key)
				.channel(NioSocketChannel.class)
				.option(ChannelOption.RCVBUF_ALLOCATOR, new AdaptiveRecvByteBufAllocator())
				.option(ChannelOption.ALLOCATOR, BufferManager.getInstance());		    	
		        return new SimpleChannelPool(b, poolHandler);
		    }
		};
		eventExecutor = new DefaultEventExecutor(metricMetaService.getForkJoinPool());
		channelGroup = new DefaultChannelGroup("MetricHubChannelGroup", eventExecutor);
		log.info("<<<<< HubManager Initialized.");
	}
	
	/**
	 * Acquires the next tsdb socket address
	 * @return the next tsdb socket address
	 */
	protected InetSocketAddress endpointSocketAddress() {
		final int key = endpointSequence.incrementAndGet() % endpointCount;
		return tsdbAddresses.get(key);
	}
	
	/**
	 * Acquires a channel pool
	 * @return a channel pool
	 */
	protected ChannelPool channelPool() {
		final InetSocketAddress sockAddr = endpointSocketAddress();
		return poolMap.get(sockAddr);
	}
	
	
	public static void main(String[] args) {
		final HubManager hman = HubManager.init(URLHelper.readProperties(URLHelper.toURL("./src/test/resources/conf/application.properties")));		
		final QueryContext q = new QueryContext()
				.setTimeout(-1L)
				.setContinuous(true)
				.setPageSize(100)
				.setMaxSize(1000);
		final RequestBuilder d = new RequestBuilder("5m-ago", Aggregator.NONE);
		//hman.evaluate(q, d, "sys.cpu:host=*,*");
		hman.evaluate(q, d, "os.cpu:host=*");
		StdInCommandHandler.getInstance().registerCommand("stop", new Runnable(){
			public void run() {
				System.err.println("Done");
			}
		}).run();

	}
	
	public void evaluate(final QueryContext queryContext, final RequestBuilder requestBuilder, final String expression) {
		try {
			final JsonGenerator jg = requestBuilder.renderHeader();
			final ChannelPool pool = channelPool();
			evaluate(queryContext, expression).flush().consume(lmt -> {
				final ByteBuf bb = requestBuilder.merge(jg, lmt);
				log.info("CREQUEST:\n{}", bb.toString(UTF8));
				pool.acquire().addListener(f ->{
					final HttpRequest httpRequest = buildHttpRequest(bb);
					final Channel channel = (Channel)f.get();
					channel.writeAndFlush(httpRequest);
					
				});
			});
			
			
//			final Stream<List<TSMeta>> metaStream = evaluate(queryContext, expression);
//			final JsonGenerator jg = requestBuilder.renderHeader();			
//			requestBuilder.merge(jg, metricMetaService.getDispatcher(), metaStream, new Consumer<ByteBuf>() {
//				@Override
//				public void accept(ByteBuf t) {
//					log.info("CREQUEST:\n{}", t.toString(UTF8));
//				}
//			});
			////////////////////////////////////////////////////////////////////////////////////
//			});.onSuccess(p -> {
//				log.info("REQUEST:\n{}", p.toString(UTF8));
//			});
//			metricMetaService.getDispatcher().execute(() -> {
//				try {
//					fullRequestPromise.consume(bb -> {						
//						log.info("REQUEST:\n{}", bb.toString(UTF8));
//					});
//				} catch (Exception ex) {
//					ex.printStackTrace(System.err);
//				}
//			});
//			fullRequestPromise.consume(new Consumer<ByteBuf>() {
//				
//			});
//			fullRequestPromise.consume(new Consumer<ByteBuf>() {
//				@Override
//				public void accept(final ByteBuf bb) {
//					log.info("REQUEST:\n{}", bb.toString(UTF8));
//					final ChannelPool pool = channelPool();
//					pool.acquire().addListener((GenericFutureListener<? extends Future<? super Channel>>) f -> {
//						final HttpRequest httpRequest = buildHttpRequest(bb);
//						final Channel channel = (Channel)f.get();
//						channel.writeAndFlush(httpRequest);										
//					});
//				}
//			});
		} catch (Exception ex) {
			log.error("eval error", ex);
		}
	}
	
	public static final Charset UTF8 = Charset.forName("UTF8");
	public static final String REQUEST_CLOSER = "]}]}"; 
	
	protected ByteBuf updateJsonRequest(final List<TSMeta> tsMetas, final ByteBuf header) {
		try {
			final ByteBuf request = BufferManager.getInstance().buffer(header.readableBytes() + 1024);
			request.writeBytes(header);
			header.resetReaderIndex();
			request.writerIndex(request.writerIndex()-REQUEST_CLOSER.length());
			for(TSMeta ts: tsMetas) {
				request.writeCharSequence(new StringBuilder("\"").append(ts.getTSUID()).append("\","), UTF8);
			}
			request.writerIndex(request.writerIndex()-1);
			request.writeCharSequence(REQUEST_CLOSER, UTF8);
			return request;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
	
	protected HttpRequest buildHttpRequest(final ByteBuf jsonRequest) {
		final String[] endpoints = tsdbEndpoint.getUpServers();
		final URL postUrl = URLHelper.toURL(endpoints[0] + "/query/");
		log.info("Http Post to [{}]", postUrl);
		final DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, postUrl.getPath(), jsonRequest);
		request.headers().set(HttpHeaderNames.HOST, postUrl.getHost());
		request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
		request.headers().set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.GZIP);
		request.headers().set(HttpHeaderNames.CONTENT_LENGTH, jsonRequest.readableBytes());
		request.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
		return request;
	}
	
	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.pool.ChannelPoolHandler#channelAcquired(io.netty.channel.Channel)
	 */
	@Override
	public void channelAcquired(final Channel ch) throws Exception {
		log.info("Channel Acquired: {}", ch);
		
	}
	
	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.pool.ChannelPoolHandler#channelCreated(io.netty.channel.Channel)
	 */
	@Override
	public void channelCreated(final Channel ch) throws Exception {
		log.info("Channel Created: {}", ch);
		channelGroup.add(ch);
		final ChannelPipeline p = ch.pipeline();
		//p.addLast("timeout",    new IdleStateHandler(0, 0, 60));  // TODO: configurable		
		p.addLast("httpcodec",    new HttpClientCodec());
		p.addLast("inflater",   new HttpContentDecompressor());
		p.addLast("aggregator", new HttpObjectAggregator(1048576));
//		p.addLast("logging", loggingHandler);
		p.addLast("qdecoder", queryResultDecoder);
		
	}
	
	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.pool.ChannelPoolHandler#channelReleased(io.netty.channel.Channel)
	 */
	@Override
	public void channelReleased(final Channel ch) throws Exception {
		log.info("Channel Released: {}", ch);
	}
	
	

	/**
	 * Returns the data source
	 * @return the data source
	 */
	public DataSource getDataSource() {
		return metricMetaService.getDataSource();
	}

	/**
	 * Returns the sql worker
	 * @return the sql worker
	 */
	public SQLWorker getSqlWorker() {
		return metricMetaService.getSqlWorker();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#find(com.heliosapm.streams.metrichub.QueryContext, net.opentsdb.uid.UniqueId.UniqueIdType, java.lang.String)
	 */
	@Override
	public Stream<List<UIDMeta>> find(final QueryContext queryContext, final UniqueIdType type, final String name) {
		return metricMetaService.find(queryContext, type, name);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#getTagKeys(com.heliosapm.streams.metrichub.QueryContext, java.lang.String, java.lang.String[])
	 */
	@Override
	public Stream<List<UIDMeta>> getTagKeys(final QueryContext queryContext, final String metric, final String... tagKeys) {
		return metricMetaService.getTagKeys(queryContext, metric, tagKeys);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#getTagValues(com.heliosapm.streams.metrichub.QueryContext, java.lang.String, java.util.Map, java.lang.String)
	 */
	@Override
	public Stream<List<UIDMeta>> getTagValues(final QueryContext queryContext, final String metric, final Map<String, String> tagPairs, final String tagKey) {
		return metricMetaService.getTagValues(queryContext, metric, tagPairs, tagKey);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#getMetricNames(com.heliosapm.streams.metrichub.QueryContext, java.lang.String[])
	 */
	@Override
	public Stream<List<UIDMeta>> getMetricNames(final QueryContext queryContext, final String... tagKeys) {
		return metricMetaService.getMetricNames(queryContext, tagKeys);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#getMetricNames(com.heliosapm.streams.metrichub.QueryContext, java.util.Map)
	 */
	@Override
	public Stream<List<UIDMeta>> getMetricNames(final QueryContext queryContext, final Map<String, String> tags) {
		return metricMetaService.getMetricNames(queryContext, tags);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#getTSMetas(com.heliosapm.streams.metrichub.QueryContext, java.lang.String, java.util.Map)
	 */
	@Override
	public Stream<List<TSMeta>> getTSMetas(final QueryContext queryContext, final String metricName, final Map<String, String> tags) {
		return metricMetaService.getTSMetas(queryContext, metricName, tags);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#evaluate(com.heliosapm.streams.metrichub.QueryContext, java.lang.String)
	 */
	@Override
	public Stream<List<TSMeta>> evaluate(final QueryContext queryContext, final String expression) {
		return metricMetaService.evaluate(queryContext, expression);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#match(java.lang.String, byte[])
	 */
	@Override
	public Promise<Boolean> match(final String expression, final byte[] tsuid) {
		return metricMetaService.match(expression, tsuid);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#match(java.lang.String, java.lang.String)
	 */
	@Override
	public Promise<Boolean> match(final String expression, final String tsuid) {
		return metricMetaService.match(expression, tsuid);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#overlap(java.lang.String, java.lang.String)
	 */
	@Override
	public long overlap(final String expressionOne, final String expressionTwo) {
		return metricMetaService.overlap(expressionOne, expressionTwo);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#getAnnotations(com.heliosapm.streams.metrichub.QueryContext, java.lang.String, long[])
	 */
	@Override
	public Stream<List<Annotation>> getAnnotations(final QueryContext queryContext, final String expression, final long... startTimeEndTime) {
		return metricMetaService.getAnnotations(queryContext, expression, startTimeEndTime);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetricsMetaAPI#getGlobalAnnotations(com.heliosapm.streams.metrichub.QueryContext, long[])
	 */
	@Override
	public Stream<List<Annotation>> getGlobalAnnotations(final QueryContext queryContext, final long... startTimeEndTime) {
		return metricMetaService.getGlobalAnnotations(queryContext, startTimeEndTime);
	}

	/**
	 * Returns an array of URLs to accessible OpenTSDB api endpoints
	 * @return an array of URLs
	 * @see com.heliosapm.streams.metrichub.TSDBEndpoint#getUpServers()
	 */
	public String[] getUpServers() {
		return tsdbEndpoint.getUpServers();
	}
	
	
	
	
}
