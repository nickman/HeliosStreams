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
package com.heliosapm.streams.tracing.writers;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.heliosapm.streams.buffers.BufferManager;
import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.tracing.AbstractMetricWriter;
import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.jmx.JMXManagedThreadFactory;
import com.heliosapm.utils.lang.StringHelper;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.UnorderedThreadPoolEventExecutor;

/**
 * <p>Title: NetWriter</p>
 * <p>Description: The base Netty network metric writer</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tracing.writers.NetWriter</code></p>
 * @param <C> The type of netty channel implemented by this writer
 */

public abstract class NetWriter<C extends Channel> extends AbstractMetricWriter implements RejectedExecutionHandler {

	/** The config key for the remote URIs (<b><code>host:port</code></b>) to write metrics to */
	public static final String CONFIG_REMOTE_URIS = "metricwriter.net.remotes";
	/** The default remote URIs (<b><code>host:port</code></b>) to write metrics to */
	public static final String[] DEFAULT_REMOTE_URIS = {"localhost:1892"};

	/** The config key for the core thread count in the channel group event executor */
	public static final String CONFIG_EXEC_THREADS = "metricwriter.net.group.threads";
	/** The default core thread count in the channel group event executor */
	public static final int DEFAULT_EXEC_THREADS = 4;

	/** The config key for the core thread count in the event executor */
	public static final String CONFIG_ELOOP_THREADS = "metricwriter.net.eventloop.threads";
	/** The default core thread count in the event executor */
	public static final int DEFAULT_ELOOP_THREADS = Runtime.getRuntime().availableProcessors() * 2;
	
	/** Remote URIs */
	protected final Set<String> remoteUris = new LinkedHashSet<String>();
	/** The client's event loop group */
	protected EventLoopGroup group = null;
	/** The writer's event executor */
	protected EventExecutor eventExecutor = null;
	/** A map of all the connected channels */
	protected ChannelGroup channels = null;
	/** The thread factory for the channel group event executor */
	protected final JMXManagedThreadFactory groupThreadFactory = (JMXManagedThreadFactory) JMXManagedThreadFactory.newThreadFactory(getClass().getSimpleName() + "NetWriterChannelGroup", true); 
	/** The thread factory for the event loop executor */
	protected final JMXManagedThreadFactory eventLoopThreadFactory = (JMXManagedThreadFactory) JMXManagedThreadFactory.newThreadFactory(getClass().getSimpleName() + "NetWriterEventLoop", true); 
	/** The client bootstrap */
	protected final Bootstrap bootstrap = new Bootstrap();
	/** The channel type class */
	protected final Class<C> channelType;
	
	/** The core threads in the channel group event executor */
	protected int channelGroupThreads = -1;
	/** The core threads in the event loop */
	protected int eventLoopThreads = -1;
	
	/**
	 * Creates a new NetWriter
	 * @param channelType The type of channels created by this netwriter
	 * @param confirmsMetrics Indicates if this writer expects metric submission confirmation
	 */
	public NetWriter(final Class<C> channelType, final boolean confirmsMetrics) {
		super(confirmsMetrics);
		this.channelType = channelType;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.AbstractMetricWriter#configure(java.util.Properties)
	 */
	@Override
	public void configure(final Properties config) {
		final String[] remotes = ConfigurationHelper.getArraySystemThenEnvProperty(CONFIG_REMOTE_URIS, DEFAULT_REMOTE_URIS, config);
		Collections.addAll(remoteUris,  remotes);
		channelGroupThreads = ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_EXEC_THREADS, DEFAULT_EXEC_THREADS, config);
		eventLoopThreads = ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_ELOOP_THREADS, DEFAULT_ELOOP_THREADS, config);
		eventExecutor = new UnorderedThreadPoolEventExecutor(channelGroupThreads, groupThreadFactory, this);		
		channels = new DefaultChannelGroup(getClass().getSimpleName() + "Channels", eventExecutor);
		group = new NioEventLoopGroup(eventLoopThreads, eventLoopThreadFactory);
		bootstrap			
			.group(group)
			.channel(channelType)
			.handler(getChannelInitializer())
		;
		bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000);  // FIXME: config
		bootstrap.option(ChannelOption.ALLOCATOR, BufferManager.getInstance());
				
			
		// FIXME: Tweaks for channel configuration
			
	}
	
	/**
	 * Returns the channel initializer for this writer
	 * @return the channel initializer
	 */
	protected abstract ChannelInitializer<C> getChannelInitializer();

	
	/**
	 * {@inheritDoc}
	 * @see com.google.common.util.concurrent.AbstractIdleService#startUp()
	 */
	@Override
	protected void startUp()  throws Exception {
		if(remoteUris.isEmpty()) throw new IllegalStateException("No remote URIs defined");
		
		final CountDownLatch latch = new CountDownLatch(1);
		final int remotes = remoteUris.size();
		final int[] fails = new int[1];
		for(final String uri: remoteUris) {
			String _host = null;
			int _port = -1;
			try {
				final String[] hostPort = StringHelper.splitString(uri, ':', true);
				_host = hostPort[0];
				_port = Integer.parseInt(hostPort[1]);
			} catch (Exception ex) {
				log.warn("Invalid Remote URI [{}]", uri);
			}
			final ChannelFuture cf = bootstrap.connect(_host, _port);
			cf.addListener(new GenericFutureListener<Future<Void>>() {
				public void operationComplete(final Future<Void> f) throws Exception {
					if(f.isSuccess()) {
						final Channel channel = cf.channel();
						channels.add(channel);
						latch.countDown();
						log.info("Channel [{}] connected to [{}]", channel, uri);
					} else {
						fails[0]++;
						log.warn("Channel failed to connect to [{}]", uri, f.cause());
					}
				};
			});			
		}
//		if(fails[0]==remotes) { }   // FIXME: if all remotes failed, there's no point waiting

		try {
			if(!latch.await(5, TimeUnit.SECONDS)) {	// FIXME: configurable
				throw new TimeoutException("Timed out trying to connect to any endpoint");
			}
		} catch (InterruptedException iex) {
			throw new Exception("Thread interruped while waiting for a connection");
		}
	}

	/**
	 * {@inheritDoc}
	 * @see com.google.common.util.concurrent.AbstractIdleService#shutDown()
	 */
	@Override
	protected void shutDown() {
		channels.close();
		try {
			 group.shutdownGracefully();
		} catch (Exception ex) {
			log.error("Failed to gracefully shutdown the event loop group", ex);
		}
	}
	

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.AbstractMetricWriter#doMetrics(java.util.Collection)
	 */
	@Override
	protected void doMetrics(final Collection<StreamedMetric> metrics) {
		if(metrics==null || metrics.isEmpty()) return;
		final int size = metrics.size();
		boolean complete = false;
		for(Channel ch: channels) {
			final ChannelFuture cf = ch.writeAndFlush(metrics).syncUninterruptibly();
			if(cf.isSuccess()) {
				complete = true;
				break;
			}
		}
		if(!complete) {
			this.failedMetrics.add(size);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.AbstractMetricWriter#doMetrics(com.heliosapm.streams.metrics.StreamedMetric[])
	 */
	@Override
	protected void doMetrics(final StreamedMetric... metrics) {
		if(metrics==null || metrics.length==0) return;
		final int size = metrics.length;
		boolean complete = false;
		for(Channel ch: channels) {
			final ChannelFuture cf = ch.writeAndFlush(metrics).syncUninterruptibly();
			if(cf.isSuccess()) {
				complete = true;
				break;
			}
		}
		if(!complete) {
			this.failedMetrics.add(size);
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.AbstractMetricWriter#onMetrics(io.netty.buffer.ByteBuf)
	 */
	@Override
	public void onMetrics(final ByteBuf metrics) {
		if(metrics==null || metrics.readableBytes()<5) return;
		final int size = metrics.getInt(1);
		boolean complete = false;
		for(Channel ch: channels) {
			final ChannelFuture cf = ch.writeAndFlush(metrics).syncUninterruptibly();
			if(cf.isSuccess()) {
				complete = true;
				break;
			}
		}
		if(!complete) {
			this.failedMetrics.add(size);
		}
	}


	/**
	 * {@inheritDoc}
	 * @see java.util.concurrent.RejectedExecutionHandler#rejectedExecution(java.lang.Runnable, java.util.concurrent.ThreadPoolExecutor)
	 */
	@Override
	public void rejectedExecution(final Runnable r, final ThreadPoolExecutor executor) {
		log.warn("Rejected event execution. Task:[{}], Executor:[{}]");		
	}

}
