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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.cliffc.high_scale_lib.NonBlockingHashSet;

import com.heliosapm.streams.buffers.BufferManager;
import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.tracing.AbstractMetricWriter;
import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.jmx.JMXManagedThreadFactory;
import com.heliosapm.utils.jmx.SharedNotificationExecutor;
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
 * TODO:
 * 		optional storage of metrics while waiting for an open connection
 * 		implement JMX management interface
 */

public abstract class NetWriter<C extends Channel> extends AbstractMetricWriter implements RejectedExecutionHandler, ConnectionStateSupplier, ConnectionStateListener {

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
	
	/** A set of registered connection state listeners */
	protected final NonBlockingHashSet<ConnectionStateListener> connectedStateListeners = new NonBlockingHashSet<ConnectionStateListener>(); 
	
	/** The core threads in the channel group event executor */
	protected int channelGroupThreads = -1;
	/** The core threads in the event loop */
	protected int eventLoopThreads = -1;
	
	/** A map of close futures for connected channels */
	protected final NonBlockingHashMap<String, ChannelFuture> closeFutures = new NonBlockingHashMap<String, ChannelFuture>(); 
	/** A set of host/port pairs for non-connected channels */
	protected final DelayQueue<DelayedReconnect> disconnected = new DelayQueue<DelayedReconnect>();
	/** A flag indicating if the reconnect thread should keep running */
	protected final AtomicBoolean keepReconnecting = new AtomicBoolean(false);
	/** A flag indicating if there are any connected channels available */
	protected final AtomicBoolean connectionsAvailable = new AtomicBoolean(false);
	
	/** The reconnect thread */
	protected Thread reconnectThread = null;
	
	/** An array of the configured remote URIs to connect to */
	protected String[] remotes = {};
	
	/**
	 * Creates a new NetWriter
	 * @param channelType The type of channels created by this netwriter
	 * @param confirmsMetrics Indicates if this writer expects metric submission confirmation
	 */
	public NetWriter(final Class<C> channelType, final boolean confirmsMetrics) {
		super(confirmsMetrics, false);
		this.channelType = channelType;
		registerConnectionStateListener(this);
	}
	
//	public static void main(String[] args) {
//		log("DelayedReconnect Test");
//		final DelayQueue<DelayedReconnect> Q = new DelayQueue<DelayedReconnect>();
//		try {
//			Q.put(new DelayedReconnect("In 10 Secs", 10000));
//			Q.put(new DelayedReconnect("In 5 Secs", 5000));
//			int pulled = 0;
//			while(!Q.isEmpty()) {
//				DelayedReconnect dr = Q.take();
//				if(dr==null) break;
//				pulled++;
//				log("\t----->[" + new Date() + "] DR:" + dr);
//			}
//			log("Pulled:" + pulled);
//		} catch (Exception ex) {
//			ex.printStackTrace(System.err);
//		}
//		
//	}
//	
//	public static void log(Object msg) {
//		System.out.println(msg);
//	}
	
	private static class DelayedReconnect implements Delayed {
		private final String uri;
		private final long expireTime; // = System.currentTimeMillis() + 60000;  // FIXME: configurable
		
		/**
		 * Creates a new DelayedReconnect
		 * @param uri the URI to reconnect to
		 * @param expireInMs The expiration from now in ms.
		 */
		public DelayedReconnect(final String uri, final long expireInMs) {			
			this.uri = uri;
			expireTime = System.currentTimeMillis() + expireInMs;
		}
		
		/**
		 * Creates a new DelayedReconnect with an expiry of 60 seconds.
		 * @param uri the URI to reconnect to
		 */
		public DelayedReconnect(final String uri) {
			this(uri, 60000);
		}
		
		public String toString() {
			return "DelayedReconnect[" + uri + ", " + new Date(expireTime) + "]";
		}

		@Override
		public int compareTo(final Delayed o) {
			final Long myDelay = getDelay(TimeUnit.MILLISECONDS);
			final Long odelay = o.getDelay(TimeUnit.MILLISECONDS);
			if(myDelay.equals(odelay)) return -1;
			return myDelay.compareTo(odelay);
		}

		@Override
		public long getDelay(final TimeUnit unit) {			
			final long until = expireTime - System.currentTimeMillis();
			final long delay = unit.convert(until, TimeUnit.MILLISECONDS);
//			log("[" + this + "] Unit:" + unit + ", Delay:" + delay + ", MS:" + until);
			return delay;
		}

		/**
		 * {@inheritDoc}
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((uri == null) ? 0 : uri.hashCode());
			return result;
		}

		/**
		 * {@inheritDoc}
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			DelayedReconnect other = (DelayedReconnect) obj;
			if (uri == null) {
				if (other.uri != null)
					return false;
			} else if (!uri.equals(other.uri))
				return false;
			return true;
		}

	}
	
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.AbstractMetricWriter#configure(java.util.Properties)
	 */
	@Override
	public void configure(final Properties config) {	
		super.configure(config);
		remotes = ConfigurationHelper.getArraySystemThenEnvProperty(CONFIG_REMOTE_URIS, DEFAULT_REMOTE_URIS, config);
		Collections.addAll(remoteUris,  remotes);
		channelGroupThreads = ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_EXEC_THREADS, DEFAULT_EXEC_THREADS, config);
		this.config.put("channelGroupThreads", channelGroupThreads);
		eventLoopThreads = ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_ELOOP_THREADS, DEFAULT_ELOOP_THREADS, config);
		this.config.put("eventLoopThreads", eventLoopThreads);
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
		this.config.put("connectTimeout", 5000);
		
				
			
		// FIXME: Tweaks for channel configuration
			
	}
	
	/**
	 * Returns the channel initializer for this writer
	 * @return the channel initializer
	 */
	protected abstract ChannelInitializer<C> getChannelInitializer();

	
	/**
	 * Attempts to connect to the specified host/port and updates the channel tracking structures accordingly
	 * @param uri The <b><code>host:port</code></b> pair
	 */
	protected void connect(final String uri) {
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
			@Override
			public void operationComplete(final Future<Void> f) throws Exception {
				if(f.isSuccess()) {
					final Channel channel = cf.channel();
					ChannelFuture closeFuture = channel.closeFuture();
					closeFutures.put(uri, closeFuture);
					disconnected.remove(new DelayedReconnect(uri));
					closeFuture.addListener(new GenericFutureListener<Future<? super Void>>() {
						@Override
						public void operationComplete(final Future<? super Void> future) throws Exception {
							closeFutures.remove(uri);
							if(group.isShutdown() || group.isShuttingDown() || group.isTerminated()) {
								/* No Op */
							} else {
								final DelayedReconnect dc = new DelayedReconnect(uri);
								if(!disconnected.contains(dc)) {
									disconnected.add(dc);
								}
							}
							channels.remove(channel); // may have been removed already
							fireDisconnected();
						}
					});
					channels.add(channel);	
					fireConnected();
					log.info("Channel [{}] connected to [{}]", channel, uri);
				} else {
					final DelayedReconnect dc = new DelayedReconnect(uri);
					if(!disconnected.contains(dc)) {
						disconnected.add(dc);
					}
					log.warn("Channel failed to connect to [{}]", uri, f.cause());
				}
			}
		});			
	}
	
	/**
	 * Fires a connected event if the channel group has at least one channel
	 * and the current state is disconnected.
	 */
	protected void fireConnected() {
		if(!channels.isEmpty() && !connectionsAvailable.getAndSet(true)) {
			for(final ConnectionStateListener listener: connectedStateListeners) {
				SharedNotificationExecutor.getInstance().execute(new Runnable(){
					@Override
					public void run() {
						listener.onConnected();
					}
				});
			}
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.AbstractMetricWriter#isConnected()
	 */
	@Override
	public boolean isConnected() {		
		return connectionsAvailable.get();
	}
	
	/**
	 * Fires a disconnected event if the channel group is empty
	 * and the current state is connected.
	 */
	protected void fireDisconnected() {
		if(channels.isEmpty() && connectionsAvailable.getAndSet(false)) {
			for(final ConnectionStateListener listener: connectedStateListeners) {
				SharedNotificationExecutor.getInstance().execute(new Runnable(){
					@Override
					public void run() {
						listener.onDisconnected(!keepReconnecting.get());
					}
				});
			}		
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.writers.ConnectionStateSupplier#registerConnectionStateListener(com.heliosapm.streams.tracing.writers.ConnectionStateListener)
	 */
	@Override
	public void registerConnectionStateListener(final ConnectionStateListener listener) {
		if(listener!=null) {
			connectedStateListeners.add(listener);
		}		
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.writers.ConnectionStateSupplier#removeConnectionStateListener(com.heliosapm.streams.tracing.writers.ConnectionStateListener)
	 */
	@Override
	public void removeConnectionStateListener(final ConnectionStateListener listener) {
		if(listener!=null) {
			connectedStateListeners.remove(listener);
		}				
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see com.google.common.util.concurrent.AbstractIdleService#startUp()
	 */
	@Override
	protected void startUp()  throws Exception {
		if(remoteUris.isEmpty()) throw new IllegalStateException("No remote URIs defined");
		for(final String uri: remoteUris) {
			connect(uri);
		}
		startReconnectThread();
	}
	
	/**
	 * Starts the reconnect thread if it is not already running
	 */
	protected void startReconnectThread() {
		if(keepReconnecting.compareAndSet(false, true)) {
			reconnectThread = new Thread(getClass().getSimpleName() + "ReconnectThread") {
				@Override
				public void run() {
					log.info("Reconnect thread started");
					while(keepReconnecting.get()) {
						try {
							final DelayedReconnect dc  = disconnected.poll(10, TimeUnit.SECONDS);
							if(dc!=null) {
								connect(dc.uri);
							}
						} catch (InterruptedException iex) {
							if(Thread.interrupted()) Thread.interrupted();
						} catch (Exception ex) {
							log.warn("Reconnect thread error", ex);
						}						
					}
					log.info("Reconnect thread ending");
				}
			};
			reconnectThread.setDaemon(true);
			reconnectThread.start();
		}
	}
	
	/**
	 * Stops the reconnect thread
	 */
	protected void stopReconnectThread() {
		if(keepReconnecting.compareAndSet(true, false)) {
			if(reconnectThread!=null) {
				reconnectThread.interrupt();
				reconnectThread = null;
			}
		}
	}

	/**
	 * {@inheritDoc}
	 * @see com.google.common.util.concurrent.AbstractIdleService#shutDown()
	 */
	@Override
	protected void shutDown() {
		stopReconnectThread();
		disconnected.clear();
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
		if(!connectionsAvailable.get()) {
			failedMetrics.add(size);
			return;
		}
		
		boolean complete = false;
		for(Channel ch: channels) {
			final ChannelFuture cf = ch.writeAndFlush(metrics).syncUninterruptibly();
			if(cf.isSuccess()) {
				complete = true;
				sentMetrics.add(size);
				break;
			}
		}
		if(!complete) {
			failedMetrics.add(size);
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
		if(!connectionsAvailable.get()) {
			failedMetrics.add(size);
			return;
		}
		boolean complete = false;
		for(Channel ch: channels) {
			final ChannelFuture cf = ch.writeAndFlush(metrics).syncUninterruptibly();
			if(cf.isSuccess()) {
				complete = true;
				sentMetrics.add(size);
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
		if(!connectionsAvailable.get()) {
			// FIXME: drop counter
			return;
		}
		
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
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.writers.ConnectionStateListener#onConnected()
	 */
	@Override
	public void onConnected() {
		log.info(getClass().getSimpleName() + " Connected");		
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.writers.ConnectionStateListener#onDisconnected(boolean)
	 */
	@Override
	public void onDisconnected(final boolean shutdown) {
		if(!shutdown) {
			log.warn("\n\t=====================\n\t{} Disconnected !\n\t=====================\n");
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.MetricWriterMXBean#getCustomState()
	 */
	@Override
	public String getCustomState() {
		final StringBuilder b = new StringBuilder();
		b.append("Remotes:").append(Arrays.toString(remotes));
		b.append("\nConnected:").append(channels.size());
		b.append("\nDisconnected:").append(disconnected.size());
		return b.toString();
	}
	
}
