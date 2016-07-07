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
package com.heliosapm.streams.onramp;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.heliosapm.streams.metrics.internal.SharedMetricsRegistry;
import com.heliosapm.utils.jmx.JMXHelper;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCountUtil;
//import io.netty.handler.codec.embedder.CodecEmbedderException;
import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;



/**
 * <p>Title: ConnectionManager</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.onramp.ConnectionManager</code></p>
 */
@ChannelHandler.Sharable
public class ConnectionManager extends ChannelDuplexHandler implements ConnectionManagerMBean  {
	/** The singleton instance */
	private static volatile ConnectionManager instance = null;
	/** The singleton instance ctor lock */
	private static final Object lock = new Object();
	
	/** Instance logger */
	private final Logger log = LogManager.getLogger(getClass());
	/** A gauge of established connections */
	private final AtomicLong connections_established = new AtomicLong();
	/** The shared metric publication of the connections established gauge */
	@SuppressWarnings("unused")
	private final Gauge<Long> connections_established_gauge = SharedMetricsRegistry.getInstance().gauge("connections.estalished", new Callable<Long>(){
		@Override
		public Long call()  {
			return connections_established.get();
		}
	});
	/** A counter of connection instances */
	private final Counter connections = SharedMetricsRegistry.getInstance().counter("connections");
	
	/** A counter of unknown exception types */
	private final Counter exceptions_unknown = SharedMetricsRegistry.getInstance().counter("exceptions.unknown");
	/** A counter of client side closed exceptions */
	private final Counter exceptions_closed = SharedMetricsRegistry.getInstance().counter("exceptions.closed");	
	/** A counter of reset exceptions */
	private final Counter exceptions_reset = SharedMetricsRegistry.getInstance().counter("exceptions.reset");
	/** A counter of timeout exceptions */
	private final Counter exceptions_timeout = SharedMetricsRegistry.getInstance().counter("exceptions.timeout");
	/** A counter of idle session timeouts */
	private final Counter idle_timeout = SharedMetricsRegistry.getInstance().counter("idle.timeout");

	private final DefaultChannelGroup channels =
			new DefaultChannelGroup("all-channels", new DefaultEventExecutor(new ThreadFactory(){
				final AtomicInteger serial = new AtomicInteger();
				@Override
				public Thread newThread(final Runnable r) {
					final Thread t = new Thread(r, "ChannelGroupThread#" + serial.incrementAndGet());
					return t;
				}
			}));
	

	
	private ConnectionManager() {
		if(JMXHelper.isRegistered(OBJECT_NAME)) {
			try { JMXHelper.unregisterMBean(OBJECT_NAME); } catch (Exception x) {/* No Op */}
		}
		try { JMXHelper.registerMBean(this, OBJECT_NAME); } catch (Exception ex) {
			log.warn("Failed to register the ConnectionManager JMX MBean. Continuing without.", ex);
		}
	}
	
	/**
	 * Acquires and returns the ConnectionManager singleton instance
	 * @return the ConnectionManager singleton instance
	 */
	public static ConnectionManager getInstance() {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					instance = new ConnectionManager();
				}
			}
		}
		return instance;
	}
	
	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelInboundHandlerAdapter#channelActive(io.netty.channel.ChannelHandlerContext)
	 */
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		final Channel channel = ctx.channel();
		log.info("Channel Activated [{}]", channel);
		channels.add(channel);
		channel.closeFuture().addListener(new GenericFutureListener<Future<? super Void>>() {
			public void operationComplete(Future<? super Void> future) throws Exception {
				connections_established.decrementAndGet();				
			}
		});
		connections_established.incrementAndGet();
		connections.inc();
		super.channelActive(ctx);
	}
	
	
	
	
	
	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelInboundHandlerAdapter#channelRead(io.netty.channel.ChannelHandlerContext, java.lang.Object)
	 */
	@Override
	public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {		
		ReferenceCountUtil.retain(msg);
		super.channelRead(ctx, msg);
	}
	

	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelInboundHandlerAdapter#userEventTriggered(io.netty.channel.ChannelHandlerContext, java.lang.Object)
	 */
	public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) throws Exception {
		if (evt instanceof IdleStateEvent) {
			log.info("Session Timeout on channel [{}]", ctx.channel());
			idle_timeout.inc();
			ctx.channel().close();
			
         }
	}



	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelInboundHandlerAdapter#exceptionCaught(io.netty.channel.ChannelHandlerContext, java.lang.Throwable)
	 */
	@Override
	public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
		final Channel chan = ctx.channel();
		if (cause instanceof ClosedChannelException) {
			exceptions_closed.inc();
			log.warn("Attempt to write to closed channel " + chan);
			return;
		} else if(cause instanceof IllegalReferenceCountException) {
			log.warn("BadRefCount: [{}]", cause.getMessage());
		} else if (cause instanceof IOException) {
			final String message = cause.getMessage();
			if ("Connection reset by peer".equals(message)) {
				exceptions_reset.inc();
				return;
			} else if ("Connection timed out".equals(message)) {
				exceptions_timeout.inc();
				// Do nothing.  A client disconnecting isn't really our problem.  Oh,
				// and I'm not kidding you, there's no better way to detect ECONNRESET
				// in Java.  Like, people have been bitching about errno for years,
				// and Java managed to do something *far* worse.  That's quite a feat.
				return;
			}
		}
		// FIXME: not sure what the netty 4 is for this
		//if (cause instanceof CodecEmbedderException) {
		//	// payload was not compressed as it was announced to be
		//	log.warn("Http codec error : " + cause.getMessage());
		//	e.getChannel().close();
		//	return;
		//}
		exceptions_unknown.inc();
		log.error("Unexpected exception from downstream for " + chan, cause);
		chan.close();
	}

	/**
	 * Returns the number of currently established connections
	 * @return the number of currently established connections
	 */
	public long getConnectionsEstablished() {
		return connections_established.get();
	}

	/**

	/**
	 * Returns the cummulative number of connections established 
	 * @return the cummulative number of connections established
	 */
	public long getConnections() {
		return connections.getCount();
	}

	/**
	 * Returns the cummulative number of unknow caused exceptions
	 * @return the cummulative number of unknow caused exceptions
	 */
	public long getExceptionsUnknown() {
		return exceptions_unknown.getCount();
	}

	/**
	 * Returns the cummulative number of closed caused exceptions
	 * @return the cummulative number of closed caused exceptions
	 */
	public long getExceptionsClosed() {
		return exceptions_closed.getCount();
	}

	/**
	 * Returns the cummulative number of connection reset caused exceptions
	 * @return the cummulative number of connection reset caused exceptions
	 */
	public long getExceptionsReset() {
		return exceptions_reset.getCount();
	}

	/**
	 * Returns the cummulative number of connection timeout caused exceptions
	 * @return the cummulative number of connection timeout caused exceptions
	 */
	public long getExceptionsTimeout() {
		return exceptions_timeout.getCount();
	}

	/**
	 * Returns the cummulative number of idle connections that were reaped 
	 * @return the cummulative number of idle connections that were reaped
	 */
	public long getIdleTimeout() {
		return idle_timeout.getCount();
	}

	/**
	 * Returns the number of registered channels
	 * @return the number of registered channels
	 */
	public int getChannels() {
		return channels.size();
	}
	
	/**
	 * Terminates all established connections
	 */
	public void terminateAllConnections() {
		channels.close().syncUninterruptibly();
	}
	
	
	
	



}
