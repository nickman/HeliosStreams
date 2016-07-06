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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.group.DefaultChannelGroup;
//import io.netty.handler.codec.embedder.CodecEmbedderException;
import io.netty.util.concurrent.DefaultEventExecutor;



/**
 * <p>Title: ConnectionManager</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.onramp.ConnectionManager</code></p>
 */

public class ConnectionManager extends ChannelInboundHandlerAdapter {
	/** Instance logger */
	private final Logger LOG = LogManager.getLogger(getClass());
	/** A gauge of established connections */
	private static final AtomicLong connections_established = new AtomicLong();
	/** The shared metric publication of the connections established gauge */
	@SuppressWarnings("unused")
	private static final Gauge<Long> connections_established_gauge = SharedMetricsRegistry.getInstance().gauge("connections.estalished", new Callable<Long>(){
		@Override
		public Long call()  {
			return connections_established.get();
		}
	});
	/** A counter of unknown exception types */
	private static final Counter exceptions_unknown = SharedMetricsRegistry.getInstance().counter("exceptions.unknown");
	/** A counter of client side closed exceptions */
	private static final Counter exceptions_closed = SharedMetricsRegistry.getInstance().counter("exceptions.closed");	
	/** A counter of reset exceptions */
	private static final Counter exceptions_reset = SharedMetricsRegistry.getInstance().counter("exceptions.reset");
	/** A counter of timeout exceptions */
	private static final Counter exceptions_timeout = SharedMetricsRegistry.getInstance().counter("exceptions.timeout");

	private static final DefaultChannelGroup channels =
			new DefaultChannelGroup("all-channels", new DefaultEventExecutor(new ThreadFactory(){
				final AtomicInteger serial = new AtomicInteger();
				@Override
				public Thread newThread(final Runnable r) {
					final Thread t = new Thread(r, "ChannelGroupThread#" + serial.incrementAndGet());
					return t;
				}
			}));
	
	static void closeAllConnections() {
		channels.close().awaitUninterruptibly();
	}

	/** Constructor. */
	public ConnectionManager() {
	}
	
	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelInboundHandlerAdapter#channelActive(io.netty.channel.ChannelHandlerContext)
	 */
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		LOG.info("Channel Activated [{}]", ctx.channel());
		channels.add(ctx.channel());
		connections_established.incrementAndGet();
		super.channelActive(ctx);
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
			LOG.warn("Attempt to write to closed channel " + chan);
			return;
		}
		if (cause instanceof IOException) {
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
		//	LOG.warn("Http codec error : " + cause.getMessage());
		//	e.getChannel().close();
		//	return;
		//}
		exceptions_unknown.inc();
		LOG.error("Unexpected exception from downstream for " + chan, cause);
		chan.close();
	}
	
	



}
