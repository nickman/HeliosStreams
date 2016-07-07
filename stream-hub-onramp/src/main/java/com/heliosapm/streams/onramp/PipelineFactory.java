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

import java.nio.charset.Charset;
import java.util.Properties;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.heliosapm.utils.config.ConfigurationHelper;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.HashedWheelTimer;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.Timer;


/**
 * <p>Title: PipelineFactory</p>
 * <p>Description: Factory to create the netty pipeline</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.onramp.PipelineFactory</code></p>
 */

/**
 * Creates a newly configured {@link ChannelPipeline} for a new channel.
 * This class is supposed to be a singleton.
 */
@ChannelHandler.Sharable
public final class PipelineFactory extends ChannelInitializer<SocketChannel> implements ThreadFactory {
	/** The UTF8 character set */
	public static final Charset UTF8 = Charset.forName("UTF8");
	/** A UTF8 String encoder */
	private static final StringEncoder ENCODER = new StringEncoder(UTF8);
	/** Connection tracker */
	private final ConnectionManager connmgr = ConnectionManager.getInstance();
	/** Protocol detector */
	private final DetectHttpOrRpc HTTP_OR_RPC = new DetectHttpOrRpc();
	/** Idle Channel timeout handler timer */
	private final Timer timer = new HashedWheelTimer(this, 100L, TimeUnit.MILLISECONDS, 512);
	/** Thread serial factory */
	private static final AtomicInteger threadSerial = new AtomicInteger(0);

	/** Stateless handler for RPCs. */
	private final TextLineRpcHandler rpchandler;
	
	/** The idle channel reaper */
	private final IdleSessionKillerHandler idleReaper;
	/** The instance logger */
	protected final Logger log = LogManager.getLogger(getClass());


	/** The server side socket timeout. **/
	private final int socketTimeout;

	/**
	 * Creates a new pipeline factory
	 * @param appConfig The optional config properties
	 */
	public PipelineFactory(final Properties appConfig) {
		socketTimeout = ConfigurationHelper.getIntSystemThenEnvProperty("onramp.socket.timeout", 60, appConfig); 
		rpchandler = new TextLineRpcHandler();
		idleReaper = new IdleSessionKillerHandler();
	}

	/**
	 * {@inheritDoc}
	 * @see java.util.concurrent.ThreadFactory#newThread(java.lang.Runnable)
	 */
	@Override
	public Thread newThread(Runnable r) {
		final Thread t = new Thread(r, "PipelineTimer#" + threadSerial.incrementAndGet());
		t.setDaemon(true);
		return t;
	}

	private static boolean isHttp(int magic1, int magic2) {
		return
				magic1 == 'G' && magic2 == 'E' || // GET
				magic1 == 'P' && magic2 == 'O' || // POST
				magic1 == 'P' && magic2 == 'U' || // PUT
				magic1 == 'H' && magic2 == 'E' || // HEAD
				magic1 == 'O' && magic2 == 'P' || // OPTIONS
				magic1 == 'P' && magic2 == 'A' || // PATCH
				magic1 == 'D' && magic2 == 'E' || // DELETE
				magic1 == 'T' && magic2 == 'R' || // TRACE
				magic1 == 'C' && magic2 == 'O';   // CONNECT
	}  

	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelInitializer#initChannel(io.netty.channel.Channel)
	 */
	@Override
	protected void initChannel(final SocketChannel ch) throws Exception {
		final ChannelPipeline pipeline = ch.pipeline();		
		pipeline.addLast("idlestate", new IdleStateHandler(0, 0, 5)); // this.socketTimeout
//		pipeline.addLast("idlereaper", idleReaper);
		pipeline.addLast("connmgr", connmgr);
		pipeline.addLast("gzipdetector", new GZipDetector());		
		pipeline.addLast("detect", HTTP_OR_RPC);
	}




	/**
	 * Dynamically changes the {@link ChannelPipeline} based on the request.
	 * If a request uses HTTP, then this changes the pipeline to process HTTP.
	 * Otherwise, the pipeline is changed to processes an RPC.
	 */
	@ChannelHandler.Sharable
	final class DetectHttpOrRpc extends SimpleChannelInboundHandler<ByteBuf> {
		private final Logger log = LogManager.getLogger(getClass());
		@Override
		protected void channelRead0(final ChannelHandlerContext ctx, final ByteBuf in) throws Exception {
			
			if (in.readableBytes() < 5) {
				in.retain();
				return;
			}
			try {			
				log.info("Detecting Http vs. Telnet. Pipeline is {}", ctx.pipeline().names());
				final int magic1 = in.getUnsignedByte(in.readerIndex());
				final int magic2 = in.getUnsignedByte(in.readerIndex() + 1);
				if(isHttp(magic1, magic2)) {
					log.debug("Switching to Http [{}]", ctx.channel());
					switchToHttp(ctx, 512 * 1024); //tsdb.getConfig().max_chunked_requests());
				} else {
					log.debug("Switching to Telnet [{}]", ctx.channel());
					switchToTelnet(ctx);
				}
	//			ctx.pipeline().remove(this);
				in.retain();
				log.info("Sending [{}] up pipeline {}", in, ctx.pipeline().names());
				ctx.fireChannelRead(in);
			} finally {
//				ReferenceCountUtil.releaseLater(in);
			}
		}

		private void switchToTelnet(final ChannelHandlerContext ctx) {
			ChannelPipeline p = ctx.pipeline();
			p.addLast("framer", new LineBasedFrameDecoder(1024, true, true));
//			p.addLast("encoder", ENCODER);
//			p.addLast("stringdecoder", new StringDecoder(UTF8));
			p.addLast("linehandler", new TextLineRpcHandler());
			p.remove(this);
		}

		/**
		 * Modifies the pipeline to handle HTTP requests
		 * @param ctx The calling channel handler context
		 * @param maxRequestSize The maximum request size in bytes
		 */
		private void switchToHttp(final ChannelHandlerContext ctx, final int maxRequestSize) {
			ChannelPipeline p = ctx.pipeline();
			p.addLast("decompressor", new HttpContentDecompressor());
			p.addLast("httpHandler", new HttpServerCodec());  // TODO: config ?
			p.addLast("aggregator", new HttpObjectAggregator(maxRequestSize)); 
			p.addLast("compressor", new HttpContentCompressor());
			p.addLast("handler", rpchandler);
		}  
	}  
	
	/**
	 * <p>Title: IdleSessionKillerHandler</p>
	 * <p>Description: Handler to close a channel when it has idled for longer than <b><code>onramp.socket.timeout</code></b> seconds.</p> 
	 * <p>Company: Helios Development Group LLC</p>
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>com.heliosapm.streams.onramp.PipelineFactory.IdleSessionKillerHandler</code></p>
	 */
	@ChannelHandler.Sharable
	protected class IdleSessionKillerHandler extends ChannelDuplexHandler {
		/**
		 * {@inheritDoc}
		 * @see io.netty.channel.ChannelInboundHandlerAdapter#userEventTriggered(io.netty.channel.ChannelHandlerContext, java.lang.Object)
		 */
		@Override
		public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) throws Exception {
			if (evt instanceof IdleStateEvent) {
				ctx.close();
	         }
		}
	}
}
