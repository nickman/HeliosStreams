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
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

/**
 * <p>Title: TextLineRpcHandler</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.onramp.TextLineRpcHandler</code></p>
 */

public class TextLineRpcHandler extends MessageToMessageDecoder<ByteBuf> {
	/** The instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	
	/** The UTF8 character set */
	public static final Charset UTF8 = Charset.forName("UTF8");
	
	/** The message forwarder */
	protected MessageForwarder mf = MessageForwarder.getInstance();


	/**
	 * {@inheritDoc}
	 * @see io.netty.handler.codec.MessageToMessageDecoder#decode(io.netty.channel.ChannelHandlerContext, java.lang.Object, java.util.List)
	 */
	@Override
	protected void decode(final ChannelHandlerContext ctx, final ByteBuf buff, final  List<Object> out) throws Exception {
		final String v = buff.toString(UTF8);
		log.info("Received Message: [{}]", v);
		mf.send(v);
		out.add(buff.retain());
		
		
		
	}
	
	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelInboundHandlerAdapter#exceptionCaught(io.netty.channel.ChannelHandlerContext, java.lang.Throwable)
	 */
	public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
		log.warn("Caught exception: {}",  cause);
	}



}
