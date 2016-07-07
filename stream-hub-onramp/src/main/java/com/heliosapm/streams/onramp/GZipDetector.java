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

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.compression.ZlibCodecFactory;
import io.netty.handler.codec.compression.ZlibWrapper;

/**
 * <p>Title: GZipDetector</p>
 * <p>Description: Detects GZip content and adds a decompressor if detected</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.onramp.GZipDetector</code></p>
 */

//@ChannelHandler.Sharable
public class GZipDetector extends ByteToMessageDecoder  {
	/** Instance logger */
	private final Logger log = LogManager.getLogger(getClass());

	/**
	 * Creates a new GZipDetector
	 */
	public GZipDetector() {

	}
	
	
	/**
	 * {@inheritDoc}
	 * @see io.netty.handler.codec.ByteToMessageDecoder#decode(io.netty.channel.ChannelHandlerContext, io.netty.buffer.ByteBuf, java.util.List)
	 */
	@Override
	protected void decode(final ChannelHandlerContext ctx, final ByteBuf buff, final List<Object> out) throws Exception {
		if (buff.readableBytes() > 4) {
			final int magic1 = buff.getUnsignedByte(buff.readerIndex());
			final int magic2 = buff.getUnsignedByte(buff.readerIndex() + 1);
			if(isGzip(magic1, magic2)) {
				enableGzip(ctx);
			} else {
				ctx.pipeline().remove(this);
			}
			out.add(buff.retain());
		}
	}

	
	private void enableGzip(final ChannelHandlerContext ctx) {
		final ChannelPipeline p = ctx.pipeline();
		try {	         
//			p.addAfter("connmgr", "gzipdeflater", new JZlibEncoder(ZlibWrapper.GZIP){
//				// TODO
//			});	         
			p.addAfter("gzipdetector", "gzipinflater",  ZlibCodecFactory.newZlibDecoder(ZlibWrapper.GZIP));
			p.remove(this);
		} catch (Exception ex) {
			log.error("Failed to add gzip handlers", ex);
		}
	}
	
  
  
  private static boolean isGzip(final int magic1, final int magic2) {
  	return magic1 == 31 && magic2 == 139;
  }
}

