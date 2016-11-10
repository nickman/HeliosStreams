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
package com.heliosapm.streams.onramp;


import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;

/**
 * <p>Title: DatagramToBytesDecoder</p>
 * <p>Description: Relays the contents of a datagram packet upstream as a byte buff</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.onramp.DatagramToBytesDecoder</code></p>
 */

public class DatagramToBytesDecoder extends MessageToMessageDecoder<DatagramPacket> {
	/** The instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	


	/**
	 * {@inheritDoc}
	 * @see io.netty.handler.codec.MessageToMessageDecoder#decode(io.netty.channel.ChannelHandlerContext, java.lang.Object, java.util.List)
	 */
	@Override
	protected void decode(final ChannelHandlerContext ctx, final DatagramPacket msg, final List<Object> out) throws Exception {
		out.add(msg.content());		
	}
	
	
}
