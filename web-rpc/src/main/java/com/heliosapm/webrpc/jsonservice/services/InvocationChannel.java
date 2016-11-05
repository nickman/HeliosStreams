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
package com.heliosapm.webrpc.jsonservice.services;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Pipe;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.FileRegion;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

/**
 * <p>Title: InvocationChannel</p>
 * <p>Description: Faux channel to dispatch requests to an internal http listener</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>org.helios.tsdb.plugins.remoting.json.services.InvocationChannel</code></p>
 */

public class InvocationChannel implements Channel {
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	
	protected final List<Object> channelWrites = new ArrayList<Object>();
	
	public List<Object> getWrites() {
		return channelWrites;
	}
	
	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelOutboundInvoker#write(java.lang.Object)
	 */
	@Override
	public ChannelFuture write(Object message) {
		if(message!=null) {
			if(message instanceof FileRegion) {
				try {
					Pipe pipe = Pipe.open();
					FileRegion fr = (FileRegion)message;
					
					long bytesToRead = fr.count();
					fr.transferTo(pipe.sink(), 0L);
					byte[] content = new byte[(int)bytesToRead];
					pipe.source().read(ByteBuffer.wrap(content));
					channelWrites.add(content);
				} catch (Exception ex) {
					log.error("Failed to read content from pipe", ex);
					channelWrites.add(ex);
				}
			} else {
				channelWrites.add(message);
			}
			log.info("Received Channel Write [{}]  type:[{}]", message, message.getClass().getName());
		}
		
		return null;
	}

	
	
	/**
	 * {@inheritDoc}
	 * @see org.jboss.netty.channel.Channel#isOpen()
	 */
	@Override
	public boolean isOpen() {
		return true;
	}


	@Override
	public <T> Attribute<T> attr(AttributeKey<T> key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> boolean hasAttr(AttributeKey<T> key) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ChannelFuture bind(SocketAddress localAddress) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture connect(SocketAddress remoteAddress) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture disconnect() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture close() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture deregister() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture disconnect(ChannelPromise promise) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture close(ChannelPromise promise) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture deregister(ChannelPromise promise) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture write(Object msg, ChannelPromise promise) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture writeAndFlush(Object msg) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelPromise newPromise() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelProgressivePromise newProgressivePromise() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture newSucceededFuture() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture newFailedFuture(Throwable cause) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelPromise voidPromise() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int compareTo(Channel o) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ChannelId id() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public EventLoop eventLoop() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Channel parent() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelConfig config() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isRegistered() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isActive() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ChannelMetadata metadata() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SocketAddress localAddress() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SocketAddress remoteAddress() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelFuture closeFuture() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isWritable() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public long bytesBeforeUnwritable() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long bytesBeforeWritable() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Unsafe unsafe() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ChannelPipeline pipeline() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ByteBufAllocator alloc() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Channel read() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Channel flush() {
		// TODO Auto-generated method stub
		return null;
	}

	
	
	// ===================================================================================================================
	//  Totally Fake Methods
	// ===================================================================================================================


}
