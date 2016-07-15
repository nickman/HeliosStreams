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
package com.heliosapm.streams.buffers;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

/**
 * <p>Title: ByteBufSerde</p>
 * <p>Description: [De]Serialization for Netty {@link ByteBuf}s</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.buffers.ByteBufSerde</code></p>
 */

public class ByteBufSerde implements Serde<ByteBuf> {
	private static final BufferManager bufferManager = BufferManager.getInstance();	
	private static final ByteBufSerializer SER = new ByteBufSerializer();
	private static final ByteBufDeserializer DESER = new ByteBufDeserializer();
	
	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.common.serialization.Serde#serializer()
	 */
	@Override
	public Serializer<ByteBuf> serializer() {
		return SER;
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.common.serialization.Serde#deserializer()
	 */
	@Override
	public Deserializer<ByteBuf> deserializer() {
		return DESER;
	}

	
	/**
	 * <p>Title: ByteBufSerializer</p>
	 * <p>Description: A kafka serializer for Netty {@link ByteBuf}s</p> 
	 * <p>Company: Helios Development Group LLC</p>
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>com.heliosapm.streams.buffers.ByteBufSerde.ByteBufSerializer</code></p>
	 */
	public static class ByteBufSerializer implements Serializer<ByteBuf> {
		/**
		 * {@inheritDoc}
		 * @see org.apache.kafka.common.serialization.Serializer#serialize(java.lang.String, java.lang.Object)
		 */
		@Override
		public byte[] serialize(final String topic, final ByteBuf data) {
			final int len = data.readableBytes();
			try {
				final byte[] b =  ByteBufUtil.getBytes(data, 0, data.readableBytes());
				System.err.println("==== ByteBuf Ser: was:" + len + ", final:" + b.length);
				return b;
			} finally {
				data.release();
			}
		}

		/**
		 * <p>No Op</p>
		 * {@inheritDoc}
		 * @see org.apache.kafka.common.serialization.Serializer#configure(java.util.Map, boolean)
		 */
		@Override
		public void configure(final Map<String, ?> configs, final boolean isKey) {
			/* No Op */
		}

		/**
		 * <p>No Op</p>
		 * {@inheritDoc}
		 * @see org.apache.kafka.common.serialization.Serializer#close()
		 */
		@Override
		public void close() {
			/* No Op */
		}
	}
	
	/**
	 * <p>Title: ByteBufDeserializer</p>
	 * <p>Description: A kafka deserializer for Netty {@link ByteBuf}s</p> 
	 * <p>Company: Helios Development Group LLC</p>
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>com.heliosapm.streams.buffers.ByteBufSerde.ByteBufDeserializer</code></p>
	 */
	public static class ByteBufDeserializer implements Deserializer<ByteBuf> {

		/**
		 * {@inheritDoc}
		 * @see org.apache.kafka.common.serialization.Deserializer#deserialize(java.lang.String, byte[])
		 */
		@Override
		public ByteBuf deserialize(final String topic, final byte[] data) {
			return bufferManager.buffer(data.length).writeBytes(data);
		}

		/**
		 * <p>No Op</p>
		 * {@inheritDoc}
		 * @see org.apache.kafka.common.serialization.Deserializer#configure(java.util.Map, boolean)
		 */
		@Override
		public void configure(final Map<String, ?> configs, final boolean isKey) {
			/* No Op */			
		}


		/**
		 * <p>No Op</p>
		 * {@inheritDoc}
		 * @see org.apache.kafka.common.serialization.Deserializer#close()
		 */
		@Override
		public void close() {
			/* No Op */			
		}		
	}
	
	/**
	 * <p>No Op</p>
	 * {@inheritDoc}
	 * @see org.apache.kafka.common.serialization.Serde#configure(java.util.Map, boolean)
	 */
	@Override
	public void configure(final Map<String, ?> configs, final boolean isKey) {
		/* No Op */
	}

	/**
	 * <p>No Op</p>
	 * {@inheritDoc}
	 * @see org.apache.kafka.common.serialization.Serde#close()
	 */
	@Override
	public void close() {
		/* No Op */		
	}
	
	
}
