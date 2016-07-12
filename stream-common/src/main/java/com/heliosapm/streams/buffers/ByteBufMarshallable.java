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
package com.heliosapm.streams.buffers;

import com.heliosapm.streams.metrics.StreamedMetric;

import io.netty.buffer.ByteBuf;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.core.io.IORuntimeException;

/**
 * <p>Title: ByteBufMarshallable</p>
 * <p>Description: An overly complicated way to put {@link ByteBuf} instances  into a chronicle queue and get them back out again</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.buffers.ByteBufMarshallable</code></p>
 */

public class ByteBufMarshallable implements WriteBytesMarshallable, ReadBytesMarshallable {
	/** The byte buff being acted on */
	protected ByteBuf byteBuf = null;
	
	private static final BufferManager bufferManager = BufferManager.getInstance(); 
	

	@SuppressWarnings("rawtypes")
	@Override
	public void readMarshallable(final BytesIn bytes) throws IORuntimeException {
		final int size = bytes.readInt();
		byteBuf = bufferManager.buffer(size);
		try {
			byteBuf.writeBytes(bytes.inputStream(), size);
		} catch (Exception ex) {
			byteBuf.release();
			byteBuf = null;
			throw new RuntimeException("Failed to read buffer bytes", ex);
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void writeMarshallable(final BytesOut bytes) {
		bytes.writeInt(byteBuf.readableBytes());
		bytes.writeSome(byteBuf.nioBuffer());
		byteBuf.release();
		byteBuf = null;
		
	}
	
	/**
	 * Returns the current ByteBuf
	 * @return the current ByteBuf
	 */
	public ByteBuf getByteBuff() {
		return byteBuf;
	}

	/**
	 * Sets the current ByteBuf
	 * @param byteBuff the buffer to set
	 * @return this marshallable
	 */
	public ByteBufMarshallable setByteBuff(final ByteBuf byteBuf) {
		this.byteBuf = byteBuf;
		return this;
	}
	
	/**
	 * Returns the current ByteBuf and nulls out the state
	 * @return the possibly null current ByteBuf
	 */
	public ByteBuf getAndNullByteBuf() {
		final ByteBuf b = byteBuf;
		byteBuf = null;
		return b;
	}
	
	

}
