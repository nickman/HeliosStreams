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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.heliosapm.utils.buffer.BufferManager;

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
	/** A ref to the BufferManager */
	private static final BufferManager bufferManager = BufferManager.getInstance(); 
	
	/** Indicates if compression is enabled */
	protected final boolean useGzip;
	private static byte NOT_COMPRESSED = 0;
	private static byte COMPRESSED = 1;
	
	
	/**
	 * Creates a new ByteBufMarshallable
	 * @param gzip true to gzip payloads
	 */
	public ByteBufMarshallable(final boolean gzip) {
		useGzip = gzip;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.openhft.chronicle.bytes.ReadBytesMarshallable#readMarshallable(net.openhft.chronicle.bytes.BytesIn)
	 */
	@Override
	public void readMarshallable(final BytesIn bytes) throws IORuntimeException {
		final long start = System.currentTimeMillis();
		final byte compressed = bytes.readByte();
		final int size = bytes.readInt();
//		System.err.println("*** readMarshallable size:" + size);
		byteBuf = bufferManager.buffer(size);
		InputStream is = null;
		try {			
			is = compressed==COMPRESSED ? wrap(bytes.inputStream(), 1024) : bytes.inputStream();
			final byte[] bb = new byte[1024 * 64];
			int bytesRead = -1;			
			while((bytesRead = is.read(bb))!=-1) {
				byteBuf.writeBytes(bb, 0, bytesRead);
			}
//			int bytesRead = 0;
//			while(bytesRead < size) {
//				bytesRead += byteBuf.writeBytes(is, size-bytesRead);
//			}
			//			
//			final long elapsed = System.currentTimeMillis() - start;
//			System.err.println("*** readMarshallable complete size:" + byteBuf.readableBytes() + ", elapsed:" + elapsed);
//			System.err.println("*** readMarshallable remaining:" + is.available());
		} catch (Exception ex) {
			throw new RuntimeException("Failed to read buffer bytes", ex);
		} finally {
			if(useGzip && is != null) try { is.close(); } catch (Exception x) {/* No Op */}
		}
	}
	
	/**
	 * Wraps the passed input stream in a gzip input stream
	 * @param is the input stream to wrap
	 * @param size The buffer size
	 * @return the gzip input stream
	 * @throws IOException if an I/O error has occurred
	 */
	public static InputStream wrap(final InputStream is, final int size) throws IOException {
		return new GZIPInputStream(is, size);
	}
	
	/**
	 * Wraps the passed output stream in a gzip output stream
	 * @param is the output stream to wrap
	 * @param size The buffer size
	 * @return the gzip output stream
	 * @throws IOException if an I/O error has occurred
	 */
	public static OutputStream wrap(final OutputStream is, final int size) throws IOException {
		return new GZIPOutputStream(is, size, true);
	}
	


	@Override
	public void writeMarshallable(final BytesOut bytes) {
		bytes.writeByte(useGzip ? COMPRESSED : NOT_COMPRESSED);		
		bytes.writeInt(byteBuf.readableBytes());
		OutputStream os = null;
		try {
			os = useGzip ? wrap(bytes.outputStream(), 1024) : bytes.outputStream();
			final long pre = bytes.writePosition();
			byteBuf.readBytes(os, byteBuf.readableBytes());
//			if(useGzip) {
//				os.flush();
//				((GZIPOutputStream)os).finish();
//			}
//			bytes.outputStream().flush();
			os.flush();
			final long total = bytes.writePosition() - pre;
		} catch (Exception ex) {
			throw new RuntimeException("Failed to write buffer bytes", ex);
		} finally {
			if(useGzip && os != null) try { os.close(); } catch (Exception x) {/* No Op */}
			byteBuf.release();
			byteBuf = null;
		}
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
	 * @param byteBuf the buffer to set
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
