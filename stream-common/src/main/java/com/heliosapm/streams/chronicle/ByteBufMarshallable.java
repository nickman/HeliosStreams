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
package com.heliosapm.streams.chronicle;

import com.heliosapm.streams.buffers.BufferManager;

import io.netty.buffer.ByteBuf;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.core.io.IORuntimeException;

/**
 * <p>Title: ByteBufMarshallable</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.chronicle.ByteBufMarshallable</code></p>
 */

public class ByteBufMarshallable implements BytesMarshallable {
	/** THe buffer manager */
	protected static final BufferManager bufferManager = BufferManager.getInstance();
	protected ByteBuf buf = null;
	
	@Override
	public void readMarshallable(final BytesIn bytes) throws IORuntimeException {
		final Bytes rbytes = bytes.bytesForRead();
		final int len = rbytes.length();
		if(buf==null) buf = bufferManager.buffer(len);
		else buf.clear();
		try {
			rbytes.
			buf.writeBytes(rbytes.inputStream(), len);
		} catch (Exception ex) {
			throw new RuntimeException("Failed to read in [" + len + "] bytes");
		}
	}
	
	@Override
	public void writeMarshallable(BytesOut bytes) {
		// TODO Auto-generated method stub
		BytesMarshallable.super.writeMarshallable(bytes);
	}
	
}
