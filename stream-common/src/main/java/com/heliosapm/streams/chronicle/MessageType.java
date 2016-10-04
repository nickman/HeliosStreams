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

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.core.io.IORuntimeException;

/**
 * <p>Title: MessageType</p>
 * <p>Description: Byte decodes enum for chronicle dispatched messages</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.chronicle.MessageType</code></p>
 */

public enum MessageType implements MessageTypeProvider, BytesMarshallable {
	ANNOTATION(null){
		@Override
		public BytesMarshallable instance() {			
			return null;
		}
	},
	DATAPOINT(DataPoint.class){
		@Override
		public BytesMarshallable instance() {			
			return DataPoint.getAndReset();
		}
	},
	METRICMETA(TSDBMetricMeta.class){
		@Override
		public BytesMarshallable instance() {			
			return TSDBMetricMeta.FACTORY.newInstance();
		}		
	};
	
	
	
	private static final MessageType[] values = values();
	private static final byte maxValidByte = (byte)(values.length-1);
	
	
	private MessageType(final Class<? extends BytesMarshallable> type) {
		byteOrdinal = (byte)ordinal();
		this.type = type;
	}
	
	/** The ordinal as a byte */
	public final byte byteOrdinal;
	/** The type supplied by this message type */
	public final Class<? extends BytesMarshallable> type;
	
	/**
	 * Decodes the passed byte to the corresponding MessageType
	 * @param b the byte to decode
	 * @return the MessageType
	 */
	public static MessageType decode(final byte b) {
		if(b<0 || b>maxValidByte) throw new IllegalArgumentException("Invalid MessageType byte [" + b + "]");
		return values[b];
	}
	
	@Override
	public void readMarshallable(BytesIn bytes) throws IORuntimeException {
		
		
	}
	
}
