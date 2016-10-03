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

import java.util.concurrent.atomic.AtomicReference;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.core.io.IORuntimeException;

/**
 * <p>Title: MessageContainer</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tsdb.listener.MessageContainer</code></p>
 */

public class MessageContainer<T extends BytesMarshallable> implements BytesMarshallable {
	private final AtomicReference<T> ref = new AtomicReference<T>(null);
	
	private static final ThreadLocal<MessageContainer<BytesMarshallable>> MC = new ThreadLocal<MessageContainer<BytesMarshallable>>() {
		/**
		 * {@inheritDoc}
		 * @see java.lang.ThreadLocal#initialValue()
		 */
		@Override
		protected MessageContainer<BytesMarshallable> initialValue() {			
			return new MessageContainer<BytesMarshallable>();
		}
	};

	public static MessageContainer<BytesMarshallable> get() {
		return MC.get();
	}
	
	/**
	 * Creates a new MessageContainer
	 */
	private MessageContainer() {
	}
	
	@Override
	public void readMarshallable(final BytesIn bytes) throws IORuntimeException {
		final byte type = bytes.readByte();
		final MessageType mt = MessageType.decode(type);
		mt.instance().readMarshallable(bytes);
		ref.set((T) mt.instance());
	}
	
	public T getMessage() {
		return ref.getAndSet(null);
	}

}
