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
package com.heliosapm.webrpc.serialization;

import io.netty.buffer.ByteBuf;

/**
 * <p>Title: ChannelBufferizable</p>
 * <p>Description: Marks a class as knowing how to convert itself to a {@link ByteBuf}.</p>
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><b><code>com.heliosapm.webrpc.serialization.ChannelBufferizable</code></b>
 */

public interface ChannelBufferizable {
	/**
	 * Marshalls this object to a ByteBuf 
	 * @return a ByteBuf with this marshalled object
	 */
	public ByteBuf toByteBuf();
	
	/**
	 * Writes this object into the passed ByteBuf
	 * @param buffer The ByteBuf to write to
	 */
	public void write(ByteBuf buffer);
}
