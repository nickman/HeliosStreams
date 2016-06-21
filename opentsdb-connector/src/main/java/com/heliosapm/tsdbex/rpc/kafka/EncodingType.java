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
package com.heliosapm.tsdbex.rpc.kafka;

/**
 * <p>Title: EncodingType</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.tsdbex.rpc.kafka.EncodingType</code></p>
 */

public enum EncodingType {
	/** Binary encoding */
	BINARY,
	/** Text encoding (opentsdb telnet) */
	TEXT,
	/** opentsdb JSON encoding */
	JSON;
	
	private static final EncodingType[] values = values();
	private static final byte MAX_ORD = (byte)(values.length-1);
	
	private EncodingType() {
		bordinal = (byte)ordinal();
	}

	/**
	 * Returns the EncodingType for the passed ordinal
	 * @param code The ordinal code
	 * @return the corresponding EncodingType
	 */
	public static EncodingType ord(final byte code) {
		if(code<0 || code>MAX_ORD) throw new IllegalArgumentException("The ordinal [" + code + "] is out of range");
		return values[code];
	}
	
	
	/** The byte ordinal of this value type */
	public final byte bordinal;

}
