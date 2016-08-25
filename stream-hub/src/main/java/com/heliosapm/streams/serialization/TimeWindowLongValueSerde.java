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
package com.heliosapm.streams.serialization;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;

/**
 * <p>Title: TimeWindowLongValueSerde</p>
 * <p>Description: A triplet long array intended to represent a time window (start, end) and a long value</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.serialization.TimeWindowLongValueSerde</code></p>
 */

public class TimeWindowLongValueSerde implements Serde<long[]>, Serializer<long[]>, Deserializer<long[]> {

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.common.serialization.Deserializer#deserialize(java.lang.String, byte[])
	 */
	@Override
	public long[] deserialize(final String topic, final byte[] data) {
		final long[] triplet = new long[3];
		byte[] b = new byte[8];
		System.arraycopy(data, 0, b, 0, 8);
		triplet[0] = Longs.fromByteArray(b);
		System.arraycopy(data, 8, b, 0, 8);
		triplet[1] = Longs.fromByteArray(b);
		System.arraycopy(data, 16, b, 0, 8);
		triplet[2] = Longs.fromByteArray(b);
		return triplet;
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.common.serialization.Serializer#serialize(java.lang.String, java.lang.Object)
	 */
	@Override
	public byte[] serialize(final String topic, final long[] data) {
		return Bytes.concat(
				Longs.toByteArray(data[0]),
				Longs.toByteArray(data[1]),
				Longs.toByteArray(data[2])
		);
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.common.serialization.Serde#configure(java.util.Map, boolean)
	 */
	@Override
	public void configure(final Map<String, ?> configs, final boolean isKey) {
		/* No Op */
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.common.serialization.Serde#close()
	 */
	@Override
	public void close() {
		/* No Op */
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.common.serialization.Serde#serializer()
	 */
	@Override
	public Serializer<long[]> serializer() {
		return this;
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.common.serialization.Serde#deserializer()
	 */
	@Override
	public Deserializer<long[]> deserializer() {
		return this;
	}


}
