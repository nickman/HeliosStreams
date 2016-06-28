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

/**
 * <p>Title: StatelessSerde</p>
 * <p>Description: Wrapper class for stateless Serdes</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.serialization.StatelessSerde</code></p>
 * @param <T> The type that this Serde operates on
 */

public class StatelessSerde<T> implements Serde<T> {
	/** The underlying serializer */
	private final Serializer<T> serializer;
	/** The underlying deserializer */
	private final Deserializer<T> deserializer;

	/**
	 * Creates a new StatelessSerde
	 * @param serializer The underlying serializer
	 * @param deserializer The underlying deserializer
	 */
	public StatelessSerde(final Serializer<T> serializer, final Deserializer<T> deserializer) {
		this.serializer = serializer;
		this.deserializer = deserializer;
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
	public Serializer<T> serializer() {
		return serializer;
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.common.serialization.Serde#deserializer()
	 */
	@Override
	public Deserializer<T> deserializer() {
		return deserializer;
	}

}
