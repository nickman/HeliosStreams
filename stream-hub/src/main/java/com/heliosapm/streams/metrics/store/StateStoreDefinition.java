/**
 * Helios, OpenSource Monitoring
 * Brought to you by the Helios Development Group
 *
 * Copyright 2016, Helios Development Group and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org. 
 *
 */
package com.heliosapm.streams.metrics.store;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Required;

/**
 * <p>Title: StateStoreDefinition</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.store.StateStoreDefinition</code></p>
 * @param <K> The store key type
 * @param <V> The store value type
 */

public class StateStoreDefinition<K,V> implements StateStoreSupplier, InitializingBean {
	/** The state store name */
	protected String name = null;
	/** The key serializer */
	protected Serializer<K> keySerializer = null;
	/** The key deserializer */
	protected Deserializer<K> keyDeserializer = null;
	/** The value serializer */
	protected Serializer<V> valueSerializer = null;
	/** The value deserializer */
	protected Deserializer<V> valueDeserializer = null;
	/** The key Serde */
	protected Serde<K> keySerde = null;
	/** The value Serde */
	protected Serde<V> valueSerde = null;
	/** Indicates if the store should be in memory (true) or persistent (false) */
	protected boolean inMemory = false;
	/** The built state store supplier */
	protected StateStoreSupplier stateStoreSupplier = null;
	
	/**
	 * {@inheritDoc}
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() throws Exception {
		final Stores.KeyValueFactory<K, V> factory = Stores.create(name)
			.withKeys(keySerde)
			.withValues(valueSerde);
		if(inMemory) {
			stateStoreSupplier = factory.inMemory().build();
		} else {
			stateStoreSupplier = factory.persistent().build();
		}
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.processor.StateStoreSupplier#name()
	 */
	@Override
	public String name() {
		return name;
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.processor.StateStoreSupplier#get()
	 */
	@Override
	public StateStore get() {
		return stateStoreSupplier.get();
	}

	/**
	 * Sets the state store name 
	 * @param name the name to set
	 */
	@Required
	public void setName(final String name) {
		this.name = name;
	}

	/**
	 * Sets the store's key serializer
	 * @param keySerializer the keySerializer to set
	 */
	@Required
	public void setKeySerializer(final Serializer<K> keySerializer) {
		this.keySerializer = keySerializer;
	}

	/**
	 * Sets the store's key deserializer
	 * @param keyDeserializer the keyDeserializer to set
	 */
	@Required
	public void setKeyDeserializer(final Deserializer<K> keyDeserializer) {
		this.keyDeserializer = keyDeserializer;
	}

	/**
	 * Sets the store's value serializer
	 * @param valueSerializer the valueSerializer to set
	 */
	@Required
	public void setValueSerializer(final Serializer<V> valueSerializer) {
		this.valueSerializer = valueSerializer;
	}

	/**
	 * Sets the store's value deserializer
	 * @param valueDeserializer the valueDeserializer to set
	 */
	@Required
	public void setValueDeserializer(final Deserializer<V> valueDeserializer) {
		this.valueDeserializer = valueDeserializer;
	}


	/**
	 * Indicates if the state store should be in memory or persistent
	 * @param inMemory true for in memory, false for persistent
	 */
	@Required
	public void setInMemory(final boolean inMemory) {
		this.inMemory = inMemory;
	}

}
