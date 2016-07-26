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
package com.heliosapm.streams.metrics.store;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

/**
 * <p>Title: TestKeyValueStore</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.store.TestKeyValueStore</code></p>
 * @param <K> The key type
 * @param <V> The value type
 */

public class TestKeyValueStore<K, V> implements KeyValueStore<K, V> {
	/** The store implementation */
	protected final NonBlockingHashMap<K,V> map = new NonBlockingHashMap<K,V>();
	
	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.processor.StateStore#name()
	 */
	@Override
	public String name() {		
		return "TestKeyValueStore";
	}
	
	/**
	 * Clears the store
	 */
	public void clear() {
		map.clear();
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.processor.StateStore#init(org.apache.kafka.streams.processor.ProcessorContext, org.apache.kafka.streams.processor.StateStore)
	 */
	@Override
	public void init(ProcessorContext context, StateStore root) {
		/* No Op */
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.processor.StateStore#flush()
	 */
	@Override
	public void flush() {
		/* No Op */
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.processor.StateStore#close()
	 */
	@Override
	public void close() {
		/* No Op */
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.processor.StateStore#persistent()
	 */
	@Override
	public boolean persistent() {
		return false;
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.state.KeyValueStore#get(java.lang.Object)
	 */
	@Override
	public V get(final K key) {		
		return map.get(key);
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.state.KeyValueStore#put(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void put(final K key, final V value) {
		map.put(key, value);
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.state.KeyValueStore#putIfAbsent(java.lang.Object, java.lang.Object)
	 */
	@Override
	public V putIfAbsent(final K key, final V value) {		
		return map.putIfAbsent(key, value);
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.state.KeyValueStore#putAll(java.util.List)
	 */
	@Override
	public void putAll(final List<KeyValue<K, V>> entries) {
		for(KeyValue<K, V> kv: entries) {
			put(kv.key, kv.value);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.state.KeyValueStore#delete(java.lang.Object)
	 */
	@Override
	public V delete(final K key) {		
		return map.remove(key);
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.state.KeyValueStore#range(java.lang.Object, java.lang.Object)
	 */
	@Override
	public KeyValueIterator<K, V> range(final K from, final K to) {
		final KVI kvi = new KVI();
		for(K key: map.keySet()) {
			final int f = key.toString().compareTo(from.toString());
			final int t = key.toString().compareTo(to.toString());
			if(f > -1 && t < 1) {
				kvi.set.add(KeyValue.pair(key, map.get(key)));
			}
		}
		return kvi;
	}
	
	public class KVI implements KeyValueIterator<K, V> {
		protected final Set<KeyValue<K, V>> set = new LinkedHashSet<KeyValue<K, V>>();
		protected volatile Iterator<KeyValue<K, V>> iter = null;
		
		/**
		 * {@inheritDoc}
		 * @see java.util.Iterator#hasNext()
		 */
		@Override
		public boolean hasNext() {
			if(iter==null) {
				iter = set.iterator();
			}
			return iter.hasNext();
		}

		/**
		 * {@inheritDoc}
		 * @see java.util.Iterator#next()
		 */
		@Override
		public KeyValue<K, V> next() {
			if(iter==null) {
				iter = set.iterator();
			}
			return iter.next();
		}

		/**
		 * {@inheritDoc}
		 * @see org.apache.kafka.streams.state.KeyValueIterator#close()
		 */
		@Override
		public void close() {
			/* No Op */			
		}
		
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.state.KeyValueStore#all()
	 */
	@Override
	public KeyValueIterator<K, V> all() {
		// TODO Auto-generated method stub
		return null;
	}

}
