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
package com.heliosapm.streams.common.kafka.ext;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StreamPartitioner;

/**
 * <p>Title: KStreamExt</p>
 * <p>Description: An extended {@link KStream} to support native java streaming in and out of the Kafka-Streams engine</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.common.kafka.ext.KStreamExt</code></p>
 * @param <K> Type of keys
 * @param <V> Type of values
 */

public class KStreamExt<K, V> implements KStream<K, V> {
	protected final KStream<K,V> delegate;
	
	public KStreamExt(final KStream<K,V> delegate) {
		this.delegate = delegate;	
	}
	
	protected Stream<KeyValue<K,V>> append(final AtomicReference<Stream<KeyValue<K,V>>> ref, final K key, final V value) {
		final Stream<KeyValue<K,V>> newstr = Stream.of(new KeyValue<K,V>(key, value));
		return Stream.concat(ref.getAndSet(newstr), newstr);
	}
	
	public Stream<KeyValue<K,V>> toJStream() {
		final Stream<KeyValue<K,V>> initial = Stream.empty();
		final AtomicReference<Stream<KeyValue<K,V>>> ref = new AtomicReference<Stream<KeyValue<K,V>>>(initial); 		
		foreach((k,v) -> append(ref, k, v));
		Stream.generate(new Supplier<KeyValue<K,V>>(){
			@Override
			public KeyValue<K, V> get() {				
				return ref.get().iterator().next();
			}
		});
		return initial;
	}

	/**
	 * @param predicate
	 * @return
	 * @see org.apache.kafka.streams.kstream.KStream#filter(org.apache.kafka.streams.kstream.Predicate)
	 */
	public KStream<K, V> filter(Predicate<K, V> predicate) {
		return delegate.filter(predicate);
	}

	/**
	 * @param predicate
	 * @return
	 * @see org.apache.kafka.streams.kstream.KStream#filterNot(org.apache.kafka.streams.kstream.Predicate)
	 */
	public KStream<K, V> filterNot(Predicate<K, V> predicate) {
		return delegate.filterNot(predicate);
	}

	/**
	 * @param <K1>
	 * @param mapper
	 * @return
	 * @see org.apache.kafka.streams.kstream.KStream#selectKey(org.apache.kafka.streams.kstream.KeyValueMapper)
	 */
	public <K1> KStream<K1, V> selectKey(KeyValueMapper<K, V, K1> mapper) {
		return delegate.selectKey(mapper);
	}

	/**
	 * @param <K1>
	 * @param <V1>
	 * @param mapper
	 * @return
	 * @see org.apache.kafka.streams.kstream.KStream#map(org.apache.kafka.streams.kstream.KeyValueMapper)
	 */
	public <K1, V1> KStream<K1, V1> map(KeyValueMapper<K, V, KeyValue<K1, V1>> mapper) {
		return delegate.map(mapper);
	}

	/**
	 * @param <V1>
	 * @param mapper
	 * @return
	 * @see org.apache.kafka.streams.kstream.KStream#mapValues(org.apache.kafka.streams.kstream.ValueMapper)
	 */
	public <V1> KStream<K, V1> mapValues(ValueMapper<V, V1> mapper) {
		return delegate.mapValues(mapper);
	}

	/**
	 * 
	 * @see org.apache.kafka.streams.kstream.KStream#print()
	 */
	public void print() {
		delegate.print();
	}

	/**
	 * @param keySerde
	 * @param valSerde
	 * @see org.apache.kafka.streams.kstream.KStream#print(org.apache.kafka.common.serialization.Serde, org.apache.kafka.common.serialization.Serde)
	 */
	public void print(Serde<K> keySerde, Serde<V> valSerde) {
		delegate.print(keySerde, valSerde);
	}

	/**
	 * @param filePath
	 * @see org.apache.kafka.streams.kstream.KStream#writeAsText(java.lang.String)
	 */
	public void writeAsText(String filePath) {
		delegate.writeAsText(filePath);
	}

	/**
	 * @param filePath
	 * @param keySerde
	 * @param valSerde
	 * @see org.apache.kafka.streams.kstream.KStream#writeAsText(java.lang.String, org.apache.kafka.common.serialization.Serde, org.apache.kafka.common.serialization.Serde)
	 */
	public void writeAsText(String filePath, Serde<K> keySerde, Serde<V> valSerde) {
		delegate.writeAsText(filePath, keySerde, valSerde);
	}

	/**
	 * @param <K1>
	 * @param <V1>
	 * @param mapper
	 * @return
	 * @see org.apache.kafka.streams.kstream.KStream#flatMap(org.apache.kafka.streams.kstream.KeyValueMapper)
	 */
	public <K1, V1> KStream<K1, V1> flatMap(KeyValueMapper<K, V, Iterable<KeyValue<K1, V1>>> mapper) {
		return delegate.flatMap(mapper);
	}

	/**
	 * @param <V1>
	 * @param processor
	 * @return
	 * @see org.apache.kafka.streams.kstream.KStream#flatMapValues(org.apache.kafka.streams.kstream.ValueMapper)
	 */
	public <V1> KStream<K, V1> flatMapValues(ValueMapper<V, Iterable<V1>> processor) {
		return delegate.flatMapValues(processor);
	}

	/**
	 * @param predicates
	 * @return
	 * @see org.apache.kafka.streams.kstream.KStream#branch(org.apache.kafka.streams.kstream.Predicate[])
	 */
	public KStream<K, V>[] branch(Predicate<K, V>... predicates) {
		return delegate.branch(predicates);
	}

	/**
	 * @param topic
	 * @return
	 * @see org.apache.kafka.streams.kstream.KStream#through(java.lang.String)
	 */
	public KStream<K, V> through(String topic) {
		return delegate.through(topic);
	}

	/**
	 * @param action
	 * @see org.apache.kafka.streams.kstream.KStream#foreach(org.apache.kafka.streams.kstream.ForeachAction)
	 */
	public void foreach(ForeachAction<K, V> action) {
		delegate.foreach(action);
	}

	/**
	 * @param partitioner
	 * @param topic
	 * @return
	 * @see org.apache.kafka.streams.kstream.KStream#through(org.apache.kafka.streams.processor.StreamPartitioner, java.lang.String)
	 */
	public KStream<K, V> through(StreamPartitioner<K, V> partitioner, String topic) {
		return delegate.through(partitioner, topic);
	}

	/**
	 * @param keySerde
	 * @param valSerde
	 * @param topic
	 * @return
	 * @see org.apache.kafka.streams.kstream.KStream#through(org.apache.kafka.common.serialization.Serde, org.apache.kafka.common.serialization.Serde, java.lang.String)
	 */
	public KStream<K, V> through(Serde<K> keySerde, Serde<V> valSerde, String topic) {
		return delegate.through(keySerde, valSerde, topic);
	}

	/**
	 * @param keySerde
	 * @param valSerde
	 * @param partitioner
	 * @param topic
	 * @return
	 * @see org.apache.kafka.streams.kstream.KStream#through(org.apache.kafka.common.serialization.Serde, org.apache.kafka.common.serialization.Serde, org.apache.kafka.streams.processor.StreamPartitioner, java.lang.String)
	 */
	public KStream<K, V> through(Serde<K> keySerde, Serde<V> valSerde, StreamPartitioner<K, V> partitioner,
			String topic) {
		return delegate.through(keySerde, valSerde, partitioner, topic);
	}

	/**
	 * @param topic
	 * @see org.apache.kafka.streams.kstream.KStream#to(java.lang.String)
	 */
	public void to(String topic) {
		delegate.to(topic);
	}

	/**
	 * @param partitioner
	 * @param topic
	 * @see org.apache.kafka.streams.kstream.KStream#to(org.apache.kafka.streams.processor.StreamPartitioner, java.lang.String)
	 */
	public void to(StreamPartitioner<K, V> partitioner, String topic) {
		delegate.to(partitioner, topic);
	}

	/**
	 * @param keySerde
	 * @param valSerde
	 * @param topic
	 * @see org.apache.kafka.streams.kstream.KStream#to(org.apache.kafka.common.serialization.Serde, org.apache.kafka.common.serialization.Serde, java.lang.String)
	 */
	public void to(Serde<K> keySerde, Serde<V> valSerde, String topic) {
		delegate.to(keySerde, valSerde, topic);
	}

	/**
	 * @param keySerde
	 * @param valSerde
	 * @param partitioner
	 * @param topic
	 * @see org.apache.kafka.streams.kstream.KStream#to(org.apache.kafka.common.serialization.Serde, org.apache.kafka.common.serialization.Serde, org.apache.kafka.streams.processor.StreamPartitioner, java.lang.String)
	 */
	public void to(Serde<K> keySerde, Serde<V> valSerde, StreamPartitioner<K, V> partitioner, String topic) {
		delegate.to(keySerde, valSerde, partitioner, topic);
	}

	/**
	 * @param <K1>
	 * @param <V1>
	 * @param transformerSupplier
	 * @param stateStoreNames
	 * @return
	 * @see org.apache.kafka.streams.kstream.KStream#transform(org.apache.kafka.streams.kstream.TransformerSupplier, java.lang.String[])
	 */
	public <K1, V1> KStream<K1, V1> transform(TransformerSupplier<K, V, KeyValue<K1, V1>> transformerSupplier,
			String... stateStoreNames) {
		return delegate.transform(transformerSupplier, stateStoreNames);
	}

	/**
	 * @param <R>
	 * @param valueTransformerSupplier
	 * @param stateStoreNames
	 * @return
	 * @see org.apache.kafka.streams.kstream.KStream#transformValues(org.apache.kafka.streams.kstream.ValueTransformerSupplier, java.lang.String[])
	 */
	public <R> KStream<K, R> transformValues(ValueTransformerSupplier<V, R> valueTransformerSupplier,
			String... stateStoreNames) {
		return delegate.transformValues(valueTransformerSupplier, stateStoreNames);
	}

	/**
	 * @param processorSupplier
	 * @param stateStoreNames
	 * @see org.apache.kafka.streams.kstream.KStream#process(org.apache.kafka.streams.processor.ProcessorSupplier, java.lang.String[])
	 */
	public void process(ProcessorSupplier<K, V> processorSupplier, String... stateStoreNames) {
		delegate.process(processorSupplier, stateStoreNames);
	}

	/**
	 * @param <V1>
	 * @param <R>
	 * @param otherStream
	 * @param joiner
	 * @param windows
	 * @param keySerde
	 * @param thisValueSerde
	 * @param otherValueSerde
	 * @return
	 * @see org.apache.kafka.streams.kstream.KStream#join(org.apache.kafka.streams.kstream.KStream, org.apache.kafka.streams.kstream.ValueJoiner, org.apache.kafka.streams.kstream.JoinWindows, org.apache.kafka.common.serialization.Serde, org.apache.kafka.common.serialization.Serde, org.apache.kafka.common.serialization.Serde)
	 */
	public <V1, R> KStream<K, R> join(KStream<K, V1> otherStream, ValueJoiner<V, V1, R> joiner, JoinWindows windows,
			Serde<K> keySerde, Serde<V> thisValueSerde, Serde<V1> otherValueSerde) {
		return delegate.join(otherStream, joiner, windows, keySerde, thisValueSerde, otherValueSerde);
	}

	/**
	 * @param <V1>
	 * @param <R>
	 * @param otherStream
	 * @param joiner
	 * @param windows
	 * @return
	 * @see org.apache.kafka.streams.kstream.KStream#join(org.apache.kafka.streams.kstream.KStream, org.apache.kafka.streams.kstream.ValueJoiner, org.apache.kafka.streams.kstream.JoinWindows)
	 */
	public <V1, R> KStream<K, R> join(KStream<K, V1> otherStream, ValueJoiner<V, V1, R> joiner, JoinWindows windows) {
		return delegate.join(otherStream, joiner, windows);
	}

	/**
	 * @param <V1>
	 * @param <R>
	 * @param otherStream
	 * @param joiner
	 * @param windows
	 * @param keySerde
	 * @param thisValueSerde
	 * @param otherValueSerde
	 * @return
	 * @see org.apache.kafka.streams.kstream.KStream#outerJoin(org.apache.kafka.streams.kstream.KStream, org.apache.kafka.streams.kstream.ValueJoiner, org.apache.kafka.streams.kstream.JoinWindows, org.apache.kafka.common.serialization.Serde, org.apache.kafka.common.serialization.Serde, org.apache.kafka.common.serialization.Serde)
	 */
	public <V1, R> KStream<K, R> outerJoin(KStream<K, V1> otherStream, ValueJoiner<V, V1, R> joiner,
			JoinWindows windows, Serde<K> keySerde, Serde<V> thisValueSerde, Serde<V1> otherValueSerde) {
		return delegate.outerJoin(otherStream, joiner, windows, keySerde, thisValueSerde, otherValueSerde);
	}

	/**
	 * @param <V1>
	 * @param <R>
	 * @param otherStream
	 * @param joiner
	 * @param windows
	 * @return
	 * @see org.apache.kafka.streams.kstream.KStream#outerJoin(org.apache.kafka.streams.kstream.KStream, org.apache.kafka.streams.kstream.ValueJoiner, org.apache.kafka.streams.kstream.JoinWindows)
	 */
	public <V1, R> KStream<K, R> outerJoin(KStream<K, V1> otherStream, ValueJoiner<V, V1, R> joiner,
			JoinWindows windows) {
		return delegate.outerJoin(otherStream, joiner, windows);
	}

	/**
	 * @param <V1>
	 * @param <R>
	 * @param otherStream
	 * @param joiner
	 * @param windows
	 * @param keySerde
	 * @param otherValueSerde
	 * @return
	 * @see org.apache.kafka.streams.kstream.KStream#leftJoin(org.apache.kafka.streams.kstream.KStream, org.apache.kafka.streams.kstream.ValueJoiner, org.apache.kafka.streams.kstream.JoinWindows, org.apache.kafka.common.serialization.Serde, org.apache.kafka.common.serialization.Serde)
	 */
	public <V1, R> KStream<K, R> leftJoin(KStream<K, V1> otherStream, ValueJoiner<V, V1, R> joiner, JoinWindows windows,
			Serde<K> keySerde, Serde<V1> otherValueSerde) {
		return delegate.leftJoin(otherStream, joiner, windows, keySerde, otherValueSerde);
	}

	/**
	 * @param <V1>
	 * @param <R>
	 * @param otherStream
	 * @param joiner
	 * @param windows
	 * @return
	 * @see org.apache.kafka.streams.kstream.KStream#leftJoin(org.apache.kafka.streams.kstream.KStream, org.apache.kafka.streams.kstream.ValueJoiner, org.apache.kafka.streams.kstream.JoinWindows)
	 */
	public <V1, R> KStream<K, R> leftJoin(KStream<K, V1> otherStream, ValueJoiner<V, V1, R> joiner,
			JoinWindows windows) {
		return delegate.leftJoin(otherStream, joiner, windows);
	}

	/**
	 * @param <V1>
	 * @param <V2>
	 * @param table
	 * @param joiner
	 * @return
	 * @see org.apache.kafka.streams.kstream.KStream#leftJoin(org.apache.kafka.streams.kstream.KTable, org.apache.kafka.streams.kstream.ValueJoiner)
	 */
	public <V1, V2> KStream<K, V2> leftJoin(KTable<K, V1> table, ValueJoiner<V, V1, V2> joiner) {
		return delegate.leftJoin(table, joiner);
	}

	/**
	 * @param <W>
	 * @param reducer
	 * @param windows
	 * @param keySerde
	 * @param valueSerde
	 * @return
	 * @see org.apache.kafka.streams.kstream.KStream#reduceByKey(org.apache.kafka.streams.kstream.Reducer, org.apache.kafka.streams.kstream.Windows, org.apache.kafka.common.serialization.Serde, org.apache.kafka.common.serialization.Serde)
	 */
	public <W extends Window> KTable<Windowed<K>, V> reduceByKey(Reducer<V> reducer, Windows<W> windows,
			Serde<K> keySerde, Serde<V> valueSerde) {
		return delegate.reduceByKey(reducer, windows, keySerde, valueSerde);
	}

	/**
	 * @param <W>
	 * @param reducer
	 * @param windows
	 * @return
	 * @see org.apache.kafka.streams.kstream.KStream#reduceByKey(org.apache.kafka.streams.kstream.Reducer, org.apache.kafka.streams.kstream.Windows)
	 */
	public <W extends Window> KTable<Windowed<K>, V> reduceByKey(Reducer<V> reducer, Windows<W> windows) {
		return delegate.reduceByKey(reducer, windows);
	}

	/**
	 * @param reducer
	 * @param keySerde
	 * @param valueSerde
	 * @param name
	 * @return
	 * @see org.apache.kafka.streams.kstream.KStream#reduceByKey(org.apache.kafka.streams.kstream.Reducer, org.apache.kafka.common.serialization.Serde, org.apache.kafka.common.serialization.Serde, java.lang.String)
	 */
	public KTable<K, V> reduceByKey(Reducer<V> reducer, Serde<K> keySerde, Serde<V> valueSerde, String name) {
		return delegate.reduceByKey(reducer, keySerde, valueSerde, name);
	}

	/**
	 * @param reducer
	 * @param name
	 * @return
	 * @see org.apache.kafka.streams.kstream.KStream#reduceByKey(org.apache.kafka.streams.kstream.Reducer, java.lang.String)
	 */
	public KTable<K, V> reduceByKey(Reducer<V> reducer, String name) {
		return delegate.reduceByKey(reducer, name);
	}

	/**
	 * @param <T>
	 * @param <W>
	 * @param initializer
	 * @param aggregator
	 * @param windows
	 * @param keySerde
	 * @param aggValueSerde
	 * @return
	 * @see org.apache.kafka.streams.kstream.KStream#aggregateByKey(org.apache.kafka.streams.kstream.Initializer, org.apache.kafka.streams.kstream.Aggregator, org.apache.kafka.streams.kstream.Windows, org.apache.kafka.common.serialization.Serde, org.apache.kafka.common.serialization.Serde)
	 */
	public <T, W extends Window> KTable<Windowed<K>, T> aggregateByKey(Initializer<T> initializer,
			Aggregator<K, V, T> aggregator, Windows<W> windows, Serde<K> keySerde, Serde<T> aggValueSerde) {
		return delegate.aggregateByKey(initializer, aggregator, windows, keySerde, aggValueSerde);
	}

	/**
	 * @param <T>
	 * @param <W>
	 * @param initializer
	 * @param aggregator
	 * @param windows
	 * @return
	 * @see org.apache.kafka.streams.kstream.KStream#aggregateByKey(org.apache.kafka.streams.kstream.Initializer, org.apache.kafka.streams.kstream.Aggregator, org.apache.kafka.streams.kstream.Windows)
	 */
	public <T, W extends Window> KTable<Windowed<K>, T> aggregateByKey(Initializer<T> initializer,
			Aggregator<K, V, T> aggregator, Windows<W> windows) {
		return delegate.aggregateByKey(initializer, aggregator, windows);
	}

	/**
	 * @param <T>
	 * @param initializer
	 * @param aggregator
	 * @param keySerde
	 * @param aggValueSerde
	 * @param name
	 * @return
	 * @see org.apache.kafka.streams.kstream.KStream#aggregateByKey(org.apache.kafka.streams.kstream.Initializer, org.apache.kafka.streams.kstream.Aggregator, org.apache.kafka.common.serialization.Serde, org.apache.kafka.common.serialization.Serde, java.lang.String)
	 */
	public <T> KTable<K, T> aggregateByKey(Initializer<T> initializer, Aggregator<K, V, T> aggregator,
			Serde<K> keySerde, Serde<T> aggValueSerde, String name) {
		return delegate.aggregateByKey(initializer, aggregator, keySerde, aggValueSerde, name);
	}

	/**
	 * @param <T>
	 * @param initializer
	 * @param aggregator
	 * @param name
	 * @return
	 * @see org.apache.kafka.streams.kstream.KStream#aggregateByKey(org.apache.kafka.streams.kstream.Initializer, org.apache.kafka.streams.kstream.Aggregator, java.lang.String)
	 */
	public <T> KTable<K, T> aggregateByKey(Initializer<T> initializer, Aggregator<K, V, T> aggregator, String name) {
		return delegate.aggregateByKey(initializer, aggregator, name);
	}

	/**
	 * @param <W>
	 * @param windows
	 * @param keySerde
	 * @return
	 * @see org.apache.kafka.streams.kstream.KStream#countByKey(org.apache.kafka.streams.kstream.Windows, org.apache.kafka.common.serialization.Serde)
	 */
	public <W extends Window> KTable<Windowed<K>, Long> countByKey(Windows<W> windows, Serde<K> keySerde) {
		return delegate.countByKey(windows, keySerde);
	}

	/**
	 * @param <W>
	 * @param windows
	 * @return
	 * @see org.apache.kafka.streams.kstream.KStream#countByKey(org.apache.kafka.streams.kstream.Windows)
	 */
	public <W extends Window> KTable<Windowed<K>, Long> countByKey(Windows<W> windows) {
		return delegate.countByKey(windows);
	}

	/**
	 * @param keySerde
	 * @param name
	 * @return
	 * @see org.apache.kafka.streams.kstream.KStream#countByKey(org.apache.kafka.common.serialization.Serde, java.lang.String)
	 */
	public KTable<K, Long> countByKey(Serde<K> keySerde, String name) {
		return delegate.countByKey(keySerde, name);
	}

	/**
	 * @param name
	 * @return
	 * @see org.apache.kafka.streams.kstream.KStream#countByKey(java.lang.String)
	 */
	public KTable<K, Long> countByKey(String name) {
		return delegate.countByKey(name);
	}


}
