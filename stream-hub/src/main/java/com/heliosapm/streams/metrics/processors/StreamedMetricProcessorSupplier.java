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
package com.heliosapm.streams.metrics.processors;

import java.util.Set;

import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

import com.heliosapm.streams.metrics.store.StateStoreDefinition;

/**
 * <p>Title: StreamedMetricProcessorSupplier</p>
 * <p>Description: Defines a streamed metric processor</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.processors.impl.StreamedMetricProcessorSupplier</code></p>
 * @param <K> The expected type of the source key
 * @param <V> The expected type of the source value
 * @param <SK> The expected type of the sink key
 * @param <SV> The expected type of the sink value
 */

public interface StreamedMetricProcessorSupplier<K, V, SK, SV> extends ProcessorSupplier<K, V> {
	
	
	/** The suffix for the source name */
	public static final String SOURCE_NAME_SUFFIX = ".Source";
	/** The suffix for the processor name */
	public static final String PROCESSOR_NAME_SUFFIX = ".Processor";
	/** The suffix for the sink name */
	public static final String SINK_NAME_SUFFIX = ".Sink";
	
	/**
	 * Returns the state store definitions for this processor supplier
	 * @return the state store definitions for this processor supplier
	 */
	public Set<StateStoreDefinition<?, ?>> getStateStoreDefinitions();
	
	/**
	 * Returns the names of the declared state stores
	 * @return the names of the declared state stores
	 */
	public String[] getStateStoreNames();
	
	/**
	 * Directs the processor supplier to configure itself using the passed builder
	 * @param builder the builder
	 * @param textLineSourceName The name of the text line source
	 * @return this supplier's name that the parent will use as a sink
	 */
	public String configure(TopologyBuilder builder, String textLineSourceName);
	
	
	/**
	 * Stops all processors created by this supplier
	 */
	public void shutdown();
}
