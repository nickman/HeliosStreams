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
package com.heliosapm.streams.metrics.router.nodes;

import java.util.Arrays;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.serialization.HeliosSerdes;

/**
 * <p>Title: TextToBinaryTransformNode</p>
 * <p>Description: Listens on the {@link #sourceTopics} topics for string based {@link StreamedMetric} messages,
 * transforms them to binary format and forwards them to the {@link #sinkTopic} topic. The key of the incoming
 * messages is ignored as this node is primarilly intended to supply routing and meaningful keys
 * for metric publishers that do not supply a key that is load balancing friendly.</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.router.nodes.TextToBinaryTransformNode</code></p>
 */

public class TextToBinaryTransformNode extends AbstractMetricStreamNode {
	
	/** The mapper to transform the text message to a binary StreamedMetric */
	protected final KeyValueMapper<String, String, KeyValue<String, StreamedMetric>> mapper
	 = new KeyValueMapper<String, String, KeyValue<String, StreamedMetric>>() {
		@Override
		public KeyValue<String, StreamedMetric> apply(final String key, final String value) {
			try {
				final StreamedMetric sm = StreamedMetric.fromString(value);
				inboundCount.increment();
				outboundCount.increment();
				return new KeyValue<String, StreamedMetric>(fullKey ? sm.metricKey() : sm.getMetricName(), sm);
			} catch (Throwable t) {
				failedCount.increment();
				return null;
			}
		}
	};
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.router.nodes.MetricStreamNode#configure(org.apache.kafka.streams.kstream.KStreamBuilder)
	 */
	@Override
	public void configure(final KStreamBuilder streamBuilder) {
		log.info("Source Topics: {}", Arrays.toString(sourceTopics));
		log.info("Sink Topic: [{}]", sinkTopic);

		streamBuilder.stream(HeliosSerdes.STRING_SERDE, HeliosSerdes.STRING_SERDE, sourceTopics)
		.map(mapper)		
		.to(HeliosSerdes.STRING_SERDE, HeliosSerdes.STREAMED_METRIC_SERDE, sinkTopic);
//		.foreach((a,b) -> outboundCount.increment());
	}


}
