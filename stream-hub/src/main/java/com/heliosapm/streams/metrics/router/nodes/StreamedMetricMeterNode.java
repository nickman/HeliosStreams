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
import java.util.concurrent.atomic.LongAdder;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.jmx.export.annotation.ManagedAttribute;

import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.StreamedMetricValue;
import com.heliosapm.streams.metrics.router.StreamHubKafkaClientSupplier;
import com.heliosapm.streams.metrics.router.util.TimeWindowSummary;
import com.heliosapm.streams.serialization.HeliosSerdes;

/**
 * <p>Title: StreamedMetricMeterNode</p>
 * <p>Description: Provides metering for incoming metrics where the total number of
 * {@link StreamedMetric}s ingested will be accumulated in fixed time windows then forwarded
 * as a new metric with the number of incidents as the value. There are some variables: <ol>
 *  <li><b>{@link #ignoreValues}</b>: </li>
 *  <li><b>{@link #ignoreDoubles}</b>: </li>
 *  <li><b>{@link #reportInSeconds}</b>: If true, the final count will be adjusted to report events per second.</li>
 * 
 * </ol></p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.router.nodes.StreamedMetricMeterNode</code></p>
 */

public class StreamedMetricMeterNode extends AbstractMetricStreamNode  {
	/** Indicates if the actual value of a metric should be ignored and to focus only on the count of the metric id. Defaults to false. */
	private boolean ignoreValues = false;
	/** Indicates if metrics that have a value type of {@link Double} should be ignored. Defaults to false. */
	private boolean ignoreDoubles = false;
	/** Indicates if the reported value published upstream should be adjusted to per/Second (i.e. the {@link #windowSize} divided by 1000). Defaults to true. */
	private boolean reportInSeconds = true;
	/** The time window summary to report the final summary using the window start, end (default) or middle time */
	private TimeWindowSummary windowTimeSummary = TimeWindowSummary.END;
	/** The divisor to report tps (windowSize/1000) */
	private double tpsDivisor = 5D;
	/** The number of outbounds sent in the last punctuation */
	private final LongAdder lastOutbound = new LongAdder();
	/** The provided aggregation window */
	private WindowAggregation<String, StreamedMetricMeterAggregator, StreamedMetric> aggregationWindow = null;
	/** The producer created to forward aggregated metrics */
	private Producer<String, StreamedMetric> producer = null;
	

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.router.nodes.MetricStreamNode#configure(org.apache.kafka.streams.kstream.KStreamBuilder)
	 */
	@Override
	public void configure(final KStreamBuilder streamBuilder) {
		log.info("Source Topics: {}", Arrays.toString(sourceTopics));
		log.info("Sink Topic: [{}]", sinkTopic);
		// ========================= PROCESSOR STYLE =========================
		// persistentStores
//		final KeyValueFactory<String, long[]> factory = Stores.create(storeName).withStringKeys().withValues(HeliosSerdes.TIMEWINDOW_VALUE_SERDE);
//		final StateStoreSupplier ss = persistentStores ? factory.persistent().build() : factory.inMemory().build();
//		
//		streamBuilder
//			.addSource("MeterMetricProcessorSource", HeliosSerdes.STRING_SERDE.deserializer(), HeliosSerdes.STREAMED_METRIC_SERDE.deserializer(), sourceTopics)			
//			.addProcessor("MeterMetricProcessor", this, "MeterMetricProcessorSource")
//			.addStateStore(ss, "MeterMetricProcessor")
//			.addSink("MeterMetricProcessorSink", sinkTopic, HeliosSerdes.STRING_SERDE.serializer(), HeliosSerdes.STREAMED_METRIC_SERDE.serializer(), "MeterMetricProcessor");
		// ===================================================================
		streamBuilder
			.stream(HeliosSerdes.STRING_SERDE, HeliosSerdes.STREAMED_METRIC_SERDE, sourceTopics)
			.foreach((k,v) -> {
				aggregationWindow.aggregate(k, cleanIn(v));
				inboundCount.increment();
			});		
	}
	
	/**
	 * Cleans the incoming metric, applying any filtering or modification rules and returning the metric to be aggregated
	 * @param sm The metric to clean
	 * @return The metric to aggregate
	 */
	protected StreamedMetric cleanIn(final StreamedMetric sm) {
		if(ignoreValues) return sm.isValued() ? new StreamedMetric(sm.getTimestamp(), sm.getMetricName(), sm.getTags()) : sm.forValue(1L);
		if(ignoreDoubles) return (sm.isValued() && ((StreamedMetricValue)sm).isDoubleValue()) ? new StreamedMetric(sm.getTimestamp(), sm.getMetricName(), sm.getTags()) : sm.forValue(1L);
		return sm.forValue(1L);
	}
	
	/**
	 * Cleans the outgoing metric, applying any filtering or modification rules and returning the metric to be forwarded
	 * @param sm The metric to clean
	 * @return The metric to forward
	 */
	protected StreamedMetric cleanOut(final StreamedMetric sm) {
		if(reportInSeconds) {
			final StreamedMetricValue smv = (StreamedMetricValue)sm;
			return new StreamedMetricValue(smv.getTimestamp(), calcRate(smv.getValueNumber().doubleValue()), sm.getMetricName(), sm.getTags());
		}
		return sm;		
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.router.nodes.AbstractMetricStreamNode#onStart(com.heliosapm.streams.metrics.router.StreamHubKafkaClientSupplier, org.apache.kafka.streams.KafkaStreams)
	 */
	@Override
	public void onStart(final StreamHubKafkaClientSupplier clientSupplier, final KafkaStreams kafkaStreams) {		
		super.onStart(clientSupplier, kafkaStreams);
		producer = clientSupplier.getProducer(HeliosSerdes.STRING_SERDE, HeliosSerdes.STREAMED_METRIC_SERDE);
		aggregationWindow.addAction((stream, keys) -> stream.forEach(kv -> { 
			outboundCount.increment();
			producer.send(new ProducerRecord<String, StreamedMetric>(sinkTopic, fullKey ? kv.key : kv.value.getMetricName(), cleanOut(kv.value)));
			producer.flush();
		}));
	}
	
	
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.router.nodes.AbstractMetricStreamNode#close()
	 */
	@Override
	public void close() {		
		if(producer!=null) {
			try { producer.close(); } catch (Exception x) {/* No Op */}
		}
		super.close();		
	}
	
	/**
	 * Calculates the TPS rate
	 * @param count the number of events received during the window period
	 * @return the number of events per second
	 */
	public double calcRate(final double count) {
		if(count==0D) return 0D;
		return count/tpsDivisor;
	}
	
	/**
	 * Sets the aggregation window that will aggregated metered metrics
	 * @param aggregationWindow ther aggregation window instance
	 */
	@Required
	public void setAggregationWindow(final WindowAggregation<String, StreamedMetricMeterAggregator, StreamedMetric> aggregationWindow) {
		this.aggregationWindow = aggregationWindow;
		tpsDivisor = aggregationWindow.getWindowDuration();		
	}

	/**
	 * Indicates if metrics with values are counted as 1 event or the number of the value
	 * @return true if values are ignored, false otherwise
	 */
	@ManagedAttribute(description="Indicates if metrics with values are counted as 1 event or the number of the value")
	public boolean isIgnoreValues() {
		return ignoreValues;
	}

	/**
	 * Sets if metrics with values are counted as 1 event
	 * @param ignoreValues true to treat metrics with values as 1 event, false to use the value 
	 */
	public void setIgnoreValues(final boolean ignoreValues) {
		this.ignoreValues = ignoreValues;
	}

	/**
	 * Indicates if metrics with double values are counted as 1 event or the number of the value
	 * @return true if values are ignored, false otherwise
	 */
	@ManagedAttribute(description="Indicates if metrics with double values are counted as 1 event or the number of the value")
	public boolean isIgnoreDoubles() {
		return ignoreDoubles;
	}

	/**
	 * Sets if metrics with double values are counted as 1 event
	 * @param ignoreDoubles true to treat metrics with double values as 1 event, false to use the value 
	 */
	public void setIgnoreDoubles(final boolean ignoreDoubles) {
		this.ignoreDoubles = ignoreDoubles;
	}

	/**
	 * Indicates if final rates are reported in events/sec or the natural rate of the configured window
	 * @return true if final rates are reported in tps, false otherwise
	 */
	@ManagedAttribute(description="Indicates if final rates are reported in events/sec")
	public boolean isReportInSeconds() {
		return reportInSeconds;
	}

	/**
	 * Specifies if final rates are reported in events/sec or the natural rate of the configured window
	 * @param reportInSeconds true to report in tps, false otherwise
	 */
	public void setReportInSeconds(boolean reportInSeconds) {
		this.reportInSeconds = reportInSeconds;
	}

	/**
	 * Returns the time window summarization strategy
	 * @return the time window summarization strategy
	 */
	@ManagedAttribute(description="The time window summarization strategy")
	public String getWindowTimeSummary() {
		return windowTimeSummary.name();
	}

	/**
	 * Sets the time window summarization strategy
	 * @param windowSum The time window summarization strategy
	 */
	public void setWindowTimeSummary(final TimeWindowSummary windowSum) {
		this.windowTimeSummary = windowSum;
	}

	/**
	 * Returns the number of sunk events in the last punctuation
	 * @return the number of sunk events in the last punctuation
	 */
	@ManagedAttribute(description="The number of sunk events in the last punctuation")
	public long getLastOutbound() {
		return lastOutbound.longValue();
	}



}
