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

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.Stores.KeyValueFactory;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedOperationParameter;
import org.springframework.jmx.export.annotation.ManagedOperationParameters;

import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.StreamedMetricValue;
import com.heliosapm.streams.metrics.router.util.TimeWindowSummary;
import com.heliosapm.streams.serialization.HeliosSerdes;

/**
 * <p>Title: StreamedMetricMeterNode</p>
 * <p>Description: Provides metering for incoming metrics where the total number of
 * {@link StreamedMetric}s ingested will be accumulated in fixed time windows then forwarded
 * as a new metric with the number of incidents as the value. There are some variables: <ol>
 * 	<li><b>{@link #windowSize}</b>: The size of the window period to accumulate within in ms.</li>
 *  <li><b>{@link #ignoreValues}</b>: </li>
 *  <li><b>{@link #ignoreDoubles}</b>: </li>
 *  <li><b>{@link #reportInSeconds}</b>: If true, the final count will be adjusted to report events per second.</li>
 * 
 * </ol></p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.router.nodes.StreamedMetricMeterNode</code></p>
 */

public class StreamedMetricMeterNode extends AbstractMetricStreamNode implements ProcessorSupplier<String, StreamedMetric>, Runnable {
	/** The accumulation window size in ms. Defaults to 5000 */
	protected long windowSize = 5000;
	/** Indicates if the actual value of a metric should be ignored and to focus only on the count of the metric id. Defaults to false. */
	protected boolean ignoreValues = false;
	/** Indicates if metrics that have a value type of {@link Double} should be ignored. Defaults to false. */
	protected boolean ignoreDoubles = false;
	/** Indicates if the reported value published upstream should be adjusted to per/Second (i.e. the {@link #windowSize} divided by 1000). Defaults to true. */
	protected boolean reportInSeconds = true;
	/** The time window summary to report the final summary using the window start, end (default) or middle time */
	protected TimeWindowSummary windowTimeSummary = TimeWindowSummary.END;
		
	/** The divisor to report tps (windowSize/1000) */
	protected double tpsDivisor = 5D;
	/** The number of outbounds sent in the last punctuation */
	protected final LongAdder lastOutbound = new LongAdder();
	/** Circular counter incremented each time a processor instance's punctuate is invoked, resetting once all known instances have reprocessed */
	protected final AtomicInteger processorInvokes = new AtomicInteger(0);
	/** The number of processor instances created */
	protected final AtomicInteger processorInstances = new AtomicInteger(0);
	/** A No Op KeyValue that should be ignored */
	private static final KeyValue<String, StreamedMetric> OUT = new KeyValue<String, StreamedMetric>("DROPME", null);
	
	/** The processor's key value store */
	protected volatile KeyValueStore<String, long[]> periodEventCounts = null;

	protected String storeName = null;
	
	/** The key value mapper that generates the final rolled up StreamedMetric */
	protected KeyValueMapper<Windowed<String>, long[], KeyValue<String,StreamedMetric>> rollupMapper = 
			new KeyValueMapper<Windowed<String>, long[], KeyValue<String,StreamedMetric>>() {
				@Override
				public KeyValue<String, StreamedMetric> apply(final Windowed<String> key, final long[] timeValuePair) {
					if(timeValuePair[0] < key.window().start() || timeValuePair[0] > key.window().end()) {
						return OUT;
					}
					final double aggregatedValue = reportInSeconds ? calcRate(timeValuePair[1]) : (double)timeValuePair[1];
					final KeyValue<String, StreamedMetric> kv = new KeyValue<String, StreamedMetric>(key.key(), 
						StreamedMetric.fromKey(
								windowTimeSummary.time(key.window()),
								key.key(),
								aggregatedValue)
					);
					outboundCount.increment();
					//log.info("{}", kv.value);
					return kv;
				}
	};
	
	
	/**
	 * Increments the punctuation period event counter, resetting on the first update of the period
	 * @param itemsFlushed The number of events processed by one processor
	 */
	protected void incrementFlushes(final long itemsFlushed) {
		if(processorInvokes.compareAndSet(processorInstances.get(), 0)) {
			lastOutbound.reset();
		} else {
			processorInvokes.incrementAndGet();
			lastOutbound.add(itemsFlushed);
		}
		
	}
	
	
	@Override
	public Processor<String, StreamedMetric> get() {
		processorInstances.incrementAndGet();
		return new Processor<String, StreamedMetric>() {
			ProcessorContext context = null;

			@SuppressWarnings("unchecked")
			@Override
			public void init(final ProcessorContext context) {
				this.context = context;
				context.schedule(windowSize);
				if(periodEventCounts==null) {
					periodEventCounts = (KeyValueStore<String, long[]>) context.getStateStore(storeName);
				}
			}

			@Override
			public void process(final String key, final StreamedMetric sm) {
				inboundCount.increment();
				final boolean valued = sm.isValued();
				final long timestamp = sm.getTimestamp();
				final long increment;
				if(ignoreValues || !valued) {
					increment = 1L;
				} else {
					final StreamedMetricValue smv = !valued ? sm.forValue(1L) : sm.forValue();
					if(smv.isDoubleValue()) {
						increment = ignoreDoubles ? 1L : (long)smv.getDoubleValue();
					} else {
						increment = smv.getLongValue();
					}
				}
				long[] aggr = periodEventCounts.get(sm.metricKey());
				if(aggr==null) {
					aggr = new long[]{timestamp, timestamp, increment};
				} else {
					if(timestamp < aggr[0]) {
						// FIXME: DROP & Increment Count
						return;
					}
					aggr[1] = sm.getTimestamp();
					aggr[2] += increment; 
				}
				periodEventCounts.put(sm.metricKey(), aggr);
				periodEventCounts.flush();				
			}

			@Override
			public void punctuate(final long timestamp) {
				long sent = 0;
				final KeyValueIterator<String, long[]> iter = periodEventCounts.all();
				try {
					final long streamTime = context.timestamp();  // the smallest timestamp
					final HashSet<KeyValue<String, long[]>> saveValues = new HashSet<KeyValue<String, long[]>>(); 
					while(iter.hasNext()) {
						final KeyValue<String, long[]> kv = iter.next();
						if(kv.value!=null) {
							final double val;
							if(kv.value[1] < streamTime) {
								if(kv.value[2]> 0L) {
									kv.value[2] = 0L;		
									saveValues.add(kv);
								}
								val = 0D;
							} else {
								val = reportInSeconds ? calcRate(kv.value[2]) : kv.value[2];
							}
							final StreamedMetric sm = StreamedMetric.fromKey(streamTime, kv.key, val);
							context.forward(kv.key, sm);
							outboundCount.increment();
							sent++;
						}						
					}
					incrementFlushes(sent);
					for(KeyValue<String, long[]> toSave: saveValues) {
						periodEventCounts.put(toSave.key, toSave.value);
					}
					periodEventCounts.flush();					
				} finally {
					iter.close();
					context.commit();
				}
			}

			@Override
			public void close() {
				periodEventCounts.close();
			}
			
		};
	}
	
	/**
	 * <p>Task periodically executed to continue sending zeros for idle metrics until the idle timeout kicks in.</p>
	 * {@inheritDoc}
	 * @see java.lang.Runnable#run()
	 */
	public void run() {
		
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.router.nodes.MetricStreamNode#configure(org.apache.kafka.streams.kstream.KStreamBuilder)
	 */
	@Override
	public void configure(final KStreamBuilder streamBuilder) {
		log.info("Source Topics: {}", Arrays.toString(sourceTopics));
		log.info("Sink Topic: [{}]", sinkTopic);
		storeName = "MeteringMetricAccumulator-" + nodeName;
		// ========================= PROCESSOR STYLE =========================
		// persistentStores
		final KeyValueFactory<String, long[]> factory = Stores.create(storeName).withStringKeys().withValues(HeliosSerdes.TIMEWINDOW_VALUE_SERDE);
		final StateStoreSupplier ss = persistentStores ? factory.persistent().build() : factory.inMemory().build();
		
		streamBuilder
			.addSource("MeterMetricProcessorSource", HeliosSerdes.STRING_SERDE.deserializer(), HeliosSerdes.STREAMED_METRIC_SERDE.deserializer(), sourceTopics)			
			.addProcessor("MeterMetricProcessor", this, "MeterMetricProcessorSource")
			.addStateStore(ss, "MeterMetricProcessor")
			.addSink("MeterMetricProcessorSink", sinkTopic, HeliosSerdes.STRING_SERDE.serializer(), HeliosSerdes.STREAMED_METRIC_SERDE.serializer(), "MeterMetricProcessor");
		// ===================================================================
	}
	
	/**
	 * Writes the content of the time window to a file
	 * @param dirName The directory name to write to
	 */
	@ManagedOperation(description="Writes the content of the time window to a file")
	@ManagedOperationParameters({@ManagedOperationParameter(name="OutputDirectory", description="The optional directory to write to. Defaults to tmpdir")})
	public void writeStateToFile(final String dirName) {
		final File dir = new File((dirName==null || dirName.trim().isEmpty()) ? System.getProperty("java.io.tmpdir") : dirName.trim());
		final File f = new File(dir, "MeteringWindowAccumulator-" + nodeName + "-state.txt");
		f.delete();
		log.info("Writing file to [{}]...", f);
//		meteredWindow.writeAsText(f.getAbsolutePath(), HeliosSerdes.WINDOWED_STRING_SERDE, HeliosSerdes.TIMEVALUE_PAIR_SERDE);
		log.info("Write file to [{}]", f);
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
	 * Returns the configured window size in ms. 
	 * @return the window size
	 */
	@ManagedAttribute(description="The configured window size in ms.")
	public long getWindowSize() {
		return windowSize;
	}

	/**
	 * Sets the window size in ms. Should be a multiple of 1000
	 * @param windowSize the window size to set
	 */
	public void setWindowSize(final long windowSize) {
		if(windowSize < 1000) throw new IllegalArgumentException("Invalid window size: [" + windowSize + "]");
		this.windowSize = windowSize;
		tpsDivisor = this.windowSize/1000D;
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

	/**
	 * Returns the number of created processor instances
	 * @return the number of created processor instances
	 */
	@ManagedAttribute(description="The number of created processor instances")
	public int getProcessorInstances() {
		return processorInstances.get();
	}





}
