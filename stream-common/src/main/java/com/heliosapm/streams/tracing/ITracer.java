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
package com.heliosapm.streams.tracing;

import java.io.Closeable;
import java.util.Map;

import javax.management.ObjectName;

import com.heliosapm.utils.config.ConfigurationHelper;

/**
 * <p>Title: ITracer</p>
 * <p>Description: Defines the base tracer interface</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tracing.ITracer</code></p>
 */

public interface ITracer extends Closeable {
	/** The config prop for the buffer extend percentage */
	public static final String CONF_EXT_PCT = "tracer.buff.ext";
	/** The default buffer extend percentage */
	public static final float DEFAULT_EXT_PCT = 0.5f;
	/** The config prop for the maximum number of traces to buffer before flushing */
	public static final String CONF_MAX_TRACES = "tracer.buff.max";
	/** The default maximum number of traces to buffer before flushing */
	public static final int DEFAULT_MAX_TRACES = 128;
	/** The config prop for the initial buffer capacity in bytes */
	public static final String CONF_INIT_SIZE = "tracer.buff.size";
	/** The configured default number of traces to buffer before an auto-flush */
	public static final int MAX_TRACES = ConfigurationHelper.getIntSystemThenEnvProperty(CONF_MAX_TRACES, DEFAULT_MAX_TRACES);	

	/** The conf property key for the metric name segment delimiter */
	public static final String CONF_METRIC_SEG_DELIM = "tracer.metricseg.delim";	
	/** The default metric name segment delimiter */
	public static final char DEFAULT_METRIC_SEG_DELIM = '.';
	
	
	
	// =====================================
	//  State ops
	// =====================================

	/**
	 * Clears the metric name segments, tags, tag key stack
	 * and checkpoint stack
	 * @return this trace
	 */
	public ITracer clear();
	
	/**
	 * Clears the metric name segments, tags and tag key stack 
	 * but <b>not</b> the checkpoint stack</b>
	 * @return this trace
	 */
	public ITracer reset();
	
	/**
	 * Pushes the state of this tracer onto the checkpoint stack
	 * @return this tracer
	 */
	public ITracer checkpoint();
	
	/**
	 * Pops this tracer's state off the checkpoint stack
	 * @return this tracer with it's new state
	 */
	public ITracer popCheckpoint();
	
	// =====================================
	//  Cache lookup ops
	// =====================================
	
	
	// =====================================
	//  ObjectName ops
	// =====================================
	
	/**
	 * Clears the current metric name stack and tags and replaces with
	 * the passed ObjectName, where the metric name stack is set to 
	 * the split domain and the tags are set to the properties
	 * @param objectName The ObjectName to set
	 * @return this tracer
	 */
	public ITracer objectName(final ObjectName objectName);
	
	/**
	 * Clears the current metric name stack and tags and replaces with
	 * the passed ObjectName string, where the metric name stack is set to 
	 * the split domain and the tags are set to the properties
	 * @param objectName The ObjectName string to set
	 * @return this tracer
	 */
	public ITracer objectName(final String objectName);
	
	
	// =====================================
	//  Metric segment ops
	// =====================================
	
	/**
	 * Sets the full metric segment, discarding the existing
	 * @param fullSegment The full metric name segment
	 * @return this tracer
	 */
	public ITracer seg(final String fullSegment);
	
	/**
	 * Appends a segment on the end of the existing segments
	 * @param segment The segment to append
	 * @return this tracer
	 */
	public ITracer pushSeg(final String segment);
	
	/**
	 * Pops the last segment off the end of the existing segments
	 * @return this tracer
	 */
	public ITracer popSeg();
	
	/**
	 * Pops the last <code>n</code> segments off the end of the existing segments
	 * @param n the number of segments to pop
	 * @return this tracer
	 */
	public ITracer popSeg(final int n);
	
	/**
	 * Returns the current full metric name segment
	 * @return the current full metric name segment
	 */
	public String seg();
	
	/**
	 * Returns the current tags
	 * @return the current tags
	 */
	public Map<String, String> tags();
	
	/**
	 * Returns the current metric name segments
	 * @return the current metric name segments
	 */
	public String[] segs();
	
	// =====================================
	//  Tag Pair ops
	// =====================================

	/**
	 * Sets the current tags, discarding the current state
	 * @param tags The tags to set
	 * @return this tracer
	 */
	public ITracer tags(final Map<String, String> tags);
	
	/**
	 * Sets the current tags, in the form of <b><code>t1, v1, t2, v2....tn, vn</code></b>, discarding the current state.
	 * An odd number of arguments won't work....
	 * @param tags The tags to set
	 * @return this tracer
	 */
	public ITracer tags(final String... tags);
	
	/**
	 * Pushes a tag pair onto the tag stack in the form of <b><code>K=V</code></b>
	 * @param tagPair The tag pair in the form of <b><code>K=V</code></b>
	 * @return this tracer
	 */
	public ITracer pushTagPair(final String tagPair);
	
	/**
	 * Pushes a tag pair onto the tag stack
	 * @param tagKey The tag key
	 * @param tagValue The tag value
	 * @return this tracer
	 */
	public ITracer pushTag(final String tagKey, final String tagValue);
	
	/**
	 * Pushes a map of tags onto the tag stack
	 * @param tags The map of tags to push
	 * @return this tracer
	 */
	public ITracer pushTags(final Map<String, String> tags);
	
	/**
	 * Pops the most recent tag pair off the tag stack 
	 * @return this tracer.
	 */
	public ITracer popTag();
	
	/**
	 * Pops the most recent <code>n</code> tag pairs off the tag stack
	 * @param n The number of tag pairs to pop off the tag stack
	 * @return this tracer
	 */
	public ITracer popTags(final int n);
	
	/**
	 * Removes tag pairs from the tag stack where the key is equal to any of the passed keys
	 * @param tagKeys The keys to remove from the tag stack
	 * @return this tracer
	 */
	public ITracer popTag(final String...tagKeys);
	
	/**
	 * Pushes the passed tag keys onto the tag key stack.
	 * This method should be followed by a call to trace(value, ts, tagValue....)
	 * where the passed values will be paired up with these keys by order index.
	 * Don't pass a different number of keys now than the number of values later.
	 * If you don't know what this is for, best not use it.
	 * @param keys The keys to push onto the tag key stack
	 * @return this tracer
	 */
	public ITracer pushKeys(final String... keys);
	
	/**
	 * Clears the tag key stack.
	 * @return this tracer.
	 */
	public ITracer popKeys();
	
	/**
	 * Pops <code>n</code> keys off the tag key stack.
	 * @param n The number of keys to pop off the tag key stack.
	 * @return this tracer.
	 */
	public ITracer popKeys(final int n);
	
	
	/**
	 * Pops one tag key of the tag key stack.
	 * @return this tracer.
	 */
	public ITracer popKey();
	
	// =====================================
	//  Timestamp ops
	// =====================================
	
	/**
	 * Sets the stateful timestamp which will be used for all subsequent
	 * trace/dtrace calls without a specified timestamp until cleared by {@link #popTs()}.
	 * The ts in state is cleared by {@link #reset()} and {@link #clear()}, 
	 * but is not saved in a {@link #checkpoint()}.
	 * @param timestamp The ms. timestamp to set
	 * @return this tracer
	 */
	public ITracer pushTs(final long timestamp);
	
	/**
	 * Clears the pushed timestamp
	 * @return this tracer
	 */
	public ITracer popTs();
	
	// =====================================
	//  Trace ops
	// =====================================
	
	/**
	 * Traces the passed value with the passed timestamp using the metric name and tags in state.
	 * @param value The value to trace
	 * @param timestamp The timestamp of the metric
	 * @param tagValues The optional tag values that will be matched up with the pushed keys in the tag key stack.
	 * @return this tracer
	 */
	public ITracer trace(final long value, final long timestamp, final String... tagValues);
	
	/**
	 * Traces the passed value with the passed timestamp using the metric name and tags in state.
	 * @param value The value to trace
	 * @param timestamp The timestamp of the metric
	 * @param tagValues The optional tag values that will be matched up with the pushed keys in the tag key stack.
	 * @return this tracer
	 */
	public ITracer trace(final double value, final long timestamp, final String... tagValues);
	
	/**
	 * Traces the delta of the passed value and the prior traced value with the passed timestamp using the metric name and tags in state.
	 * If there is no prior value, the delta state is captured, but no trace is emitted.
	 * @param value The value to trace
	 * @param timestamp The timestamp of the metric
	 * @param tagValues The optional tag values that will be matched up with the pushed keys in the tag key stack.
	 * @return this tracer
	 */
	public ITracer dtrace(final long value, final long timestamp, final String... tagValues);
	
	/**
	 * Traces the delta of the passed value and the prior traced value with the passed timestamp using the metric name and tags in state.
	 * If there is no prior value, the delta state is captured, but no trace is emitted.
	 * @param value The value to trace
	 * @param timestamp The timestamp of the metric
	 * @param tagValues The optional tag values that will be matched up with the pushed keys in the tag key stack.
	 * @return this tracer
	 */
	public ITracer dtrace(final double value, final long timestamp, final String... tagValues);
	
	/**
	 * Traces the passed value with the pushed timestamp in state using the metric name and tags in state.
	 * Throws an exception if no timestamp is in state (set by {@link #pushTs(long)}.
	 * @param value The value to trace
	 * @param tagValues The optional tag values that will be matched up with the pushed keys in the tag key stack.
	 * @return this tracer
	 */
	public ITracer trace(final long value, final String... tagValues);
	
	/**
	 * Traces the passed value with the pushed timestamp in state using the metric name and tags in state.
	 * Throws an exception if no timestamp is in state (set by {@link #pushTs(long)}.
	 * @param value The value to trace
	 * @param tagValues The optional tag values that will be matched up with the pushed keys in the tag key stack.
	 * @return this tracer
	 */
	public ITracer trace(final double value, final String... tagValues);
	
	/**
	 * Traces the delta of the passed value and the prior traced value with the pushed timestamp in state using the metric name and tags in state.
	 * Throws an exception if no timestamp is in state (set by {@link #pushTs(long)}.
	 * If there is no prior value, the delta state is captured, but no trace is emitted.
	 * @param value The value to trace
	 * @param tagValues The optional tag values that will be matched up with the pushed keys in the tag key stack.
	 * @return this tracer
	 */
	public ITracer dtrace(final long value, final String... tagValues);
	
	/**
	 * Traces the delta of the passed value and the prior traced value with the pushed timestamp in state using the metric name and tags in state.
	 * Throws an exception if no timestamp is in state (set by {@link #pushTs(long)}.
	 * If there is no prior value, the delta state is captured, but no trace is emitted.
	 * @param value The value to trace
	 * @param tagValues The optional tag values that will be matched up with the pushed keys in the tag key stack.
	 * @return this tracer
	 */
	public ITracer dtrace(final double value, final String... tagValues);


	// =====================================
	//  Meta Trace ops
	// =====================================
	
//	/**
//	 * Traces an annotation based on the passed annotation builder which is associated to the
//	 * most recently traced trace's metric name. As such, once the state of the tracer changes, this operation
//	 * will fail unti the next trace is traced. 
//	 * @param annotationBuilder The annotation builder to build the annotation from.
//	 * @return this tracer
//	 */
//	public ITracer trace(final AnnotationBuilder annotationBuilder);
//	
//	/**
//	 * Traces the passed annotation
//	 * @param annotation the annotation to trace
//	 * @return this tracer
//	 */
//	public ITracer trace(final IAnnotation annotation);
	
	
//	/**
//	 * Returns an annotation builder
//	 * @param description The annotation description
//	 * @param startTime The annotation start time
//	 * @return the builder
//	 */
//	public AnnotationBuilder annotationBuilder(final String description, final long startTime);
//	
//	/**
//	 * Returns an annotation builder starting the current time
//	 * @param description The annotation description
//	 * @return the builder
//	 */
//	public AnnotationBuilder annotationBuilder(final String description);
	
	/**
	 * Places a conditional on the next trace. If set to false, the next trace 
	 * will be suppressed. The flag will be set back true as soon as the trace is suppressed.
	 * @param active false to suppress the next trace, true otherwise
	 * @return this tracer
	 */
	public ITracer active(final boolean active);
	
	
	/**
	 * Traces a metric meta which describes the associated metric (or sub-metric, meaning the description
	 * applies to multiple metrics)
	 * @return this tracer
	 */
	public ITracer traceMeta();
/*
	{"metric":"exceptional.exceptions.count","name":"rate","value":"counter"}
	{"metric":"exceptional.exceptions.count","name":"unit","value":"errors"}
	{"metric":"exceptional.exceptions.count","name":"desc","value":"Exceptions per second stored in each Opserver data source."}
 */
	
	// =====================================
	//  House keeping & Misc
	// =====================================
	
	/**
	 * Writes the traces in the buffer to the store, flushes the trace buffer and resets the trace count.
	 * @return this tracer
	 */
	public ITracer flush();
	
	/**
	 * Dumps the full state of this tracer
	 * @return the full state of this tracer
	 */
	public String dump();

	/**
	 * Returns the maximum number of traces that will be buffered before an auto-flush occurs
	 * @return the max.... you know.
	 */
	public int getMaxTracesBeforeFlush();
	
	/**
	 * Sets the maximum number of traces that can be buffered before a flush
	 * @param maxTracesBeforeFlush the maximum number of traces that can be buffered before a flush
	 * @return this tracer
	 */
	public ITracer setMaxTracesBeforeFlush(final int maxTracesBeforeFlush);	

	/**
	 * Returns the current number of buffered events
	 * @return the current number of buffered events
	 */
	public int getBufferedEvents();
	
	/**
	 * Returns the total number of output events
	 * @return the total number of output events
	 */
	public long getTotalEvents();
	
	/**
	 * Retrieves the most recently set delta for the current in state metric name (plus the optional tag values)
	 * @param slot The slot to write the delta value into
	 * @param tagValues The optional tag values that will be matched up with the pushed keys in the tag key stack.
	 * @return this tracer
	 */
	public ITracer getLastDoubleDelta(final Number[] slot, final String... tagValues);
	
	/**
	 * Retrieves the most recently set delta for the current in state metric name (plus the optional tag values)
	 * @param slot The slot to write the delta value into
	 * @param tagValues The optional tag values that will be matched up with the pushed keys in the tag key stack.
	 * @return this tracer
	 */
	public ITracer getLastLongDelta(final Number[] slot, final String... tagValues);
	
	/**
	 * Retrieves the most recently set delta for the current in state metric name (plus the optional tag values)
	 * @param slot The slot to write the delta value into
	 * @param tagValues The optional tag values that will be matched up with the pushed keys in the tag key stack.
	 * @return this tracer
	 */
	public ITracer getLastIntegerDelta(final Number[] slot, final String... tagValues);
	
	/**
	 * Returns the number of events sent since the last time we inquired
	 * @return the number of events sent since the last time we inquired
	 */
	public long getSentEventCount();

}
