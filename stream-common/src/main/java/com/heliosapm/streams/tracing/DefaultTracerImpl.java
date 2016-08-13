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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.SortedMap;
import java.util.Stack;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.ObjectName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Meter;
import com.google.common.base.Predicate;
import com.heliosapm.streams.buffers.BufferManager;
import com.heliosapm.streams.common.metrics.SharedMetricsRegistry;
import com.heliosapm.streams.common.naming.AgentName;
import com.heliosapm.streams.metrics.StreamedMetricValue;
import com.heliosapm.streams.tracing.deltas.DeltaManager;
import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.lang.StringHelper;
import com.heliosapm.utils.time.SystemClock;
import com.heliosapm.utils.time.SystemClock.ElapsedTime;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

/**
 * <p>Title: DefaultTracerImpl</p>
 * <p>Description: The default tracer implementation</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tracing.DefaultTracerImpl</code></p>
 */

public class DefaultTracerImpl implements ITracer {
	/** Thread pool used to flush tracing buffer */
	private static final ExecutorService flushPool = Executors.newFixedThreadPool(2, new ThreadFactory(){
		final AtomicInteger serial = new AtomicInteger();
		@Override
		public Thread newThread(final Runnable r) {
			final Thread t = new Thread(r, "TracerFlushThread#" + serial.incrementAndGet());
			t.setDaemon(true);
			return t;
		}
	});
	/** Write event timer */
	private static final Meter traceMeter = SharedMetricsRegistry.getInstance().meter("tracer.tracemeter");

	// ===============================================================================
	// Current MetricName Components
	// ===============================================================================
	/** The metric name segments stack */
	private Stack<String> metricNameStack = new Stack<String>();
	/** The tag stack */
	private Stack<String[]> tagStack = new Stack<String[]>();	
//	/** The metric tags sorter */
//	private final TreeMap<String, String> tags = new TreeMap<String, String>(TagKeySorter.INSTANCE);
	/** The timestamp state in ms time format */
	private Long msTime = null;
	/** The max number of traces before an auto-flush */
	private int maxTracesBeforeFlush;
	/** The current number of un-flushed events in the buffer */
	private int bufferedEvents = 0;
	/** The inquiry event count */
	private long inqCount = 0;
	/** The total number of events generated */
	private long totalEvents = 0;
	/** The configured metric name segment delimiter */
	public final char metricSegDelim; 
	/** The configured max number of tags in a trace */
	public final int maxTags; 
	/** The configured min number of tags in a trace */
	public final int minTags; 
	/** The stateful tracing conditional which, if set to false, will suppress the next trace */
	private boolean traceActive = true;
	
	/** The suppression predicate */
	protected Predicate<PredicateTrace> suppressPredicate = null;
	
	/** The writer that delivers the buffered metrics to an end-point */
	protected volatile IMetricWriter writer;
	
	// ===============================================================================
	// Push/Pop for Tags and Tracer State
	// ===============================================================================	
	/** tracks the tag key stack */
	private final LinkedList<String> tagKeyStack = new LinkedList<String>();
	/** The checkpoint stack so we can push/pop this tracer's state */
	private final Stack<TracerState> checkpointStack = new Stack<TracerState>();
	/** Flag tracking if the current tracer state has been modified since the last trace */
	private boolean modified = false;
	
	/** The app/host tags */
	private final Map<String, String> appHostTags;

	
	
	public static final int COMPRESS_OFFSET = 0;
	public static final int COUNT_OFFSET = 1;
	public static final int START_DATA_OFFSET = 5;
	
	// ===============================================================================
	// Where all traces go when their time comnes
	// ===============================================================================
	/** The buffer factory used to create buffers for all tracers */
	private static final ByteBufAllocator bufferFactory = BufferManager.getInstance();
//	ConfigurationHelper.getIntSystemThenEnvProperty(CONF_INIT_SIZE, DEFAULT_INIT_SIZE),
//	ConfigurationHelper.getFloatSystemThenEnvProperty(CONF_EXT_PCT, DEFAULT_EXT_PCT)
	
	/** The buffer this tracer's traces are written out to before they're flushed */
	private final ByteBuf outBuffer;
	
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	
	void updateWriter(final IMetricWriter writer) {
		this.writer = writer;
	}
	
	/**
	 * <p>Title: TracerState</p>
	 * <p>Description: Holds the state of a Tracer between checkpoints</p> 
	 * <p>Company: Helios Development Group LLC</p>
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>com.heliosapm.streams.tracing.DefaultTracerImpl.TracerState</code></p>
	 */
	protected class TracerState {
		// ===============================================================================
		// Current MetricName Components
		// ===============================================================================
		/** The metric name as linked list of metric name segments */
		private LinkedList<String> cpMetricNameSegs = new LinkedList<String>();
		/** The timestamp in ms time format */
		private Long cpMsTime;
		/** tracks the tag key stack */
		private final LinkedList<String> cpTagKeyStack = new LinkedList<String>();
		/** tracks the tag stack */
		private final LinkedList<String[]> cpTagStack = new LinkedList<String[]>();
		/** The suppression predicate */
		private final Predicate<PredicateTrace> cpSuppressPredicate;
		
		
		/**
		 * Creates a new TracerState from the passed tracer
		 * @param t The tracer to read the state from
		 */
		private TracerState(final DefaultTracerImpl t) {
			cpMetricNameSegs.addAll(t.metricNameStack);
			cpTagStack.addAll(t.tagStack);
			cpTagKeyStack.addAll(t.tagKeyStack);
			cpMsTime = msTime;
			cpSuppressPredicate = t.suppressPredicate;
			
		}
		
		/**
		 * Pops this tracer state back into the passed tracer
		 * @param t The tracer to pop back to
		 */
		private void pop(final DefaultTracerImpl t) {
			t.metricNameStack.clear(); t.metricNameStack.addAll(cpMetricNameSegs);
			t.tagStack.clear(); t.tagStack.addAll(cpTagStack);
			t.tagKeyStack.clear(); t.tagKeyStack.addAll(cpTagKeyStack);
			msTime = cpMsTime;
			t.suppressPredicate = cpSuppressPredicate;
		}
	}
	
	/**
	 * Creates a new Tracer with an initial metric name and current timestamp
	 * @param writer The writer to send traced metrics with
	 */
	protected DefaultTracerImpl(final IMetricWriter writer) {
		this.writer = writer;
		metricSegDelim = ConfigurationHelper.getCharSystemThenEnvProperty(CONF_METRIC_SEG_DELIM, DEFAULT_METRIC_SEG_DELIM);
		maxTags = 8;  // FIXME: config
		minTags = 1;  // FIXME: config... allow zero for graphite et.al.
		maxTracesBeforeFlush = 200;  // FIXME: config a default
		outBuffer = bufferFactory.buffer(this.maxTracesBeforeFlush * 128); 
		outBuffer.setByte(COMPRESS_OFFSET, 0);
		outBuffer.setInt(COUNT_OFFSET, 0);
		outBuffer.writerIndex(START_DATA_OFFSET);
		appHostTags = Collections.unmodifiableMap(AgentName.getInstance().defaultTags());
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() throws IOException {
		outBuffer.release();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#clear()
	 */
	@Override
	public ITracer clear() {
		reset();
		checkpointStack.clear();		
		outBuffer.clear();
		return this;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#reset()
	 */
	@Override
	public ITracer reset() {
		metricNameStack.clear();
		tagStack.clear();
		tagKeyStack.clear();
//		tags.clear();		
		return this;
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "ITracer[" + writer.toString() + "]";
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#checkpoint()
	 */
	@Override
	public ITracer checkpoint() {
		checkpointStack.push(new TracerState(this));
		return this;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#popCheckpoint()
	 */
	@Override
	public ITracer popCheckpoint() {
		checkpointStack.pop().pop(this);
		return this;
	}
	
	/**
	 * Builds the metric name from the metric name stack
	 * @return the metric name
	 */
	protected String buildMetricName() {
		if(metricNameStack.isEmpty()) throw new RuntimeException("No metric name segments");
		final StringBuilder b = new StringBuilder();
		for(String s: metricNameStack) {
			b.append(s).append(metricSegDelim);
		}
		return b.deleteCharAt(b.length()-1).toString();
	}
	
	/**
	 * Builds a tag map based on the current tag stack
	 * @param tagValues The optional tag values which will be paired up with the tag key stack
	 * @return a tag map 
	 */
	protected SortedMap<String, String> buildTags(final String...tagValues) {
		final TreeMap<String, String> tmap = new TreeMap<String, String>(TagKeySorter.INSTANCE);
		if(tagValues!=null && tagValues.length > 0) {
			final int tks = tagKeyStack.size();
			if(tagValues.length > 0 && tks != tagValues.length) {
				log.error("TagValue & Tag Key Stack Mismatch. TagKeyStack: [{}], TagValues: [{}]", tagKeyStack, Arrays.toString(tagValues));
				throw new IllegalStateException("TagValue & Tag Key Stack Mismatch. TagKeyStack: " + tagKeyStack.size() + ", TagValues: " + tagValues.length) ;
			}
			 
			final String[] tagKeys = tagKeyStack.toArray(new String[tks]);			
			for(int i = 0; i < tagValues.length; i++) {		
				try {
					if(tagKeys[i].isEmpty() || tagValues[i].isEmpty()) continue;
					tmap.put(tagKeys[i], tagValues[i]);
				} catch (Exception ex) {
					log.error("Failed to build tag pair: [{}]:[{}]", tagKeys[i], tagValues[i]);
				}
			}
		}
		for(String[] pair: tagStack) {
			tmap.put(pair[0], pair[1]);
		}
		return tmap;
	}



	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#seg(java.lang.String)
	 */
	@Override
	public ITracer seg(final String fullSegment) {		
		metricNameStack.clear();
		if(fullSegment==null || fullSegment.trim().isEmpty()) return this;
		Collections.addAll(metricNameStack, StringHelper.splitString(fullSegment, metricSegDelim, true));
		return this;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#pushSeg(java.lang.String)
	 */
	@Override
	public ITracer pushSeg(final String segment) {
		if(segment != null && !segment.trim().isEmpty()) {
			metricNameStack.push(segment.trim());
		}
		return this;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#popSeg()
	 */
	@Override
	public ITracer popSeg() {
		metricNameStack.pop();
		return this;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#popSeg(int)
	 */
	@Override
	public ITracer popSeg(final int n) {
		if(n > 0) {
			for(int i = 0; i < n; i++) {
				metricNameStack.pop();
			}
		}
		return this;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#seg()
	 */
	@Override
	public String seg() {
		return buildMetricName();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#tags()
	 */
	@Override
	public Map<String, String> tags() {
		return Collections.unmodifiableSortedMap(buildTags());
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#segs()
	 */
	@Override
	public String[] segs() {
		return metricNameStack.toArray(new String[0]);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#tags(java.util.Map)
	 */
	@Override
	public ITracer tags(final Map<String, String> tags) {
		if(tags!=null && !tags.isEmpty()) {
			tagStack.clear();
			for(Map.Entry<String, String> entry: tags.entrySet()) {
				tagStack.push(new String[]{entry.getKey(), entry.getValue()});
			}
		}
		return this;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#tags(java.lang.String[])
	 */
	@Override
	public ITracer tags(final String... tags) {
		if(tags!=null) {
			if(tags.length==0) {
				tagStack.clear();
			} else {
				if(tags.length%2 != 0) throw new IllegalArgumentException("Odd number of tag pairs " + Arrays.toString(tags));
				for(int i = 0; i < tags.length;) {
					tagStack.push(new String[]{tags[i++].trim(), tags[i++].trim()});
				}				
			}			
		}
		return this;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#pushTagPair(java.lang.String)
	 */
	@Override
	public ITracer pushTagPair(final String tagPair) {
		if(tagPair==null || tagPair.trim().isEmpty()) throw new IllegalArgumentException("Tag pair was null or empty");
		return tags(StringHelper.splitString(tagPair, '=', true));
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#pushTag(java.lang.Object, java.lang.Object)
	 */
	@Override
	public ITracer pushTag(final Object tagKey, final Object tagValue) {
		tagStack.push(new String[]{ts(tagKey, "Tag Key"), ts(tagValue, "Tag Value")});		
		return this;
	}
	
	private static String ts(final Object obj, final String name) {
		if(obj==null) throw new IllegalArgumentException(name + " was null");
		final String s = obj.toString().trim().toLowerCase();
		if(s.isEmpty()) throw new IllegalArgumentException(name + " was empty");
		return s;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#pushTags(java.util.Map)
	 */
	@Override
	public ITracer pushTags(final Map<String, String> tags) {
		if(tags!=null && !tags.isEmpty()) {
			for(Map.Entry<String, String> entry: tags.entrySet()) {
				pushTag(entry.getKey(), entry.getValue());
			}
		}
		return this;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#popTag()
	 */
	@Override
	public ITracer popTag() {
		tagStack.pop();
		return this;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#popTags(int)
	 */
	@Override
	public ITracer popTags(final int n) {
		if(n<1) throw new IllegalArgumentException("Invalid number of tags to pop [" + n + "]");
		for(int i = 0; i < n; i++) {
			tagStack.pop();
		}
		return this;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#pushKeys(java.lang.String[])
	 */
	@Override
	public ITracer pushKeys(final String... keys) {
		if(keys != null) {
			for(String key: keys) {
				if(key==null) continue;
				final String k = key.trim();
				if(k.isEmpty()) continue;
				tagKeyStack.addLast(k);
			}
		}
		return this;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#objectName(javax.management.ObjectName)
	 */
	@Override
	public ITracer objectName(final ObjectName objectName) {
		if(objectName==null) throw new IllegalArgumentException("The passed ObjectName was null");
		metricNameStack.clear();
		tagStack.clear();
		Collections.addAll(metricNameStack, StringHelper.splitString(objectName.getDomain(), '.', true));
		putTagStack(objectName.getKeyPropertyList());
		return this;
	}
	
	/**
	 * Puts a tag stack
	 * @param tags the tags to put
	 */
	protected void putTagStack(final Map<String, String> tags) {
		for(Map.Entry<String, String> entry: tags.entrySet()) {
			final String key = entry.getKey();
			final String value = entry.getValue();
			if(!tagStack.isEmpty()) {
				checkReplace(key);
			}
			tagStack.push(new String[]{key, value});
		}
	}
	
	/**
	 * Checks the replacement of a key
	 * @param key the key to check
	 */
	protected void checkReplace(final String key) {
		for(final Iterator<String[]> iter = tagStack.iterator(); iter.hasNext();) {
			final String[] pair = iter.next();
			if(key.equals(pair[0])) {
				iter.remove();
				break;
			}
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#objectName(java.lang.String)
	 */
	@Override
	public ITracer objectName(String objectName) {
		if(objectName==null || objectName.trim().isEmpty()) throw new IllegalArgumentException("The passed ObjectName was null or empty");
		try {
			objectName(new ObjectName(objectName));
		} catch (Exception ex) {
			throw new IllegalArgumentException("The passed ObjectName [" + objectName + "] was invalid", ex);
		}
		return this;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#popKeys()
	 */
	@Override
	public ITracer popKeys() {
		tagKeyStack.clear();
		return this;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#popKeys(int)
	 */
	@Override
	public ITracer popKeys(final int n) {
		for(int i = 0; i < n; i++) {
			tagKeyStack.pop();
		}
		return this;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#popKey()
	 */
	@Override
	public ITracer popKey() {
		tagKeyStack.pop();
		return this;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#popTag(java.lang.String[])
	 */
	@Override
	public ITracer popTag(final String... tagKeys) {
		if(tagKeys!=null && tagKeys.length!=0) {
			for(String tagKey: tagKeys) {
				if(tagKey==null) continue;
				final String _tagKey = tagKey.trim();
				for(Iterator<String[]> iter = tagStack.iterator(); iter.hasNext();) {
					final String[] pair = iter.next();
					if(pair[0].equals(_tagKey)) {
						iter.remove();
						break;
					}
				}
			}
		}
		return this;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#pushTs(long)
	 */
	@Override
	public ITracer pushTs(final long timestamp) {
		msTime = timestamp;
		return this;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#popTs()
	 */
	@Override
	public ITracer popTs() {
		msTime = null;
		return this;
	}
	
	
//	private ITrace _trace(final long value, final long timestamp, final String...tagValues) {
//		return traceOut(TraceFactory.make(value, timestamp, metricId(tagValues)));	
//	}
//	
//	private ITrace _trace(final long value, final String...tagValues) {
//		return _trace(value, msTime==null ? System.currentTimeMillis() : msTime, tagValues);
//	}
//	
//	private ITrace _trace(final double value, final long timestamp, final String...tagValues) {
//		return traceOut(TraceFactory.make(value, timestamp, metricId(tagValues)));	
//	}
//	
//	private ITrace _trace(final double value, final String...tagValues) {
//		return _trace(value, msTime==null ? System.currentTimeMillis() : msTime, tagValues);
//	}
	
	private void traceOut(final long value, final long timestamp, final String...tagValues) {
		final int pos = outBuffer.writerIndex();
		try {			
			modified = false;
			incr();
			final SortedMap<String, String> outTags;
			if(tagValues!=null && tagValues.length > 0) {
				outTags = new TreeMap<String, String>(TagKeySorter.INSTANCE);
				outTags.putAll(buildTags(tagValues));
			} else {
				outTags = buildTags();
			}
			addAppHostTags(outTags);
			final String mn = buildMetricName();
			if(suppressPredicate!=null) {
				state.update(value, timestamp, mn, outTags);
				if(suppressPredicate.apply(state)) {
					decr();
					return;
				}
			}
			StreamedMetricValue.write(outBuffer, null, mn, timestamp, value, outTags);
		} catch (Exception ex) {
			outBuffer.writerIndex(pos);
			decr();
			throw new RuntimeException("Failed to trace", ex);
		} finally {
			if(bufferedEvents==maxTracesBeforeFlush) {
				flush();
			}
		}
	}

	private void traceOut(final double value, final long timestamp, final String...tagValues) {
		final int pos = outBuffer.writerIndex();
		try {			
			modified = false;
			incr();
			final SortedMap<String, String> outTags;
			if(tagValues!=null && tagValues.length > 0) {
				outTags = new TreeMap<String, String>(TagKeySorter.INSTANCE);
				outTags.putAll(buildTags(tagValues));
			} else {
				outTags = buildTags();
			}
			addAppHostTags(outTags);
			final String mn = buildMetricName();
			if(suppressPredicate!=null) {
				state.update(value, timestamp, mn, outTags);
				if(suppressPredicate.apply(state)) {
					decr();
					return;
				}
			}
			StreamedMetricValue.write(outBuffer, null, mn, timestamp, value, outTags);
		} catch (Exception ex) {
			outBuffer.writerIndex(pos);
			decr();
			throw new RuntimeException("Failed to trace", ex);
		} finally {
			if(bufferedEvents==maxTracesBeforeFlush) {
				flush();
			}
		}
	}
	
	/**
	 * Adds the app and host tags if not already present
	 * @param map The map to add to
	 */
	protected void addAppHostTags(final Map<String, String> map) {
		for(Map.Entry<String, String> entry: appHostTags.entrySet()) {
			map.putIfAbsent(entry.getKey(), entry.getValue());
		}
	}
	
	/**
	 * Increments traced event counts
	 */
	protected void incr() {
		bufferedEvents++;
		totalEvents++;
		inqCount++;
		traceMeter.mark();		
	}
	
	/**
	 * Decrements traced event counts
	 */
	protected void decr() {
		bufferedEvents--;
		totalEvents--;
		inqCount--;
		traceMeter.mark(-1L);		
	}
	
	
	private static final PredicateTrace state = new PredicateTrace();
	
	/**
	 * <p>Title: PredicateTrace</p>
	 * <p>Description: A wrapper around the traceable state of this tracer</p> 
	 * <p>Company: Helios Development Group LLC</p>
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>com.heliosapm.streams.tracing.DefaultTracerImpl.PredicateTrace</code></p>
	 */
	public static class PredicateTrace {
		private boolean dv = true;
		private double doubleValue = -1D;
		private long longValue = -1L;
		private long timestamp = -1L;
		private String metricName = null;
		private Map<String, String> tags = new TreeMap<String, String>(TagKeySorter.INSTANCE);
		
		void update(final double value, final long timestamp, final String metricName, final Map<String, String> tags) {
			dv = true;
			doubleValue = value;
			this.timestamp = timestamp;
			this.metricName = metricName;
			this.tags.clear();
			this.tags.putAll(tags);
		}
		
		void update(final long value, final long timestamp, final String metricName, final Map<String, String> tags) {
			dv = false;
			longValue = value;
			this.timestamp = timestamp;
			this.metricName = metricName;
			this.tags.clear();
			this.tags.putAll(tags);
		}
		
		
		/**
		 * Indicates if this is a double value or a long value
		 * @return the dv true if this is a double value, false if this is a long value
		 */
		public boolean isDv() {
			return dv;
		}
		
		/**
		 * Returns the value
		 * @return the value
		 */
		public Number getValue() {
			return dv ? doubleValue : longValue;
		}
		/**
		 * Returns the double value
		 * @return the doubleValue
		 */
		public double getDoubleValue() {
			if(!dv) throw new IllegalStateException("This trace is long type. Cannot call getDoubleValue()");
			return doubleValue;
		}
		/**
		 * Returns the long value
		 * @return the longValue
		 */
		public long getLongValue() {
			if(dv) throw new IllegalStateException("This trace is double type. Cannot call getLongValue()");
			return longValue;
		}
		/**
		 * Returns the timestamp in ms.
		 * @return the timestamp
		 */
		public long getTimestamp() {
			return timestamp;
		}
		/**
		 * Returns the metric name
		 * @return the metricName
		 */
		public String getMetricName() {
			return metricName;
		}
		/**
		 * Returns the tags
		 * @return the tags
		 */
		public Map<String, String> getTags() {
			return tags;
		}
		
		
		
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#trace(long, long, java.lang.String[])
	 */
	@Override
	public ITracer trace(final long value, final long timestamp, final String... tagValues) {
		if(!traceActive) { traceActive=true; return this; }
		traceOut(value, timestamp, tagValues);
		return this;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#trace(double, long, java.lang.String[])
	 */
	@Override
	public ITracer trace(final double value, final long timestamp, final String... tagValues) {
		if(!traceActive) { traceActive=true; return this; }
		traceOut(value, timestamp, tagValues);
		return this;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#dtrace(long, long, java.lang.String[])
	 */
	@Override
	public ITracer dtrace(final long value, final long timestamp, final String... tagValues) {
		if(!traceActive) { traceActive=true; return this; }
		final Long delta = DeltaManager.getInstance().delta(buildMetricName() + buildTags(tagValues).toString(), value);
		if(delta!=null) {
			trace(delta, timestamp, tagValues);
		}
		return this;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#dtrace(double, long, java.lang.String[])
	 */
	@Override
	public ITracer dtrace(final double value, final long timestamp, final String... tagValues) {
		if(!traceActive) { traceActive=true; return this; }
		final Double delta = DeltaManager.getInstance().delta(buildMetricName() + buildTags(tagValues).toString(), value);
		if(delta!=null) {
			trace(delta, timestamp, tagValues);
		}
		return this;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#trace(long, java.lang.String[])
	 */
	@Override
	public ITracer trace(final long value, final String... tagValues) {
		if(!traceActive) { traceActive=true; return this; }		
		return trace(value, msTime==null ? System.currentTimeMillis() : msTime, tagValues);		
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#trace(double, java.lang.String[])
	 */
	@Override
	public ITracer trace(final double value, final String... tagValues) {
		if(!traceActive) { traceActive=true; return this; }		
		return trace(value, msTime==null ? System.currentTimeMillis() : msTime, tagValues);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#dtrace(long, java.lang.String[])
	 */
	@Override
	public ITracer dtrace(final long value, final String... tagValues) {
		if(!traceActive) { traceActive=true; return this; }
		final Long delta = DeltaManager.getInstance().delta(buildMetricName() + buildTags(tagValues).toString(), value);
		if(delta!=null) {
			trace(delta, msTime==null ? System.currentTimeMillis() : msTime, tagValues);
		}
		return this;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#dtrace(double, java.lang.String[])
	 */
	@Override
	public ITracer dtrace(final double value, final String... tagValues) {
		if(!traceActive) { traceActive=true; return this; }
		final Double delta = DeltaManager.getInstance().delta(buildMetricName() + buildTags(tagValues).toString(), value);
		if(delta!=null) {
			trace(delta, msTime==null ? System.currentTimeMillis() : msTime, tagValues);
		}
		return this;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#getLastDoubleDelta(java.lang.Number[], java.lang.String[])
	 */
	@Override
	public ITracer getLastDoubleDelta(final Number[] slot, final String... tagValues) {
		final Double delta = DeltaManager.getInstance().doubleDeltav(buildMetricName() + buildTags(tagValues).toString());
		if(delta!=null) slot[0] = delta;
		return this;
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#getLastLongDelta(java.lang.Number[], java.lang.String[])
	 */
	@Override
	public ITracer getLastLongDelta(final Number[] slot, final String... tagValues) {
		final Long delta = DeltaManager.getInstance().longDeltav(buildMetricName() + buildTags(tagValues).toString());
		if(delta!=null) slot[0] = delta;
		return this;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#getLastIntegerDelta(java.lang.Number[], java.lang.String[])
	 */
	@Override
	public ITracer getLastIntegerDelta(final Number[] slot, final String... tagValues) {
		final Integer delta = DeltaManager.getInstance().intDeltav(buildMetricName() + buildTags(tagValues).toString());
		if(delta!=null) slot[0] = delta;
		return this;
	}
	
	




	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#flush()
	 */
	@Override
	public ITracer flush() {
		if(bufferedEvents > 0) {
			final ElapsedTime et = SystemClock.startClock();
			outBuffer.setInt(COUNT_OFFSET, bufferedEvents);
			final ByteBuf bufferCopy = bufferFactory.buffer(outBuffer.readableBytes());
//			bufferCopy.writeByte(0);
//			bufferCopy.writeInt(bufferedEvents);
			bufferCopy.writeBytes(outBuffer);
			outBuffer.resetReaderIndex();
			outBuffer.writerIndex(START_DATA_OFFSET);
			final int finalCount = bufferedEvents;
			flushPool.execute(new Runnable(){
				@Override
				public void run() {
					writer.onMetrics(bufferCopy);
					log.info(et.printAvg("Metrics flushed", finalCount));
				}
			});
			bufferedEvents = 0;
		}
		return this;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#getMaxTracesBeforeFlush()
	 */
	@Override
	public int getMaxTracesBeforeFlush() {
		return maxTracesBeforeFlush;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#setMaxTracesBeforeFlush(int)
	 */
	@Override
	public ITracer setMaxTracesBeforeFlush(final int maxTracesBeforeFlush) {
		if(maxTracesBeforeFlush < 1) throw new IllegalArgumentException("Invalid max traces [" + maxTracesBeforeFlush + "]. Must be > 0");
		this.maxTracesBeforeFlush = maxTracesBeforeFlush;
		if(bufferedEvents >= maxTracesBeforeFlush) {
			flush();
		}		
		return this;
	}
	
//	/**
//	 * {@inheritDoc}
//	 * @see com.heliosapm.streams.tracing.ITracer#trace(com.heliosapm.tsdbex.tracing.model.IAnnotation)
//	 */
//	@Override
//	public ITracer trace(final IAnnotation annotation) {
//		if(!traceActive) { traceActive=true; return this; }
//		if(annotation==null) throw new IllegalArgumentException("The passed annotation was null");
//		final int pos = outBuffer.writerIndex();
//		try {
//			traceOut.writeByte(StoreRecordType.ANNOTATION.byteOrdinal);
//			traceOut.writeInt(annotation.length());
//			log.info("Annotation Length:{} bytes", annotation.length());
//			AnnotationFactory.write(traceOut, annotation);
//			traceOut.flush();
//			bufferedEvents++;
//			totalEvents++;
//			modified = false;
//			return this;
//		} catch (Exception ex) {
//			outBuffer.writerIndex(pos);
//			throw new RuntimeException("Failed to trace annotation", ex);
//		} finally {
//			if(bufferedEvents==maxTracesBeforeFlush) {
//				flush();
//			}
//		}		
//	}
	
//	/**
//	 * {@inheritDoc}
//	 * @see com.heliosapm.streams.tracing.ITracer#trace(com.heliosapm.tsdbex.tracing.model.AnnotationFactory.AnnotationBuilder)
//	 */
//	@Override
//	public ITracer trace(final AnnotationBuilder annotationBuilder) {
//		if(!traceActive) { traceActive=true; return this; }
//		if(annotationBuilder!=null) {
//			trace(annotationBuilder.build());
//		}
//		return this;
//	}
//	
//	/**
//	 * {@inheritDoc}
//	 * @see com.heliosapm.streams.tracing.ITracer#trace(java.lang.String, long, long, long, java.lang.String, java.util.Map)
//	 */
//	@Override
//	public ITracer trace(final String description, final long startTime, final long endTime, final long metricNameId, final String notes, final Map<String, String> custom) {
//		if(!traceActive) { traceActive=true; return this; }
//		trace(
//			AnnotationFactory.builder(startTime, description)
//				.endTime(endTime)
//				.metricName(metricNameId)
//				.notes(notes)
//				.custom(custom)
//				.build()
//		);
//		return this;
//	}
	
//	/**
//	 * {@inheritDoc}
//	 * @see com.heliosapm.streams.tracing.ITracer#annotationBuilder(java.lang.String, long)
//	 */
//	@Override
//	public AnnotationBuilder annotationBuilder(final String description, final long startTime) {
//		return AnnotationFactory.builder(startTime, description, this);
//	}
//	
//	/**
//	 * {@inheritDoc}
//	 * @see com.heliosapm.streams.tracing.ITracer#annotationBuilder(java.lang.String)
//	 */
//	@Override
//	public AnnotationBuilder annotationBuilder(final String description) {
//		return AnnotationFactory.builder(System.currentTimeMillis(), description, this);
//	}
	

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#traceMeta()
	 */
	@Override
	public ITracer traceMeta() {
		if(!traceActive) { traceActive=true; return this; }
		// TODO Auto-generated method stub
		return null;
	}
	

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#dump()
	 */
	@Override
	public String dump() {
		final StringBuilder b = new StringBuilder("Tracer State: [");
		b.append("\n\tMetricName:").append(metricNameStack)
		.append("\n\tTagStack:").append(tagStackToString())
		.append("\n\tTagKeys:").append(tagKeyStack)
		.append("\n\tTimestamp:").append(msTime)
		.append("\n\tSupressPredicate:").append(suppressPredicate)
		.append("\n\tFlushCount:").append(maxTracesBeforeFlush)
		.append("\n\tBufferedEvents:").append(bufferedEvents)
		.append("\n\tBuffer:").append(outBuffer)
		.append("\n\tTotalTraces:").append(totalEvents);
		return b.append("\n]").toString();
	}
	
	/**
	 * Renders the tag stack
	 * @return the tag stack rendered as a flat string
	 */
	protected String tagStackToString() {
		if(tagStack.isEmpty()) return "";
		final StringBuilder b = new StringBuilder("{");
		for(String[] pair: tagStack) {
			if(b.length() > 1) {
				b.append(", ");
			}
			b.append(pair[0]).append("=").append(pair[1]);
		}
		return b.append("}").toString();
	}

	

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#getBufferedEvents()
	 */
	@Override
	public int getBufferedEvents() {
		return bufferedEvents;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#getTotalEvents()
	 */
	@Override
	public long getTotalEvents() {
		return totalEvents;
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#active(boolean)
	 */
	@Override
	public ITracer active(boolean active) {
		traceActive = active;
		return this;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#getSentEventCount()
	 */
	@Override
	public long getSentEventCount() {
		final long x = inqCount;
		inqCount = 0;
		return x;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#setSuppressPredicate(com.google.common.base.Predicate)
	 */
	@Override
	public ITracer setSuppressPredicate(final Predicate<PredicateTrace> predicate) {
		this.suppressPredicate = predicate;
		return this;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.ITracer#clearSuppressPredicate()
	 */
	@Override
	public ITracer clearSuppressPredicate() {
		this.suppressPredicate = null;
		return this;
	}

}
