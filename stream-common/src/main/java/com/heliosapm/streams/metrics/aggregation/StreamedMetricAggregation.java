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
package com.heliosapm.streams.metrics.aggregation;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.state.KeyValueStore;

import com.heliosapm.utils.buffer.BufferManager;
import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.StreamedMetricValue;
import com.heliosapm.utils.enums.TimeUnitSymbol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

/**
 * <p>Title: StreamedMetricAggregation</p>
 * <p>Description: A period aggregation of a streamed metric</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.StreamedMetricAggregation</code></p>
 */

public class StreamedMetricAggregation {
	/** Indicates if the metric is sticky */
	protected final boolean sticky;
	/** The value type (true for double, false for long) */
	protected final boolean doubleType;
	/** The creation timestamp */
	protected final long createTime;
	/** The aggregation period */
	protected final long period;
	/** The aggregation period unit */
	protected final TimeUnit periodUnit;
	/** The min, max, avg, count, last update, period start and period end */
	protected final ByteBuffer values = ByteBuffer.allocateDirect(VALUE_SIZE);
	/** The metric name */
	protected final String metricName;
	/** The metric tags */
	protected final Map<String, String> tags = new TreeMap<String, String>();
	/** The size of a read instance */
	protected volatile transient int size = -1;
	
	private static final TimeUnit[] TUNITS = TimeUnit.values();
	
	/** The length of the metric values storage */
	public static final byte VALUE_SIZE = 8 * 8; 

	/** The offset for the stickiness */
	public static final byte STICKY = 0;	
	/** The offset for the double type */
	public static final byte DOUBLE_TYPE = STICKY + 1;
	/** The offset for the create time */
	public static final byte CREATE_TIME = DOUBLE_TYPE + 1;
	/** The offset for the period */
	public static final byte PERIOD = CREATE_TIME + 8;
	/** The offset for the period unit */
	public static final byte PERIOD_UNIT = PERIOD + 8;
	/** The offset for the metric values */
	public static final byte METRIC_VALUES = PERIOD_UNIT + 8;
	/** The offset for the beginning of the tag count */
	public static final byte TAG_COUNT = METRIC_VALUES + VALUE_SIZE;	
	/** The offset for the beginning of the metric name */
	public static final byte METRIC_NAME = TAG_COUNT + 1;
	
	
	/** The relative offset of the prior value */
	public static final byte PRIOR = 0;
	/** The relative offset of the min value */
	public static final byte MIN = PRIOR + 8;
	/** The relative offset of the min value */
	public static final byte MAX = MIN + 8;
	/** The relative offset of the avg value */
	public static final byte AVG = MAX + 8;
	/** The relative offset of the sample count */
	public static final byte COUNT = AVG + 8;	
	/** The relative offset of the last update timestamp */
	public static final byte LAST_TS = COUNT + 8;
	/** The relative offset of the period start timestamp */
	public static final byte P_START = LAST_TS + 8;
	/** The relative offset of the period end timestamp */
	public static final byte P_END = P_START + 8;
	
	/** The UTF8 character set */
	public static final Charset UTF8 = Charset.forName("UTF8");
	
	
	/**
	 * Creates a new StreamedMetricAggregation from the passed bytes
	 * @param bytes The bytes to read from
	 */
	private StreamedMetricAggregation(final byte[] bytes) {
		size = bytes.length;
		final ByteBuf b = BufferManager.getInstance().buffer(size).writeBytes(bytes);
		try {
			sticky = b.getByte(STICKY)==1;				
			doubleType = b.getByte(DOUBLE_TYPE)==1;
			createTime = b.getLong(CREATE_TIME);
			period = b.getLong(PERIOD);
			periodUnit = TUNITS[b.getByte(PERIOD_UNIT)];
			b.readerIndex(METRIC_VALUES);
			values.put(bytes, METRIC_VALUES, VALUE_SIZE);
			final byte tagCount = b.getByte(TAG_COUNT);
			b.readerIndex(METRIC_NAME);
			metricName = nextString(b);
			for(int i = 0; i < tagCount; i++) {
				tags.put(nextString(b), nextString(b));
				i++;
			}
		} finally {
			try { b.release(); } catch (Exception x) {/* No Op */}
		}
	}
	
	/**
	 * Returns this aggregation as a byte array
	 * @return a byte array
	 */
	public byte[] toByteArray() {
		final ByteBuf b = BufferManager.getInstance().buffer(size==-1 ? 128 : size);
		try {
			b.writeByte(sticky ? 1 : 0);
			b.writeByte(doubleType ? 1 : 0);
			b.writeLong(createTime);
			b.writeLong(period);
			b.writeByte(periodUnit.ordinal());
			values.position(0);
			b.writeBytes(values);
			b.writeByte(tags.size());
			BufferManager.writeUTF(metricName, b);
			for(Map.Entry<String, String> entry: tags.entrySet()) {
				BufferManager.writeUTF(entry.getKey(), b);
				BufferManager.writeUTF(entry.getValue(), b);
			}
			return ByteBufUtil.getBytes(b);
		} finally {
			try { b.release(); } catch (Exception x) {/* No Op */}
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		final StringBuilder b = new StringBuilder(getClass().getSimpleName()).append(" [");
		b.append("\n\tMetricName: ").append(metricName);
		b.append("\n\tTags: ").append(tags);
		b.append("\n\tCreated: ").append(getCreateDate());
		b.append("\n\tData Type: ").append(doubleType ? "double" : "long");
		b.append("\n\tSticky: ").append(sticky);
		b.append("\n\tPeriod: ").append(period);
		b.append("\n\tPeriodUnit: ").append(periodUnit);
		b.append("\n\tLastValue: ").append(new Date(getLastSampleTime()));
		b.append("\n\tPeriodWindow: ").append(new Date(getPeriodStartTime())).append(" --> ").append(new Date(getPeriodEndTime()));
		b.append("\n\tValues:");
		b.append("\n\t\tMin: ").append(getMin());
		b.append("\n\t\tMax: ").append(getMax());
		b.append("\n\t\tAvg: ").append(getAvg());
		b.append("\n\t\tCount: ").append(getCount());
		return b.append("\n]").toString();
	}
	
	private static String nextString(final ByteBuf b) {
		return BufferManager.readUTF(b);		
	}
	
	/**
	 * Creates a new StreamedMetricAggregation
	 * @param The metric name
	 * @param sticky true if this metric is sticky across periods, false if metric is fully reset
	 * @param doubleType true if value type is double, false if it is long
	 * @param period The aggregation period
	 * @param unit The aggregation period unit
	 */
	StreamedMetricAggregation(final String metricName, final boolean sticky, final boolean doubleType, final long period, final TimeUnit unit) {
		this.createTime = System.currentTimeMillis();
		this.metricName = metricName;
		this.sticky = sticky;
		this.doubleType = doubleType;
		this.period = period;
		this.periodUnit = unit;
		if(doubleType) {
			values.putDouble(MIN, Double.MAX_VALUE); // Double Min
			values.putDouble(MAX, Double.MIN_VALUE); // Double Max
			values.putDouble(PRIOR, 0D); 			// Prior Max
		} else {
			values.putLong(MIN, Long.MAX_VALUE); 	// Long Min
			values.putLong(MAX, Long.MIN_VALUE); 	// Long Max	
			values.putLong(PRIOR, 0L); 				// Prior Max
		}
		values.putDouble(AVG, 0D); 					// Avg
		values.putLong(COUNT, 0L); 					// Count
		values.putLong(LAST_TS, 0L);				// Last Sample
		setPeriods();
	}
	
	/**
	 * Adds a tag pair and returns this aggregation
	 * @param key The tag key
	 * @param value The tag value
	 * @return this aggregation
	 */
	public StreamedMetricAggregation addTag(final String key, final String value) {
		if(key==null || key.trim().isEmpty()) throw new IllegalArgumentException("The passed tag key was null or empty");
		if(value==null || value.trim().isEmpty()) throw new IllegalArgumentException("The passed tag value was null or empty");
		tags.put(key.trim(), value.trim());
		return this;
	}
	
	/**
	 * Adds a map of tags and returns this aggregation
	 * @param tags The tag map to add
	 * @return this aggregation
	 */
	public StreamedMetricAggregation addTags(final Map<String, String> tags) {
		if(tags==null) throw new IllegalArgumentException("The passed tag map was null");
		for(Map.Entry<String, String> entry : tags.entrySet()) {
			addTag(entry.getKey(), entry.getValue());
		}
		return this;
	}
	
	

	/**
	 * Retrieves a StreamedMetricAggregation from the passed store
	 * @param streamedMetric The streamed metric to get the aggregator for
	 * @param sticky true if this metric is sticky across periods, false if metric is fully reset
	 * @param period The aggregation period
	 * @param unit The aggregation period unit
	 * @param store The aggregation store
	 * @return the StreamedMetricAggregation
	 */
	public static StreamedMetricAggregation get(final StreamedMetric streamedMetric, final boolean sticky, final long period, final TimeUnit unit, final KeyValueStore<String, StreamedMetricAggregation> store) { 
		if(streamedMetric==null) throw new IllegalArgumentException("The passed streamed metric was null");
		if(store==null) throw new IllegalArgumentException("The passed state store was null");
		final String key = streamedMetric.metricKey() + "-" + period + TimeUnitSymbol.symbol(unit);
		StreamedMetricAggregation sma = store.get(key);
		if(sma==null) {
			synchronized(store) {
				sma = store.get(key);
				if(sma==null) {
					sma = new StreamedMetricAggregation(streamedMetric.getMetricName(), sticky, streamedMetric.isValued() ? streamedMetric.forValue(-1L).isDoubleValue() : false, period, unit);
					sma.tags.putAll(streamedMetric.getTags());
					sma.apply(streamedMetric, sticky, store);
//					store.put(key, sma);					
				}
			}
		}
		return sma;
	}
	
	void reset(final long metricTimestamp) {
		if(!sticky) {
			if(doubleType) {
				values.putDouble(MAX, Double.MAX_VALUE); // Double Min
				values.putDouble(MAX, Double.MIN_VALUE); // Double Max
			} else {
				values.putLong(MIN, Long.MAX_VALUE); 	// Long Min
				values.putLong(MIN, Long.MIN_VALUE); 	// Long Max			
			}
			values.putDouble(AVG, 0D); 					// Avg
		}
		values.putLong(COUNT, 0L); 						// Count
		values.putLong(LAST_TS, metricTimestamp);		// Last Sample
		setPeriods(metricTimestamp);
	}
	

	public void apply(final StreamedMetric streamedMetric, final boolean sticky, final KeyValueStore<String, StreamedMetricAggregation> store) {
//		final StreamedMetricAggregation sma = get(streamedMetric, sticky, period, periodUnit, store);
		final long[] currentPeriods = getPeriods();
		final long metricTimestamp = streamedMetric.getTimestamp();
		if(metricTimestamp < currentPeriods[0]) {
			// drop. It's stale
			return;
		}
		if(metricTimestamp > currentPeriods[1]) {
			reset(metricTimestamp);
		}
		final StreamedMetricValue smv = streamedMetric.forValue(1L);
		final long preCount = values.getLong(COUNT);
		if(doubleType) {			
			final double newValue = smv.getValueAsDouble();
			if(preCount==0L && !sticky) {
				values.putDouble(MIN, newValue);
				values.putDouble(MAX, newValue);
				values.putDouble(AVG, newValue);				
			} else {
				updateAverage(smv.getDoubleValue(), preCount, values.getDouble(PRIOR));
				if(newValue < getDoubleMin()) values.putDouble(MIN, newValue);
				if(newValue > getDoubleMax()) values.putDouble(MAX, newValue);
			}
			values.putDouble(PRIOR, newValue);
		} else {
			final long newValue = smv.getValueAsLong();
			if(preCount==0L && !sticky) {
				values.putLong(MIN, newValue);
				values.putLong(MAX, newValue);
				values.putLong(AVG, newValue);								
			} else {
				updateAverage(smv.getLongValue(), preCount, values.getLong(PRIOR));
				if(newValue < getLongMin()) values.putLong(MIN, newValue);
				if(newValue > getLongMax()) values.putLong(MAX, newValue);
			}
			values.putLong(PRIOR, newValue);
		}
		values.putLong(COUNT, preCount + 1);
		values.putLong(LAST_TS, metricTimestamp);
		
		
	}
	
	protected long[] getPeriods() {
		return new long[]{
			values.getLong(P_START),
			values.getLong(P_END)
		};
	}
	
	protected long[] periods() {
		final long now = System.currentTimeMillis();
		final long periodInMillis = periodUnit.toMillis(period);
		final long start = now - (now%periodInMillis);
		return new long[]{start, start + periodInMillis};
	}
	
	
	/**
	 * Updates and returns the EWMA average
	 * @param value The new incoming value
	 * @param sampleCount The pre-increment sample count
	 * @param prior The prior value (ignored if sample count is 0)
	 * @return the updated EWMA average
	 */
	double updateAverage(final long value, final long sampleCount, final long prior) {
		if(doubleType) throw new IllegalStateException("This metric is a double type. Cannot call updateAverage(long, long, long)");
		final double avg;
		if(sampleCount==0) {
			avg = value;
		} else {
			avg = prior + alpha(sampleCount) * (value - prior);
		}
		values.putDouble(AVG, avg);
		return avg;
	}
	
	/**
	 * Updates and returns the EWMA average
	 * @param value The new incoming value
	 * @param sampleCount The pre-increment sample count
	 * @param prior The prior value (ignored if sample count is 0)
	 * @return the updated EWMA average
	 */
	double updateAverage(final double value, final long sampleCount, final double prior) {
		if(!doubleType) throw new IllegalStateException("This metric is a long type. Cannot call updateAverage(double, long, double)");
		final double avg;
		if(sampleCount==0) {
			avg = value;
		} else {
			//avg = prior + alpha(sampleCount) * (value - prior);
			avg = prior + 0.5D * (value - prior);
		}
		values.putDouble(AVG, avg);
		return avg;
	}
	
	
	private static double alpha(final double sampleCount) {
//		alpha = 2/(N+1)
		return 2D / (sampleCount + 1);
	}
	
	long[] setPeriods() {
		final long[] ps = periods();
		setPeriods(ps[0], ps[1]);
		return ps;
	}
	
	long[] setPeriods(final long timestamp) {
		final long periodInMillis = periodUnit.toMillis(period);
		final long start = timestamp - (timestamp%periodInMillis);
		final long[] ps = new long[]{start, start + periodInMillis};
		setPeriods(ps[0], ps[1]);
		return ps;
	}
	
	
	void setPeriods(final long start, final long end) {
		values.putLong(P_START, start);
		values.putLong(P_END, end);
	}
	
	/**
	 * Returns the prior raw long value
	 * @return the prior raw long value
	 */
	public long getLongPrior() {
		if(doubleType) throw new IllegalStateException("This metric is a double type. Cannot call getLongPrior()");
		return getCount()==0 ? 0L : values.getLong(PRIOR);
	}
	
	/**
	 * Returns the prior raw double value
	 * @return the prior raw double value
	 */
	public double getDoublePrior() {
		if(!doubleType) throw new IllegalStateException("This metric is a long type. Cannot call getDoublePrior()");
		return getCount()==0 ? 0D : values.getDouble(PRIOR);
	}
	

	/**
	 * Indicates if this metric is sticky across periods 
	 * @return true if this metric is sticky across periods, false if metric is fully reset
	 */
	public boolean isSticky() {
		return sticky;
	}

	/**
	 * Indicates if this metric is a double typed value
	 * @return true if this metric is a double typed value, false if it is long typed
	 */
	public boolean isDoubleType() {
		return doubleType;
	}
	
	/**
	 * Returns the minimum value observed this period
	 * @return the minimum value observed this period
	 */
	public Number getMin() {
		return doubleType ? getDoubleMin() : getLongMin();
	}
	
	/**
	 * Returns the maximum value observed this period
	 * @return the maximum value observed this period
	 */
	public Number getMax() {
		return doubleType ? getDoubleMax() : getLongMax();
	}
	
	/**
	 * Returns the prior value observed this period
	 * @return the prior value observed this period
	 */
	public Number getPrior() {
		return doubleType ? getDoublePrior() : getLongPrior();
	}

	
	
	/**
	 * Returns the minimum double value
	 * @return the minimum double value
	 */
	public double getDoubleMin() {
		if(!doubleType) throw new IllegalStateException("This metric is a long type. Cannot call getDoubleMin()");
		final double d = values.getDouble(MIN);
		return d==Double.MAX_VALUE ? 0D : d;
	}
	
	/**
	 * Returns the maximum double value
	 * @return the maximum double value
	 */
	public double getDoubleMax() {
		if(!doubleType) throw new IllegalStateException("This metric is a long type. Cannot call getDoubleMax()");
		final double d = values.getDouble(MAX);
		return d==Double.MIN_VALUE ? 0D : d;
	}
	
	/**
	 * Returns the minimum long value
	 * @return the minimum long value
	 */
	public long getLongMin() {
		if(doubleType) throw new IllegalStateException("This metric is a double type. Cannot call getLongMin()");
		final long d = values.getLong(MIN);
		return d==Long.MAX_VALUE ? 0L : d;
	}
	
	/**
	 * Returns the maximum long value
	 * @return the maximum long value
	 */
	public long getLongMax() {
		if(doubleType) throw new IllegalStateException("This metric is a double type. Cannot call getLongMax()");
		final long d = values.getLong(MAX);
		return d==Long.MIN_VALUE ? 0L : d;
	}
	
	/**
	 * Returns the average value
	 * @return the average value
	 */
	public double getAvg() {
		return values.getDouble(AVG);
	}
	
	/**
	 * Returns the sample count
	 * @return the sample count
	 */
	public long getCount() {
		return values.getLong(COUNT);
	}
	
	/**
	 * Returns the last sample time
	 * @return the last sample time
	 */
	public long getLastSampleTime() {
		return values.getLong(LAST_TS);
	}
	
	/**
	 * Returns the period start time
	 * @return the period start time
	 */
	public long getPeriodStartTime() {
		return values.getLong(P_START);
	}
	
	/**
	 * Returns the period end time
	 * @return the period end time
	 */
	public long getPeriodEndTime() {
		return values.getLong(P_END);
	}

	/**
	 * Returns the aggregation creation time
	 * @return the aggregation creation time
	 */
	public long getCreateTime() {
		return createTime;
	}
	
	/**
	 * Returns the aggregation creation date
	 * @return the aggregation creation date
	 */
	public Date getCreateDate() {
		return new Date(createTime);
	}
	

	/**
	 * Returns the period
	 * @return the period
	 */
	public long getPeriod() {
		return period;
	}

	/**
	 * Returns the period unit
	 * @return the periodUnit
	 */
	public TimeUnit getPeriodUnit() {
		return periodUnit;
	}

	/**
	 * Returns the metric name
	 * @return the metricName
	 */
	public String getMetricName() {
		return metricName;
	}

	/**
	 * Returns an unmodifiable map of the tags
	 * @return the tags
	 */
	public Map<String, String> getTags() {
		return Collections.unmodifiableMap(tags);
	}
	
	public static StreamedMetricAggregation fromBytes(final byte[] bytes) {
		return new StreamedMetricAggregation(bytes);
	}
	
}