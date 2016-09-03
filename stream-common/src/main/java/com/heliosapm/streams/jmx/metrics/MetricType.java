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
package com.heliosapm.streams.jmx.metrics;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import javax.management.Descriptor;
import javax.management.ImmutableDescriptor;
import javax.management.MBeanAttributeInfo;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Counting;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Sampling;
import com.codahale.metrics.Timer;
import com.heliosapm.utils.collections.FluentMap;
import com.heliosapm.utils.tuples.NVP;


/**
 * <p>Title: MetricType</p>
 * <p>Description: Enumerates the metric type that can be aggregated into a {@link DropWizardMetrics} instance</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.jmx.metrics.MetricType</code></p>
 */

public enum MetricType implements MetricAttributeHandler {
	/** Dropwizard Counter */
	COUNTER(Counter.class, new CounterMBeanInfoGenerator(), CounterMember.COUNT),
	/** Dropwizard Timer */
	TIMER(Timer.class, new TimerMBeanInfoGenerator(), TimerMember.TIMERCOUNT),
	/** Dropwizard Histogram */
	HISTOGRAM(Histogram.class, new HistogramMBeanInfoGenerator(), HistogramMember.HISTCOUNT),
	/** Dropwizard Meter */
	METER(Meter.class, new TimerMBeanInfoGenerator(), MeterMember.RATECOUNT),
	/** Dropwizard Gauge */
	GAUGE(Gauge.class, new GaugeMBeanInfoGenerator(), GaugeMember.GAUGE);
	
	private MetricType(final Class<? extends Metric> baseType, final MetricAttributeHandler infoGenerator, final AttributeAdapter aa) {
		this.baseType = baseType;
		this.infoGenerator = infoGenerator;
		this.aa = aa;
	}
	
	/** The base metric type */
	public final Class<? extends Metric> baseType;
	/** The MBeanInfo generator for the metric's attributes */
	public final MetricAttributeHandler infoGenerator;
	/** One instance of an attribute adapter */
	public final AttributeAdapter aa;
	
	/**
	 * Determines which MetricType the passed metric is
	 * @param clazz The class to test
	 * @return the matching metric type
	 */
	public static MetricType metricType(final Class<?> clazz) {
		if(clazz==null) throw new IllegalArgumentException("The passed class was null");
		for(MetricType mt: values()) {
			if(mt.baseType.isAssignableFrom(clazz)) return mt;
		}
		throw new IllegalArgumentException("The type [" + clazz.getName() + "] is not a supported metric type");
	}
	
	/**
	 * Determines which MetricType the passed object is
	 * @param obj The object to test
	 * @return the matching metric type
	 */
	public static MetricType metricType(final Object obj) {
		if(obj==null) throw new IllegalArgumentException("The passed object was null");
		return metricType(obj.getClass());
	}
	
	/**
	 * Creates a map for metric instances keyed by the MetricType
	 * @return a map for metric instances keyed by the MetricType
	 */
	public static Map<MetricType, Metric> metricMap() {
		return new EnumMap<MetricType, Metric>(MetricType.class);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.jmx.metrics.MetricAttributeHandler#minfo(com.codahale.metrics.Metric)
	 */
	@Override
	public NVP<Metric, MBeanAttributeInfo[]> minfo(Metric metric) {		
		return infoGenerator.minfo(metric);
	}
	
	public static Descriptor descriptor(final Map<String, Object> values) {
		return new ImmutableDescriptor(values);
	}
	
	
	public static interface AttributeAdapter {
		public String attributeName(final String name);
		public String attributeDescription(final String name, final String custom);
		public Object invoke(final Metric metric);
		public String dataType(final Metric metric);
		public MBeanAttributeInfo attrInfo(final String name, final String custom, final Metric metric);
		public boolean isSampling();
		public AttributeAdapter[] members();
	}
	
	public static NVP<MBeanAttributeInfo[], AttributeAdapter[]> attrInfos(final AttributeAdapter aa, final String name, final String custom, final Metric metric) {
		final Map<String, MBeanAttributeInfo> attrs = new HashMap<String, MBeanAttributeInfo>();
		final Map<String, AttributeAdapter> adapters = new HashMap<String, AttributeAdapter>();
		for(AttributeAdapter a: aa.members()) {
			final MBeanAttributeInfo ma = a.attrInfo(name, custom, metric);
			attrs.put(ma.getName(), ma);
			adapters.put(ma.getName(), a);
		}
		if(aa.isSampling()) {
			for(SnapshotMember sm: SnapshotMember.values()) {
				final MBeanAttributeInfo ma = sm.attrInfo(name, custom, metric);
				attrs.put(ma.getName(), ma);
				adapters.put(ma.getName(), sm);
			}
		}
		
		return new NVP<MBeanAttributeInfo[], AttributeAdapter[]>(
				attrs.values().toArray(new MBeanAttributeInfo[attrs.size()]),
				adapters.values().toArray(new AttributeAdapter[attrs.size()])
		);
	}
	
	
	public static enum SnapshotMember implements AttributeAdapter {
		MIN("%sMin", "The lowest value for %s in the rolling average window", "long"),
		MAX("%sMax", "The highest value for %s in the rolling average window", "long"),
		MEDIAN("%sMedian", "The median value for %s in the rolling average window", "double"),
		MEAN("%sMean", "The arithmetic mean for values of %s in the rolling average window", "double"),
		PCT75("%sPct75", "The value for %s at the 75th percentile in the distribution", "double"),
		PCT95("%sPct95", "The value for %s at the 95th percentile in the distribution", "double"),
		PCT98("%sPct98", "The value for %s at the 98th percentile in the distribution", "double"),
		PCT99("%sPct99", "The value for %s at the 99th percentile in the distribution", "double"),
		PCT999("%sPct999", "The value for %s at the 99.9th percentile in the distribution", "double"),		
		STDDEV("%sStdDev", "The standard deviation for values of %s in the distribution", "double");
		
		private SnapshotMember(final String nameFormat, final String descriptionFormat, final String dataType) {
			this.nameFormat = nameFormat;
			this.descriptionFormat = descriptionFormat;
			this.dataType = dataType;
		}
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#members()
		 */
		@Override
		public AttributeAdapter[] members() {			
			return values();
		}
		
		/** The format for how the attribute name is created for this metric member */
		public final String nameFormat;
		/** The format for how the attribute description is created for this metric member */
		public final String descriptionFormat;
		/** The data type */
		public final String dataType;
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#attributeName(java.lang.String)
		 */
		@Override
		public String attributeName(final String name) {
			return String.format(nameFormat, name);
		}
		
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#attributeDescription(java.lang.String, java.lang.String)
		 */
		@Override
		public String attributeDescription(final String name, final String custom) {
			final String desc = String.format(descriptionFormat, name);
			if(custom!=null && !custom.trim().isEmpty()) {
				return desc + ". " + custom.trim();
			}
			return desc;
		}
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#invoke(com.codahale.metrics.Metric)
		 */
		@Override
		public Object invoke(final Metric metric) {
			final Sampling s = (Sampling)metric;
			switch(this) {
			case MAX:
				return s.getSnapshot().getMax();
			case MEAN:
				return s.getSnapshot().getMean();
			case MEDIAN:
				return s.getSnapshot().getMedian();
			case MIN:
				return s.getSnapshot().getMin();
			case PCT75:
				return s.getSnapshot().get75thPercentile();
			case PCT95:
				return s.getSnapshot().get95thPercentile();
			case PCT98:
				return s.getSnapshot().get98thPercentile();
			case PCT99:
				return s.getSnapshot().get99thPercentile();
			case PCT999:
				return s.getSnapshot().get999thPercentile();
			case STDDEV:
				return s.getSnapshot().getStdDev();			
			}
			return null;
		}
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#dataType(com.codahale.metrics.Metric)
		 */
		@Override
		public String dataType(final Metric metric) {
			return dataType;
		}

		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#attrInfo(java.lang.String, java.lang.String, com.codahale.metrics.Metric)
		 */
		@Override
		public MBeanAttributeInfo attrInfo(final String name, final String custom, final Metric metric) {
			return new MBeanAttributeInfo(attributeName(name), dataType(metric), attributeDescription(name, custom), true, false, false, descriptor(FluentMap.newMap(String.class, Object.class)
					.fput("metricClass", metric.getClass().getName())
					.fput("metricType", "gauge")
			)); 
		}
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#isSampling()
		 */
		@Override
		public boolean isSampling() {
			return false;
		}
		
	}
	
	public static enum CounterMember implements AttributeAdapter {
		COUNT("%sCount", "The current %s count");
		
		private CounterMember(final String nameFormat, final String descriptionFormat) {
			this.nameFormat = nameFormat;
			this.descriptionFormat = descriptionFormat;			
		}

		/** The format for how the attribute name is created for this metric member */
		public final String nameFormat;
		/** The format for how the attribute description is created for this metric member */
		public final String descriptionFormat;
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#members()
		 */
		@Override
		public AttributeAdapter[] members() {			
			return values();
		}
		
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#attributeName(java.lang.String)
		 */
		@Override
		public String attributeName(final String name) {
			return String.format(nameFormat, name);
		}
		
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#attributeDescription(java.lang.String, java.lang.String)
		 */
		@Override
		public String attributeDescription(final String name, final String custom) {
			final String desc = String.format(descriptionFormat, name);
			if(custom!=null && !custom.trim().isEmpty()) {
				return desc + ". " + custom.trim();
			}
			return desc;
		}
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#invoke(com.codahale.metrics.Metric)
		 */
		@Override
		public Object invoke(final Metric metric) {
			return ((Counting)metric).getCount();
		}
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#dataType(com.codahale.metrics.Metric)
		 */
		@Override
		public String dataType(final Metric metric) {
			return "long";
		}
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#attrInfo(java.lang.String, java.lang.String, com.codahale.metrics.Metric)
		 */
		@Override
		public MBeanAttributeInfo attrInfo(final String name, final String custom, final Metric metric) {
			return new MBeanAttributeInfo(attributeName(name), dataType(metric), attributeDescription(name, custom), true, false, false, descriptor(FluentMap.newMap(String.class, Object.class)
					.fput("metricClass", com.codahale.metrics.Counter.class.getName())
					.fput("metricType", "counter")
			)); 
		}
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#isSampling()
		 */
		@Override
		public boolean isSampling() {
			return false;
		}
	}
	
	public static enum MeterMember implements AttributeAdapter {
		RATE15M("%s15mRate", "The fifteen-minute exponentially-weighted moving average rate at which %s events have occurred since the meter was created", "double"),
		RATE5M("%s5mRate", "The five-minute exponentially-weighted moving average rate at which %s events have occurred since the meter was created", "double"),
		RATE1M("%s1mRate", "The one-minute exponentially-weighted moving average rate at which %s events have occurred since the meter was created", "double"),
		RATEMEAN("%sMeanRate", "The mean rate at which %s events have occurred since the meter was created", "double"),
		RATECOUNT("%sRateCount", "The number of %s marked events", "long");
		
		private MeterMember(final String nameFormat, final String descriptionFormat, final String dataType) {
			this.nameFormat = nameFormat;
			this.descriptionFormat = descriptionFormat;
			this.dataType = dataType;
		}

		/** The format for how the attribute name is created for this metric member */
		public final String nameFormat;
		/** The format for how the attribute description is created for this metric member */
		public final String descriptionFormat;		
		/** The data type */
		public final String dataType;
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#members()
		 */
		@Override
		public AttributeAdapter[] members() {			
			return values();
		}
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#attributeName(java.lang.String)
		 */
		@Override
		public String attributeName(final String name) {
			return String.format(nameFormat, name);
		}
		
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#attributeDescription(java.lang.String, java.lang.String)
		 */
		@Override
		public String attributeDescription(final String name, final String custom) {
			final String desc = String.format(descriptionFormat, name);
			if(custom!=null && !custom.trim().isEmpty()) {
				return desc + ". " + custom.trim();
			}
			return desc;
		}

		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#dataType(com.codahale.metrics.Metric)
		 */
		@Override
		public String dataType(final Metric metric) {
			return dataType;
		}
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#attrInfo(java.lang.String, java.lang.String, com.codahale.metrics.Metric)
		 */
		@Override
		public MBeanAttributeInfo attrInfo(final String name, final String custom, final Metric metric) {
			return new MBeanAttributeInfo(attributeName(name), dataType(metric), attributeDescription(name, custom), true, false, false, descriptor(FluentMap.newMap(String.class, Object.class)
					.fput("metricClass", com.codahale.metrics.Counter.class.getName())
					.fput("metricType", RATECOUNT==this ? "counter" : "gauge")
			)); 
		}
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#invoke(com.codahale.metrics.Metric)
		 */
		@Override
		public Object invoke(final Metric metric) {
			final Meter m = (Meter)metric;
			switch(this) {
				case RATE15M:
					return m.getFifteenMinuteRate();
				case RATE1M:
					return m.getOneMinuteRate();
				case RATE5M:
					return m.getFiveMinuteRate();
				case RATECOUNT:
					return m.getCount();
				case RATEMEAN:
					return m.getMeanRate();
				default:
					throw new RuntimeException();
			}
		}
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#isSampling()
		 */
		@Override
		public boolean isSampling() {
			return false;
		}
		
	}
	
	public static enum HistogramMember implements AttributeAdapter {
		HISTCOUNT("%sHistCount", "The number of %s recorded events");
		
		// SnapshotMember
		
		private HistogramMember(final String nameFormat, final String descriptionFormat) {
			this.nameFormat = nameFormat;
			this.descriptionFormat = descriptionFormat;			
		}

		/** The format for how the attribute name is created for this metric member */
		public final String nameFormat;
		/** The format for how the attribute description is created for this metric member */
		public final String descriptionFormat;	
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#members()
		 */
		@Override
		public AttributeAdapter[] members() {			
			return values();
		}
		
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#isSampling()
		 */
		@Override
		public boolean isSampling() {
			return true;
		}
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#attributeName(java.lang.String)
		 */
		@Override
		public String attributeName(final String name) {
			return String.format(nameFormat, name);
		}
		
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#attributeDescription(java.lang.String, java.lang.String)
		 */
		@Override
		public String attributeDescription(final String name, final String custom) {
			final String desc = String.format(descriptionFormat, name);
			if(custom!=null && !custom.trim().isEmpty()) {
				return desc + ". " + custom.trim();
			}
			return desc;
		}

		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#dataType(com.codahale.metrics.Metric)
		 */
		@Override
		public String dataType(final Metric metric) {
			return "long";
		}
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#attrInfo(java.lang.String, java.lang.String, com.codahale.metrics.Metric)
		 */
		@Override
		public MBeanAttributeInfo attrInfo(final String name, final String custom, final Metric metric) {
			return new MBeanAttributeInfo(attributeName(name), dataType(metric), attributeDescription(name, custom), true, false, false, descriptor(FluentMap.newMap(String.class, Object.class)
					.fput("metricClass", com.codahale.metrics.Counter.class.getName())
					.fput("metricType", HISTCOUNT==this ? "counter" : "gauge")
			)); 
		}
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#invoke(com.codahale.metrics.Metric)
		 */
		@Override
		public Object invoke(final Metric metric) {
			return ((Counting)metric).getCount();
		}
		
	}
	
	public static enum TimerMember implements AttributeAdapter {
		TIMERCOUNT("%sTimerCount", "The number of %s marked events");
		
		// RateMember, SnapshotMember
		
		private TimerMember(final String nameFormat, final String descriptionFormat) {
			this.nameFormat = nameFormat;
			this.descriptionFormat = descriptionFormat;			
		}

		/** The format for how the attribute name is created for this metric member */
		public final String nameFormat;
		/** The format for how the attribute description is created for this metric member */
		public final String descriptionFormat;		
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#members()
		 */
		@Override
		public AttributeAdapter[] members() {			
			return values();
		}
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#isSampling()
		 */
		@Override
		public boolean isSampling() {
			return true;
		}
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#attributeName(java.lang.String)
		 */
		@Override
		public String attributeName(final String name) {
			return String.format(nameFormat, name);
		}
		
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#attributeDescription(java.lang.String, java.lang.String)
		 */
		@Override
		public String attributeDescription(final String name, final String custom) {
			final String desc = String.format(descriptionFormat, name);
			if(custom!=null && !custom.trim().isEmpty()) {
				return desc + ". " + custom.trim();
			}
			return desc;
		}

		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#dataType(com.codahale.metrics.Metric)
		 */
		@Override
		public String dataType(final Metric metric) {
			return "long";
		}
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#attrInfo(java.lang.String, java.lang.String, com.codahale.metrics.Metric)
		 */
		@Override
		public MBeanAttributeInfo attrInfo(final String name, final String custom, final Metric metric) {
			return new MBeanAttributeInfo(attributeName(name), dataType(metric), attributeDescription(name, custom), true, false, false, descriptor(FluentMap.newMap(String.class, Object.class)
					.fput("metricClass", com.codahale.metrics.Counter.class.getName())
					.fput("metricType", TIMERCOUNT==this ? "counter" : "gauge")
			)); 
		}
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#invoke(com.codahale.metrics.Metric)
		 */
		@Override
		public Object invoke(final Metric metric) {
			return ((Counting)metric).getCount();
		}
		
		
		
	}
	
	public static enum GaugeMember implements AttributeAdapter{
		GAUGE("%sGauge", "The current value for %s");
		
		// RateMember, SnapshotMember
		
		private GaugeMember(final String nameFormat, final String descriptionFormat) {
			this.nameFormat = nameFormat;
			this.descriptionFormat = descriptionFormat;			
		}

		/** The format for how the attribute name is created for this metric member */
		public final String nameFormat;
		/** The format for how the attribute description is created for this metric member */
		public final String descriptionFormat;		
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#members()
		 */
		@Override
		public AttributeAdapter[] members() {			
			return values();
		}
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#isSampling()
		 */
		@Override
		public boolean isSampling() {
			return true;
		}
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#attributeName(java.lang.String)
		 */
		@Override
		public String attributeName(final String name) {
			return String.format(nameFormat, name);
		}
		
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#attributeDescription(java.lang.String, java.lang.String)
		 */
		@Override
		public String attributeDescription(final String name, final String custom) {
			final String desc = String.format(descriptionFormat, name);
			if(custom!=null && !custom.trim().isEmpty()) {
				return desc + ". " + custom.trim();
			}
			return desc;
		}

		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#dataType(com.codahale.metrics.Metric)
		 */
		@Override
		public String dataType(final Metric metric) {
			return ((Gauge)metric).getValue().getClass().getName();
		}
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#attrInfo(java.lang.String, java.lang.String, com.codahale.metrics.Metric)
		 */
		@Override
		public MBeanAttributeInfo attrInfo(final String name, final String custom, final Metric metric) {
			return new MBeanAttributeInfo(attributeName(name), dataType(metric), attributeDescription(name, custom), true, false, false, descriptor(FluentMap.newMap(String.class, Object.class)
					.fput("metricClass", com.codahale.metrics.Counter.class.getName())
					.fput("metricType", "gauge")
			)); 
		}
		
		/**
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.jmx.metrics.MetricType.AttributeAdapter#invoke(com.codahale.metrics.Metric)
		 */
		@Override
		public Object invoke(final Metric metric) {
			return ((Gauge)metric).getValue();
		}
		
		
	}
	
	
}



/*

Snapshot:
=========highest 
Min
Max
Median
75thPercentile
95thPercentile
98thPercentile
99thPercentile
999thPercentile
Mean
StdDev

Timer:
======
Count
FifteenMinuteRate
FiveMinuteRate
MeanRate
OneMinuteRate
[Snapshot]

Meter:
======
Count
FifteenMinuteRate
FiveMinuteRate
MeanRate
OneMinuteRate


Counter:
=======
Count

Histogram:
==========
Count
[Snapshot]

*/