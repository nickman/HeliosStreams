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

import java.lang.reflect.Method;

import javax.management.ImmutableDescriptor;
import javax.management.MBeanAttributeInfo;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Sampling;
import com.codahale.metrics.Snapshot;
import com.heliosapm.utils.collections.FluentMap;
import com.heliosapm.utils.reflect.PrivateAccessor;
import com.heliosapm.utils.tuples.NVP;

/**
 * <p>Title: MetricAttributeHandler</p>
 * <p>Description: Generates MBeanInfo for a registered metric</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.jmx.metrics.MetricAttributeHandler</code></p>
 */

public interface MetricAttributeHandler {
	
	public NVP<Metric, MBeanAttributeInfo[]> minfo(final Metric metric);
	
	
	
	public static interface Getter {
		public Object get();
	}
	
	public static class AttributeGetter implements Getter {
		private final Object target;
		private final Method method;
		
		AttributeGetter(final Object target, final String methodName) {
			this.target = target;
			method = PrivateAccessor.findMethodFromClass(target.getClass(), methodName);
		}
		
		public Object get() {
			return PrivateAccessor.getMethodResult(method, target);
		}
	}
	
	public static class SnapshotAttributeGetter implements Getter {
		private final Snapshot target;
		private final Method method;
		
		SnapshotAttributeGetter(final Sampling target, final String methodName) {
			this.target = target.getSnapshot();
			method = PrivateAccessor.findMethodFromClass(Snapshot.class, methodName);
		}
		
		public Object get() {
			return PrivateAccessor.getMethodResult(method, target);
		}
	}
	
	
	public static class CounterMBeanInfoGenerator implements MetricAttributeHandler {
		@Override
		public NVP<Metric, MBeanAttributeInfo[]> minfo(final Metric metric) {
			return new NVP<Metric, MBeanAttributeInfo[]>(metric, new MBeanAttributeInfo[]{
				new MBeanAttributeInfo("Count", "long", "The counter's current value", true, false, false, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
						.fput("metricClass", com.codahale.metrics.Counter.class.getName())
						.fput("invoker", new AttributeGetter(metric, "getCount"))
						.fput("metricType", "counter")
					))
			});
		}
	}
	
	public static class GaugeMBeanInfoGenerator implements MetricAttributeHandler {
		@Override
		public NVP<Metric, MBeanAttributeInfo[]> minfo(final Metric metric) {
			final Object sample = ((Gauge<?>)metric).getValue();
			
			return new NVP<Metric, MBeanAttributeInfo[]>(metric, new MBeanAttributeInfo[]{
				new MBeanAttributeInfo("Gauge", sample.getClass().getName(), "The gauges's current value", true, false, false, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
						.fput("metricClass", com.codahale.metrics.Gauge.class.getName())
						.fput("invoker", new AttributeGetter(metric, "getValue"))
						.fput("metricType", "gauge")
					))
			});
		}
	}
	
	
	public static class MeterMBeanInfoGenerator implements MetricAttributeHandler {
		@Override
		public NVP<Metric, MBeanAttributeInfo[]> minfo(final Metric metric) {
			return new NVP<Metric, MBeanAttributeInfo[]>(metric, new MBeanAttributeInfo[]{
				new MBeanAttributeInfo("MeterCount", "long", "The number of events that have been marked", true, false, false, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
						.fput("metricClass", com.codahale.metrics.Meter.class.getName())
						.fput("invoker", new AttributeGetter(metric, "getCount"))
						.fput("metricType", "counter")
					)),
				new MBeanAttributeInfo("Rate15m", "double", "The fifteen-minute exponentially-weighted moving average rate at which events have occurred since the meter was created", true, false, false, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
						.fput("metricClass", com.codahale.metrics.Meter.class.getName())
						.fput("invoker", new AttributeGetter(metric, "getFifteenMinuteRate"))
						.fput("metricType", "gauge")
					)),
				new MBeanAttributeInfo("Rate5m", "double", "The five-minute exponentially-weighted moving average rate at which events have occurred since the meter was created", true, false, false, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
						.fput("metricClass", com.codahale.metrics.Meter.class.getName())
						.fput("invoker", new AttributeGetter(metric, "getFiveMinuteRate"))
						.fput("metricType", "gauge")
					)),
				new MBeanAttributeInfo("Rate1m", "double", "The one-minute exponentially-weighted moving average rate at which events have occurred since the meter was created", true, false, false, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
						.fput("metricClass", com.codahale.metrics.Meter.class.getName())
						.fput("invoker", new AttributeGetter(metric, "getOneMinuteRate"))
						.fput("metricType", "gauge")
					)),
				new MBeanAttributeInfo("MeanRate", "double", "The mean rate at which events have occurred since the meter was created", true, false, false, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
						.fput("metricClass", com.codahale.metrics.Meter.class.getName())
						.fput("invoker", new AttributeGetter(metric, "getMeanRate"))
						.fput("metricType", "gauge")
					))
			});
		}
	}
	
	public static class HistogramMBeanInfoGenerator implements MetricAttributeHandler {
		@Override
		public NVP<Metric, MBeanAttributeInfo[]> minfo(final Metric metric) {
			return new NVP<Metric, MBeanAttributeInfo[]>(metric, new MBeanAttributeInfo[]{
				new MBeanAttributeInfo("HistogramCount", "long", "The number of values that have been recorded", true, false, false, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
						.fput("metricClass", com.codahale.metrics.Histogram.class.getName())
						.fput("invoker", new AttributeGetter(metric, "getCount"))
						.fput("metricType", new AttributeGetter(metric, "counter"))
					)),
				new MBeanAttributeInfo("75thPct", "double", "The value at the 75th percentile in the distribution.", true, false, false, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
						.fput("metricClass", com.codahale.metrics.Histogram.class.getName())
						.fput("invoker", new AttributeGetter(metric, "get75thPercentile"))
						.fput("metricType", new SnapshotAttributeGetter((Sampling)metric, "gauge"))
					)),
				new MBeanAttributeInfo("95thPct", "double", "The value at the 95th percentile in the distribution.", true, false, false, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
						.fput("metricClass", com.codahale.metrics.Histogram.class.getName())
						.fput("invoker", new AttributeGetter(metric, "get95thPercentile"))
						.fput("metricType", new SnapshotAttributeGetter((Sampling)metric, "gauge"))
					)),
				new MBeanAttributeInfo("98thPct", "double", "The value at the 98th percentile in the distribution.", true, false, false, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
						.fput("metricClass", com.codahale.metrics.Histogram.class.getName())
						.fput("invoker", new AttributeGetter(metric, "get98thPercentile"))
						.fput("metricType", new SnapshotAttributeGetter((Sampling)metric, "gauge"))
					)),
				new MBeanAttributeInfo("99thPct", "double", "The value at the 99th percentile in the distribution.", true, false, false, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
						.fput("metricClass", com.codahale.metrics.Histogram.class.getName())
						.fput("invoker", new AttributeGetter(metric, "get99thPercentile"))
						.fput("metricType", new SnapshotAttributeGetter((Sampling)metric, "gauge"))
					)),
				new MBeanAttributeInfo("999thPct", "double", "The value at the 99.9th percentile in the distribution.", true, false, false, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
						.fput("metricClass", com.codahale.metrics.Histogram.class.getName())
						.fput("invoker", new AttributeGetter(metric, "get999thPercentile"))
						.fput("metricType", new SnapshotAttributeGetter((Sampling)metric, "gauge"))
					)),
				new MBeanAttributeInfo("HistogramMax", "double", "The highest value in the snapshot", true, false, false, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
						.fput("metricClass", com.codahale.metrics.Histogram.class.getName())
						.fput("invoker", new AttributeGetter(metric, "getMax"))
						.fput("metricType", new SnapshotAttributeGetter((Sampling)metric, "gauge"))
					)),
				new MBeanAttributeInfo("HistogramMin", "double", "The lowest value in the snapshot", true, false, false, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
						.fput("metricClass", com.codahale.metrics.Histogram.class.getName())
						.fput("invoker", new AttributeGetter(metric, "getMin"))
						.fput("metricType", new SnapshotAttributeGetter((Sampling)metric, "gauge"))
					)),
				new MBeanAttributeInfo("HistogramMean", "double", "The arithmetic mean of values in the snapshot", true, false, false, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
						.fput("metricClass", com.codahale.metrics.Histogram.class.getName())
						.fput("invoker", new AttributeGetter(metric, "getMean"))
						.fput("metricType", new SnapshotAttributeGetter((Sampling)metric, "gauge"))
					)),
				new MBeanAttributeInfo("HistogramMedian", "double", "The median of values in the snapshot", true, false, false, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
						.fput("metricClass", com.codahale.metrics.Histogram.class.getName())
						.fput("invoker", new AttributeGetter(metric, "getMedian"))
						.fput("metricType", new SnapshotAttributeGetter((Sampling)metric, "gauge"))
					)),
				new MBeanAttributeInfo("HistogramStdDev", "double", "The standard deviation of values in the snapshot", true, false, false, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
						.fput("metricClass", com.codahale.metrics.Histogram.class.getName())
						.fput("invoker", new AttributeGetter(metric, "getStdDev"))
						.fput("metricType", new SnapshotAttributeGetter((Sampling)metric, "gauge"))
					))
			});
		}
	}
	
	public static class TimerMBeanInfoGenerator implements MetricAttributeHandler {
		@Override
		public NVP<Metric, MBeanAttributeInfo[]> minfo(final Metric metric) {
			return new NVP<Metric, MBeanAttributeInfo[]>(metric, new MBeanAttributeInfo[]{
				new MBeanAttributeInfo("TimerCount", "long", "The number of events that have been marked", true, false, false, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
						.fput("metricClass", com.codahale.metrics.Timer.class.getName())
						.fput("invoker", new AttributeGetter(metric, "getCount"))
						.fput("metricType", new AttributeGetter(metric, "counter"))
					)),
				new MBeanAttributeInfo("TimerRate15m", "double", "The fifteen-minute exponentially-weighted moving average rate at which events have occurred since the meter was created", true, false, false, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
						.fput("metricClass", com.codahale.metrics.Meter.class.getName())
						.fput("invoker", new AttributeGetter(metric, "getFifteenMinuteRate"))
						.fput("metricType", "gauge")
					)),
				new MBeanAttributeInfo("TimerRate5m", "double", "The five-minute exponentially-weighted moving average rate at which events have occurred since the meter was created", true, false, false, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
						.fput("metricClass", com.codahale.metrics.Meter.class.getName())
						.fput("invoker", new AttributeGetter(metric, "getFiveMinuteRate"))
						.fput("metricType", "gauge")
					)),
				new MBeanAttributeInfo("TimerRate1m", "double", "The one-minute exponentially-weighted moving average rate at which events have occurred since the meter was created", true, false, false, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
						.fput("metricClass", com.codahale.metrics.Meter.class.getName())
						.fput("invoker", new AttributeGetter(metric, "getOneMinuteRate"))
						.fput("metricType", "gauge")
					)),
				new MBeanAttributeInfo("TimerMeanRate", "double", "The mean rate at which events have occurred since the meter was created", true, false, false, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
						.fput("metricClass", com.codahale.metrics.Meter.class.getName())
						.fput("invoker", new AttributeGetter(metric, "getMeanRate"))
						.fput("metricType", "gauge")
					)),
				
				new MBeanAttributeInfo("Timer75thPct", "double", "The value at the 75th percentile in the distribution.", true, false, false, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
						.fput("metricClass", com.codahale.metrics.Timer.class.getName())
						.fput("invoker", new AttributeGetter(metric, "get75thPercentile"))
						.fput("metricType", new SnapshotAttributeGetter((Sampling)metric, "gauge"))
					)),
				new MBeanAttributeInfo("Timer95thPct", "double", "The value at the 95th percentile in the distribution.", true, false, false, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
						.fput("metricClass", com.codahale.metrics.Timer.class.getName())
						.fput("invoker", new AttributeGetter(metric, "get95thPercentile"))
						.fput("metricType", new SnapshotAttributeGetter((Sampling)metric, "gauge"))
					)),
				new MBeanAttributeInfo("Timer98thPct", "double", "The value at the 98th percentile in the distribution.", true, false, false, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
						.fput("metricClass", com.codahale.metrics.Timer.class.getName())
						.fput("invoker", new AttributeGetter(metric, "get98thPercentile"))
						.fput("metricType", new SnapshotAttributeGetter((Sampling)metric, "gauge"))
					)),
				new MBeanAttributeInfo("Timer99thPct", "double", "The value at the 99th percentile in the distribution.", true, false, false, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
						.fput("metricClass", com.codahale.metrics.Timer.class.getName())
						.fput("invoker", new AttributeGetter(metric, "get99thPercentile"))
						.fput("metricType", new SnapshotAttributeGetter((Sampling)metric, "gauge"))
					)),
				new MBeanAttributeInfo("Timer999thPct", "double", "The value at the 99.9th percentile in the distribution.", true, false, false, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
						.fput("metricClass", com.codahale.metrics.Timer.class.getName())
						.fput("invoker", new AttributeGetter(metric, "get999thPercentile"))
						.fput("metricType", new SnapshotAttributeGetter((Sampling)metric, "gauge"))
					)),
				new MBeanAttributeInfo("TimerMax", "double", "The highest value in the snapshot", true, false, false, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
						.fput("metricClass", com.codahale.metrics.Timer.class.getName())
						.fput("invoker", new AttributeGetter(metric, "getMax"))
						.fput("metricType", new SnapshotAttributeGetter((Sampling)metric, "gauge"))
					)),
				new MBeanAttributeInfo("TimerMin", "double", "The lowest value in the snapshot", true, false, false, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
						.fput("metricClass", com.codahale.metrics.Timer.class.getName())
						.fput("invoker", new AttributeGetter(metric, "getMin"))
						.fput("metricType", new SnapshotAttributeGetter((Sampling)metric, "gauge"))
					)),
				new MBeanAttributeInfo("TimerMean", "double", "The arithmetic mean of values in the snapshot", true, false, false, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
						.fput("metricClass", com.codahale.metrics.Timer.class.getName())
						.fput("invoker", new AttributeGetter(metric, "getMean"))
						.fput("metricType", new SnapshotAttributeGetter((Sampling)metric, "gauge"))
					)),
				new MBeanAttributeInfo("TimerMedian", "double", "The median of values in the snapshot", true, false, false, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
						.fput("metricClass", com.codahale.metrics.Timer.class.getName())
						.fput("invoker", new AttributeGetter(metric, "getMedian"))
						.fput("metricType", new SnapshotAttributeGetter((Sampling)metric, "gauge"))
					)),
				new MBeanAttributeInfo("TimerStdDev", "double", "The standard deviation of values in the snapshot", true, false, false, new ImmutableDescriptor(FluentMap.newMap(String.class, Object.class)
						.fput("metricClass", com.codahale.metrics.Timer.class.getName())
						.fput("invoker", new AttributeGetter(metric, "getStdDev"))
						.fput("metricType", new SnapshotAttributeGetter((Sampling)metric, "gauge"))
					))
			});
		}
	}
	
}
