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
import java.util.Map;

import javax.management.MBeanAttributeInfo;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Timer;
import com.heliosapm.streams.jmx.metrics.MBeanInfoGenerator.CounterMBeanInfoGenerator;
import com.heliosapm.streams.jmx.metrics.MBeanInfoGenerator.HistogramMBeanInfoGenerator;
import com.heliosapm.streams.jmx.metrics.MBeanInfoGenerator.TimerMBeanInfoGenerator;
import com.heliosapm.utils.tuples.NVP;


/**
 * <p>Title: MetricType</p>
 * <p>Description: Enumerates the metric type that can be aggregated into a {@link DropWizardMetrics} instance</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.jmx.metrics.MetricType</code></p>
 */

public enum MetricType implements MBeanInfoGenerator {
	/** Dropwizard Counter */
	COUNTER(Counter.class, new CounterMBeanInfoGenerator()),
	/** Dropwizard Timer */
	TIMER(Timer.class, new TimerMBeanInfoGenerator()),
	/** Dropwizard Histogram */
	HISTOGRAM(Histogram.class, new HistogramMBeanInfoGenerator()),
	/** Dropwizard Meter */
	METER(Meter.class, new TimerMBeanInfoGenerator());
	
	private MetricType(final Class<? extends Metric> baseType, final MBeanInfoGenerator infoGenerator) {
		this.baseType = baseType;
		this.infoGenerator = infoGenerator;
	}
	
	/** The base metric type */
	public final Class<? extends Metric> baseType;
	/** The MBeanInfo generator for the metric's attributes */
	public final MBeanInfoGenerator infoGenerator;
	
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
	 * @see com.heliosapm.streams.jmx.metrics.MBeanInfoGenerator#minfo(com.codahale.metrics.Metric)
	 */
	@Override
	public NVP<Metric, MBeanAttributeInfo[]> minfo(Metric metric) {		
		return infoGenerator.minfo(metric);
	}
}



/*

Snapshot:
=========
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