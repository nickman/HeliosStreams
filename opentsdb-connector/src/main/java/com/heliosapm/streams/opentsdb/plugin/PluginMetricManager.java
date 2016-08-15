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
package com.heliosapm.streams.opentsdb.plugin;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.heliosapm.streams.common.metrics.SharedMetricsRegistry;
import com.heliosapm.streams.common.naming.AgentName;

import net.opentsdb.stats.StatsCollector;

/**
 * <p>Title: PluginMetricManager</p>
 * <p>Description: Extends the base abstract plugins to manage metrics for the plugin</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.opentsdb.plugin.PluginMetricManager</code></p>
 * 
 */

public class PluginMetricManager {
	/** The number of cores available */
	public static final int CORES = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
	/** The agent naming service */
	protected final AgentName agentName;
	/** The service tag */
	protected final String serviceTag;
	/** The shared metrics registry */
	protected final SharedMetricsRegistry metricRegistry = SharedMetricsRegistry.getInstance();
	
	/** A map of gauges for this plugin */
	@SuppressWarnings("rawtypes")
	protected final Map<String, Gauge> gauges = new ConcurrentHashMap<String, Gauge>(16, 0.75f, CORES);
	/** A map of counters for this plugin */
	protected final Map<String, Counter> counters = new ConcurrentHashMap<String, Counter>(16, 0.75f, CORES);
	/** A map of timers for this plugin */
	protected final Map<String, Timer> timers = new ConcurrentHashMap<String, Timer>(16, 0.75f, CORES);
	/** A map of meters for this plugin */
	protected final Map<String, Meter> meters = new ConcurrentHashMap<String, Meter>(16, 0.75f, CORES);
	/** A map of histograms for this plugin */
	protected final Map<String, Histogram> histograms = new ConcurrentHashMap<String, Histogram>(16, 0.75f, CORES);
	
	/** Extra tags configured by this plugin */
	protected final Map<String, String> extraTags = new HashMap<String, String>();
	
	
	/**
	 * Creates a new PluginMetricManager
	 * @param serviceName The service name assigned to this plugin
	 */
	public PluginMetricManager(final String serviceName) {
		agentName = AgentName.getInstance();
		serviceTag = "." + serviceName;// + ".host=" + agentName.getHostName() + ".app=" + agentName.getAppName();
		extraTags.put("service", serviceName);
		extraTags.put("app", agentName.getAppName());
		extraTags.put("host", agentName.getHostName());
	}
	
	/**
	 * Adds an extra tag to this plugin's metrics
	 * @param tagKey The tag key
	 * @param tagValue The tag value
	 */
	public void addExtraTag(final String tagKey, final String tagValue) {
		extraTags.put(tagKey, tagValue);
	}
	

	/**
	 * Acquires the named gauge 
	 * @param name The name of the gauge
	 * @param provider The value provider for the gauge
	 * @return the gauge
	 */
	public <T> Gauge<T> gauge(final String name, final Callable<T> provider) {
		final String fullName = name + serviceTag;
		final Gauge<T> g = metricRegistry.gauge(fullName, provider);
		gauges.put(name, g);
		return g;
	}
	
	/**
	 * Acquires the named cached gauge 
	 * @param name The name of the gauge
	 * @param provider The value provider for the gauge
	 * @param refreshPeriod The refresh period of the cached gauge in seconds
	 * @return the cached gauge
	 */
	public <T> CachedGauge<T> gauge(final String name, final Callable<T> provider, final int refreshPeriod) {
		final String fullName = name + serviceTag;
		final CachedGauge<T> g = new CachedGauge<T>(refreshPeriod, TimeUnit.SECONDS) {
			@Override
			protected T loadValue() {
				try {
				return provider.call();
				} catch (Exception ex) {
					throw new RuntimeException("Failed to get cached value for [" + name + "]", ex);
				}
			}
		};
		metricRegistry.register("gauge." + fullName, g);
		gauges.put(name, g);
		return g;
	}
	

	/**
	 * Acquires the named counter 
	 * @param name The name of the counter
	 * @return the counter
	 */
	public Counter counter(final String name) {
		final String fullName = name + serviceTag;
		final Counter c = metricRegistry.counter(fullName);
		counters.put(name, c);
		return c;
	}

	/**
	 * Acquires the named timer
	 * @param name The name of the timer
	 * @return the timer
	 */
	public Timer timer(final String name) {
		final String fullName = name + serviceTag;
		final Timer t = metricRegistry.timer(fullName);
		timers.put(name, t);
		return t;
	}

	/**
	 * Acquires the named histogram
	 * @param name The name of the histogram
	 * @return the histogram
	 */
	public Histogram histogram(final String name) {
		final String fullName = name + serviceTag;
		final Histogram t = metricRegistry.histogram(fullName);
		histograms.put(name, t);
		return t;
	}
	
	/**
	 * Acquires the named meter
	 * @param name The name of the meter
	 * @return the meter
	 */
	public Meter meter(final String name) {
		final String fullName = name + serviceTag;
		final Meter t = metricRegistry.meter(fullName);
		meters.put(fullName, t);
		return t;
	}
	

	/**
	 * Collects stats for this plugin
	 * @param collector The supplied stats collector
	 */
	
	// java.lang.IllegalArgumentException: Invalid metric name (
	// "tsd.pointsAdded.KafkaRPC.host=pp-dt-nwhi-01.app=opentsdb.rate.1m"): 
	// illegal character: =
	public void collectStats(final StatsCollector collector) {
		for(Map.Entry<String, String> xtag : extraTags.entrySet()) {
			collector.addExtraTag(xtag.getKey(), xtag.getValue());
		}
		try {
			for(@SuppressWarnings("rawtypes") Map.Entry<String, Gauge> entry: gauges.entrySet()) {
				collector.record(entry.getKey(), (Number)entry.getValue().getValue());
			}
			for(Map.Entry<String, Counter> entry: counters.entrySet()) {
				collector.record(entry.getKey(), entry.getValue().getCount());
			}
			for(Map.Entry<String, Timer> entry: timers.entrySet()) {
				final Timer t = entry.getValue();				
				final String p = entry.getKey();
				collector.record(p + ".mean", t.getMeanRate());
				collector.record(p + ".count", t.getCount());
				collector.record(p + ".rate.15m", t.getFifteenMinuteRate());
				collector.record(p + ".rate.5m", t.getFiveMinuteRate());
				collector.record(p + ".rate.1m", t.getOneMinuteRate());
				final Snapshot snap = t.getSnapshot();
				collector.record(p + ".time.mean", snap.getMean());
				collector.record(p + ".time.median", snap.getMedian());
				collector.record(p + ".time.p999", snap.get999thPercentile());
				collector.record(p + ".time.p99", snap.get99thPercentile());
				collector.record(p + ".time.p75", snap.get75thPercentile());
			}
			for(Map.Entry<String, Meter> entry: meters.entrySet()) {
				final Meter m = entry.getValue();
				final String p = entry.getKey();
				collector.record(p + ".rate.mean", m.getMeanRate());
				collector.record(p + ".rate.15m", m.getFifteenMinuteRate());
				collector.record(p + ".rate.5m", m.getFiveMinuteRate());
				collector.record(p + ".rate.1m", m.getOneMinuteRate());				
			}
			
			for(Map.Entry<String, Histogram> entry: histograms.entrySet()) {
				final Histogram h = entry.getValue();
				final String p = entry.getKey();
				collector.record(p + ".count", h.getCount());
				final Snapshot snap = h.getSnapshot();
				collector.record(p + ".time.mean", snap.getMean());
				collector.record(p + ".time.median", snap.getMedian());
				collector.record(p + ".time.p999", snap.get999thPercentile());
				collector.record(p + ".time.p99", snap.get99thPercentile());
				collector.record(p + ".time.p75", snap.get75thPercentile());				
			}
		} finally {
			for(String xtagKey : extraTags.keySet()) {
				collector.clearExtraTag(xtagKey);
			}
		}
	}
	
	
}
