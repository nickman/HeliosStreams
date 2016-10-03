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
package com.heliosapm.streams.opentsdb;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.heliosapm.streams.opentsdb.plugin.PluginMetricManager;
import com.heliosapm.utils.jmx.JMXHelper;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.Annotation;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.tsd.RTPublisher;

/**
 * <p>Title: EventCounterRTPublisher</p>
 * <p>Description: Simple event meter {@link RTPublisher}</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.opentsdb.EventCounterRTPublisher</code></p>
 */

public class EventCounterRTPublisher extends RTPublisher implements EventCounterRTPublisherMBean {
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** The metric manager for this plugin */
	protected final PluginMetricManager metricManager = new PluginMetricManager(getClass().getSimpleName());
//	/** Counter for all processed events */
//	protected final LongAdder totalEventsCounter = new LongAdder();
//	/** Counter for all processed datapoints */
//	protected final LongAdder dataPointsCounter = metricManager.meter((totalEventsCounter);
//	/** Counter for all processed long datapoints */
//	protected final LongAdder longDataPointsCounter = metricManager.meter((dataPointsCounter);
//	/** Counter for all processed double datapoints */
//	protected final LongAdder doubleDataPointsCounter = metricManager.meter((dataPointsCounter);
//	/** Counter for all processed annotations */
//	protected final LongAdder annotationsCounter = metricManager.meter((totalEventsCounter);

	/** Meter for all processed events */
	protected final Meter totalEventsMeter = metricManager.meter("events");
	/** Meter for all processed datapoints */
	protected final Meter dataPointsMeter = metricManager.meter("datapoints");
	/** Meter for all processed long datapoints */
	protected final Meter longDataPointsMeter = metricManager.meter("long-datapoints");
	/** Meter for all processed double datapoints */
	protected final Meter doubleDataPointsMeter = metricManager.meter("double-datapoints");
	/** Meter for all processed annotations */
	protected final Meter annotationsMeter = metricManager.meter("annotations");
	/** Timer for processing events */
	protected final Timer eventHandlerTimer = metricManager.timer("event-handler");
	
	

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#initialize(net.opentsdb.core.TSDB)
	 */
	@Override
	public void initialize(final TSDB tsdb) {
		JMXHelper.registerMBean(JMXHelper.objectName("net.opentsdb:service=EventMeter"), this);
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#shutdown()
	 */
	@Override
	public Deferred<Object> shutdown() {
		return Deferred.fromResult(null);
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#version()
	 */
	@Override
	public String version() {
		return "2.1";
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#collectStats(net.opentsdb.stats.StatsCollector)
	 */
	@Override
	public void collectStats(final StatsCollector collector) {
		metricManager.collectStats(collector);
	}
	
	
	class CtxCallback implements Callback<Void, Void> {
		final Context ctx;

		public CtxCallback(final Context ctx) {			
			this.ctx = ctx;
		}
		
		@Override
		public Void call(final Void arg) throws Exception {
			ctx.stop();
			return null;
		}
		
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#publishDataPoint(java.lang.String, long, long, java.util.Map, byte[])
	 */
	@Override
	public Deferred<Object> publishDataPoint(final String metric, final long timestamp, final long value, final Map<String, String> tags, final byte[] tsuid) {
		final Context ctx = eventHandlerTimer.time();
		longDataPointsMeter.mark();
		totalEventsMeter.mark();
		dataPointsMeter.mark();
		ctx.stop();
		return Deferred.fromResult(null);
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#publishDataPoint(java.lang.String, long, double, java.util.Map, byte[])
	 */
	@Override
	public Deferred<Object> publishDataPoint(final String metric, final long timestamp, final double value, final Map<String, String> tags, final byte[] tsuid) {
		final Context ctx = eventHandlerTimer.time();
		doubleDataPointsMeter.mark();
		totalEventsMeter.mark();
		dataPointsMeter.mark();
		ctx.stop();
		return Deferred.fromResult(null);
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#publishAnnotation(net.opentsdb.meta.Annotation)
	 */
	@Override
	public Deferred<Object> publishAnnotation(final Annotation annotation) {
		final Context ctx = eventHandlerTimer.time();
		annotationsMeter.mark();
		totalEventsMeter.mark();
		ctx.stop();
		return Deferred.fromResult(null);
	}
	
	
	/**
	 * Returns the rate of events in the last minute
	 * @return the rate of events in the last minute
	 */
	public double getTotalOneMinuteRate() {
		return totalEventsMeter.getOneMinuteRate();
	}
	
	/**
	 * Returns the rate of datapoints in the last minute
	 * @return the rate of datapoints in the last minute
	 */
	public double getDataPointsOneMinuteRate() {
		return dataPointsMeter.getOneMinuteRate();
	}
	
	/**
	 * Returns the rate of long datapoints in the last minute
	 * @return the rate of long datapoints in the last minute
	 */
	public double getLongDataPointsOneMinuteRate() {
		return longDataPointsMeter.getOneMinuteRate();
	}
	
	/**
	 * Returns the rate of double datapoints in the last minute
	 * @return the rate of double datapoints in the last minute
	 */
	public double getDoubleDataPointsOneMinuteRate() {
		return doubleDataPointsMeter.getOneMinuteRate();
	}
	
	/**
	 * Returns the rate of annotations in the last minute
	 * @return the rate of annotations in the last minute
	 */
	public double getAnnotationsOneMinuteRate() {
		return annotationsMeter.getOneMinuteRate();
	}
	
	
	
//	/**
//	 * Returns the total number of ingested events
//	 * @return the total number of ingested events
//	 */
//	public long getTotalEventsCounter() {
//		return totalEventsCounter.longValue();
//	}
//
//	/**
//	 * Returns the total number of ingested datapoints
//	 * @return the total number of ingested datapoints
//	 */
//	public long getDataPointsCounter() {
//		return dataPointsCounter.longValue();
//	}
//
//	/**
//	 * Returns the total number of ingested long datapoints
//	 * @return the total number of ingested long datapoints
//	 */
//	public long getLongDataPointsCounter() {
//		return longDataPointsCounter.longValue();
//	}
//
//	/**
//	 * Returns the total number of ingested double datapoints
//	 * @return the total number of ingested double datapoints
//	 */
//	public long getDoubleDataPointsCounter() {
//		return doubleDataPointsCounter.longValue();
//	}
//
//	/**
//	 * Returns the total number of ingested annotations
//	 * @return the total number of ingested annotations
//	 */
//	public long getAnnotationsCounter() {
//		return annotationsCounter.longValue();
//	}
	

}
