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
package com.heliosapm.streams.tracing;

import java.io.InputStream;
import java.util.Collection;
import java.util.Properties;

import javax.management.ObjectName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.util.concurrent.AbstractIdleService;
import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.utils.jmx.JMXHelper;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import jsr166e.LongAdder;

/**
 * <p>Title: AbstractMetricWriter</p>
 * <p>Description: Abstract base class for {@link IMetricWriter} implementations</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tracing.AbstractMetricWriter</code></p>
 */

public abstract class AbstractMetricWriter extends AbstractIdleService implements IMetricWriter {
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	
	/** A counter of sent metrics  */
	protected final LongAdder sentMetrics = new LongAdder();
	/** A counter of confirmed metrics  */
	protected final LongAdder confirmedMetrics = new LongAdder();
	/** A counter of failed metrics  */
	protected final LongAdder failedMetrics = new LongAdder();
	
	/** The configuration properties */
	protected Properties config = null;
	
	/** Indicates if this writer gets metric confirmations */
	protected final boolean confirmsMetrics;
	
	/** Indicates if this writer fully consumes metrics on write before returning */
	protected final boolean metricsConsumed;
	/** The JMX ObjectName for this writer */
	protected final ObjectName objectName = JMXHelper.objectName(String.format(OBJECT_NAME_TEMPLATE, getClass().getSimpleName()));
	
		
	/**
	 * Creates a new AbstractMetricWriter
	 * @param confirmsMetrics true if writer gets confirmations on metric delivery, false otherwise
	 * @param metricsConsumed Indicates if this writer fully consumes metrics on write before returning
	 */
	protected AbstractMetricWriter(final boolean confirmsMetrics, final boolean metricsConsumed) {
		log.info("Created MetricWriter [{}]", getClass().getSimpleName());
		this.confirmsMetrics = confirmsMetrics;
		this.metricsConsumed = metricsConsumed;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.IMetricWriter#areMetricsConsumed()
	 */
	@Override
	public boolean areMetricsConsumed() {
		return metricsConsumed;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.IMetricWriter#areMetricsConfirmed()
	 */
	@Override
	public boolean areMetricsConfirmed() {
		return confirmsMetrics;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.IMetricWriter#onMetrics(com.heliosapm.streams.metrics.StreamedMetric[])
	 */
	@Override
	public void onMetrics(final StreamedMetric... metrics) {
		final int count = metrics.length;
		try {
			doMetrics(metrics);
			sentMetrics.add(count);
		} catch (Exception ex) {
			log.debug("Failed to send {} metrics", count, ex);
			failedMetrics.add(count);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.IMetricWriter#onMetrics(java.util.Collection)
	 */
	@Override
	public void onMetrics(final Collection<StreamedMetric> metrics) {
		final int count = metrics.size();
		try {
			doMetrics(metrics);
			sentMetrics.add(count);
		} catch (Exception ex) {
			log.debug("Failed to send {} metrics", count, ex);
			failedMetrics.add(count);
		}
	}
	
	/**
	 * Delivers the metrics to the configured end-point
	 * @param metrics The metrics to deliver
	 */
	protected abstract void doMetrics(final Collection<StreamedMetric> metrics);

	/**
	 * Delivers the metrics to the configured end-point
	 * @param metrics The metrics to deliver
	 */
	protected abstract void doMetrics(final StreamedMetric... metrics);

	
	/**
	 * {@inheritDoc}
	 * @see com.google.common.util.concurrent.AbstractIdleService#toString()
	 */
	@Override
	public String toString() {
		return getClass().getSimpleName();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.IMetricWriter#onMetrics(io.netty.buffer.ByteBuf)
	 */
	@Override
	public void onMetrics(final ByteBuf metrics) {
		metrics.readerIndex(0);
		final InputStream is = new ByteBufInputStream(metrics);
		try {
			for(StreamedMetric sm: StreamedMetric.streamedMetrics(is, true, false)) {
				onMetrics(sm);
			}
			//onMetrics(StreamedMetric.read(is));
		} finally {
			try { is.close(); } catch (Exception x) {/* No Op */}
		}
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.IMetricWriter#getMetricsSent()
	 */
	@Override
	public long getMetricsSent() {
		return sentMetrics.longValue();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.IMetricWriter#getConfirmedMetricsSent()
	 */
	@Override
	public long getConfirmedMetricsSent() {
		return confirmsMetrics ? confirmedMetrics.longValue() : sentMetrics.longValue();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.IMetricWriter#getMetricsPending()
	 */
	@Override
	public long getMetricsPending() {
		return confirmsMetrics ? confirmedMetrics.longValue() - sentMetrics.longValue() : 0;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.IMetricWriter#getSendErrors()
	 */
	@Override
	public long getSendErrors() {
		return failedMetrics.longValue();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.IMetricWriter#configure(java.util.Properties)
	 */
	@Override
	public void configure(Properties config) {
		/* Default is empty */
	}
	
	protected void doStart() {
		/* Default is empty */
	}
	
	protected void doStop() {
		/* Default is empty */
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.IMetricWriter#start()
	 */
	@Override
	public void start() throws Exception {
		startAsync();
		try { if(JMXHelper.isRegistered(objectName)) JMXHelper.unregisterMBean(objectName); } catch (Exception x) {/* No Op */}
		try { JMXHelper.registerMBean(this, objectName); } catch (Exception ex) {
			log.warn("Failed to register Writer's JMX Management interface. Will continue without.", ex);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.IMetricWriter#stop()
	 */
	@Override
	public void stop() {
		stopAsync();
		try { JMXHelper.unregisterMBean(objectName); } catch (Exception x) {/* No Op */}
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.IMetricWriter#isStarted()
	 */
	@Override
	public boolean isStarted() {
		return isRunning();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.IMetricWriter#isConnected()
	 */
	@Override
	public boolean isConnected() {
		return true;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.MetricWriterMXBean#isConfirmedMetrics()
	 */
	@Override
	public boolean isConfirmedMetrics() {		
		return confirmsMetrics;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.MetricWriterMXBean#getState()
	 */
	@Override
	public String getState() {		
		return state().name();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.MetricWriterMXBean#getSentMetrics()
	 */
	@Override
	public long getSentMetrics() {
		return sentMetrics.longValue();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.MetricWriterMXBean#getPendingMetrics()
	 */
	@Override
	public long getPendingMetrics() {
		return getMetricsPending();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.MetricWriterMXBean#getDroppedMetrics()
	 */
	@Override
	public long getDroppedMetrics() {
		return failedMetrics.longValue();
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.MetricWriterMXBean#getConfiguration()
	 */
	@Override
	public Properties getConfiguration() {		
		return config;
	}
	
}
