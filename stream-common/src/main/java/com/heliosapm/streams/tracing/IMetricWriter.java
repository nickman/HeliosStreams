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

import java.util.Collection;
import java.util.Properties;

import com.google.common.util.concurrent.Service;
import com.heliosapm.streams.metrics.StreamedMetric;

import io.netty.buffer.ByteBuf;

/**
 * <p>Title: IMetricWriter</p>
 * <p>Description: Defines a class that accepts metrics from a tracer and writes it to an end-point</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tracing.IMetricWriter</code></p>
 */

public interface IMetricWriter extends Service, MetricWriterMXBean {
	
	/**
	 * Indicates if this writer fully consumes written metrics before returning.
	 * @return true if metrics are fully consumed, false if they may be cached or linger
	 */
	public boolean areMetricsConsumed();
	
	/**
	 * Indicates if metric delivery is confirmed by callback or if metric writes are fire-n-forget
	 * @return true if metric delivery is confirmed by callback, false if metric writes are fire-n-forget
	 */
	public boolean areMetricsConfirmed();
	
	/**
	 * Handles the delivery of an array of {@link StreamedMetric} instances
	 * @param metrics The metrics to deliver
	 */
	public void onMetrics(final StreamedMetric...metrics);
	
	/**
	 * Handles the delivery of a collection of {@link StreamedMetric} instances
	 * @param metrics The metrics to deliver
	 */
	public void onMetrics(final Collection<StreamedMetric> metrics);
	
	/**
	 * Handles the delivery of a collection of {@link StreamedMetric} instances encoded in a {@link ByteBuf}
	 * @param metrics The buffer containing metrics to deliver
	 * <p>Encoding rules so far: <ol>
	 * 	<li>The first byte represents a <b><code>0</code></b> for no compression, or a <b><code>1</code></b> for gzipped.</li>
	 *  <li>The next 4 bytes represent an <b><code>int</code></b> that indicates how many metrics are contained in the buffer</li>
	 *  <li></li>
	 * </ol></p>
	 */
	public void onMetrics(final ByteBuf metrics);
	
	/**
	 * Indicates the number of metrics sent, but not confirmed
	 * @return the number of metrics sent
	 */
	public long getMetricsSent();
	
	/**
	 * Indicates the number of metrics sent and confirmed
	 * @return the number of confirmed metrics sent
	 */
	public long getConfirmedMetricsSent();
	
	
	/**
	 * Returns the number of metrics sent but pending confirmation
	 * @return the number of metrics sent but pending confirmation
	 */
	public long getMetricsPending();
	
	/**
	 * Indicates the number of metric send errors
	 * @return the number of metric send errors
	 */
	public long getSendErrors();
	
	/**
	 * Passes the writer its configuration
	 * @param config the writer's configuration
	 */
	public void configure(Properties config);
	
	/**
	 * Performs any initialization required for this writer to deliver metrics
	 * @throws Exception thrown on any failure to initialize the writer
	 */
	public void start() throws Exception;
	
	/**
	 * Callback to shutdown and clean up any allocated resoures for this writer
	 */
	public void stop();
	
	/**
	 * Indicates if this writer is started
	 * @return true if this writer is started, false otherwise
	 */
	public boolean isStarted();
	
	/**
	 * Indicates if this writer is connected
	 * @return true if this writer is connected, false otherwise
	 */
	public boolean isConnected();
	
	
}
