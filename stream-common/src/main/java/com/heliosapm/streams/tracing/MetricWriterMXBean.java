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

import java.util.Properties;

/**
 * <p>Title: MetricWriterMXBean</p>
 * <p>Description: MXBean interface for metric writer instances</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tracing.MetricWriterMXBean</code></p>
 */

public interface MetricWriterMXBean {
	/** The template for the JMX MXBean ObjectName */
	public static final String OBJECT_NAME_TEMPLATE = "com.heliosapm.streams.tracing:service=MetricWriter,type=%s";
	
	/**
	 * Returns the total number of metrics written
	 * @return the total number of metrics written
	 */
	public long getSentMetrics();
	
	/**
	 * Returns the total number of metrics confirmed
	 * @return the total number of metrics confirmed
	 */
	public long getConfirmedMetricsSent();
	
	/**
	 * Returns the total number of metrics pending confirmation
	 * @return the total number of metrics pending confirmation
	 */
	public long getPendingMetrics();
	
	/**
	 * Indicates if this writer has confirmed metrics
	 * @return true if this writer has confirmed metrics, false if they are fire-n-forget or synchronous.
	 */
	public boolean isConfirmedMetrics();
	
	/**
	 * Returns the state of this writer
	 * @return the state of this writer
	 */
	public String getState();
	
	/**
	 * Indicates if this writer is connected
	 * @return true if this writer is connected, false otherwise
	 */
	public boolean isConnected();
	
	/**
	 * Returns the total number of dropped metrics
	 * @return the total number of dropped metrics
	 */
	public long getDroppedMetrics();
	
	/**
	 * Returns the writer configuration properties
	 * @return the writer configuration properties
	 */
	public Properties getConfiguration();
	
	/**
	 * Returns a writer state message, specific to the type of writer
	 * @return a writer state message
	 */
	public String getCustomState();
	
}
