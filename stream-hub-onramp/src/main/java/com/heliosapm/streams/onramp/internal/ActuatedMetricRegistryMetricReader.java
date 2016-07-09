/**
 * Helios, OpenSource Monitoring
 * Brought to you by the Helios Development Group
 *
 * Copyright 2016, Helios Development Group and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org. 
 *
 */
package com.heliosapm.streams.onramp.internal;

import org.springframework.boot.actuate.autoconfigure.ExportMetricReader;
import org.springframework.boot.actuate.metrics.reader.MetricReader;
import org.springframework.boot.actuate.metrics.reader.MetricRegistryMetricReader;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import com.heliosapm.streams.common.metrics.SharedMetricsRegistry;



/**
 * <p>Title: ActuatedMetricRegistryMetricReader</p>
 * <p>Description: A compatibility layer betweenour metrics and their metrics</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.internal.ActuatedMetricRegistryMetricReader</code></p>
 */
@Service
public class ActuatedMetricRegistryMetricReader extends MetricRegistryMetricReader {
	/** The shared metrics registry */
	protected final SharedMetricsRegistry sharedRegistry;
	/**
	 * Creates a new ActuatedMetricRegistryMetricReader
	 */
	public ActuatedMetricRegistryMetricReader() {
		super(SharedMetricsRegistry.getInstance());
		sharedRegistry = SharedMetricsRegistry.getInstance();
	}
	
	/**
	 * Exports a metric reader
	 * @return a metric reader
	 */
	@Bean
	@ExportMetricReader
	public MetricReader metricReader() {
		return new MetricRegistryMetricReader(sharedRegistry);
	}

}
