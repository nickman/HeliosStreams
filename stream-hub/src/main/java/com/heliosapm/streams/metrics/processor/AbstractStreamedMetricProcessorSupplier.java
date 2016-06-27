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
package com.heliosapm.streams.metrics.processor;

import org.apache.kafka.streams.processor.Processor;

import com.heliosapm.streams.metrics.StreamedMetric;

/**
 * <p>Title: AbstractStreamedMetricProcessorSupplier</p>
 * <p>Description: The base class for concrete StreamedMetricProcessorSupplier instances. This is basically a holder
 * for all the values required to create instances of the the corresponding StreamedMetricProcessor.</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.processor.AbstractStreamedMetricProcessorSupplier</code></p>
 */

public class AbstractStreamedMetricProcessorSupplier implements StreamedMetricProcessorSupplier {
	protected String[] sources = null;
	protected String sink = null;
	
	
	/**
	 * Creates a new AbstractStreamedMetricProcessorSupplier
	 */
	public AbstractStreamedMetricProcessorSupplier() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.kafka.streams.processor.ProcessorSupplier#get()
	 */
	@Override
	public Processor<String, StreamedMetric> get() {
		// TODO Auto-generated method stub
		return null;
	}

}
