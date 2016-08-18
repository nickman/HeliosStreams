/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.heliosapm.streams.tracing.writers;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;

import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.tracing.AbstractMetricWriter;
import com.heliosapm.streams.tracing.IMetricWriter;
import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.reflect.PrivateAccessor;

import io.netty.buffer.ByteBuf;

/**
 * <p>Title: MultiWriter</p>
 * <p>Description: A metric writer composed from multiple underlying writers</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tracing.writers.MultiWriter</code></p>
 */

public class MultiWriter extends AbstractMetricWriter {
	
	
	
	/** The config key name for the writer class names */
	public static final String CONFIG_WRITER_CLASSES = "tracing.writer.classes";
	/** The default writer class names */
	public static final String[] DEFAULT_WRITER_CLASSES = {};
	
	/** The sub writer stack */
	protected Set<IMetricWriter> subWriters;
	/** The sub writer class names */
	protected String[] writerClasses;
	
	protected String writerName = "BrokenMultiWriter"; 

	/**
	 * Creates a new MultiWriter
	 */
	protected MultiWriter() {
		super(false, true);
	}
	
	private static final Object[] EMPTY_ARGS = {};
	private static final Class<?>[] EMPTY_SIG = {};
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.AbstractMetricWriter#configure(java.util.Properties)
	 */
	@Override
	public void configure(final Properties config) {
		super.configure(config);
		final StringBuilder name = new StringBuilder(getClass().getSimpleName()).append("[");
		writerClasses = ConfigurationHelper.getArraySystemThenEnvProperty(CONFIG_WRITER_CLASSES, DEFAULT_WRITER_CLASSES, config);
		this.config.put("writerClasses", String.join(",", writerClasses));
		if(writerClasses.length==0) {
			log.warn("MultiWriter has zero subwriters ! No tracing will be performed");
			subWriters = null;
		} else {
			subWriters = new LinkedHashSet<IMetricWriter>(writerClasses.length);
		}
		for(String className: writerClasses) {
			try {
				IMetricWriter writer = (IMetricWriter)PrivateAccessor.getObjectInstance(className, EMPTY_ARGS, EMPTY_SIG);
				subWriters.add(writer);
				name.append(writer.getClass().getSimpleName()).append(",");
				log.info("Instantiated sub writer [{}]", writer.getClass().getSimpleName());
			} catch (Exception ex) {
				log.error("Failed to instantiate sub writer [{}]", className, ex);
			}
		}
		if(subWriters.isEmpty()) {
			name.append("]");
			log.warn("MultiWriter created zero subwriters ! No tracing will be performed");
		} else {
			name.deleteCharAt(name.length()-1).append("]");
			for(IMetricWriter imw : subWriters) {
				imw.configure(config);
			}
		}
		writerName = name.toString();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.AbstractMetricWriter#toString()
	 */
	@Override
	public String toString() {
		return writerName;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.AbstractMetricWriter#doMetrics(java.util.Collection)
	 */
	@Override
	protected void doMetrics(final Collection<StreamedMetric> metrics) {
		for(IMetricWriter imw : subWriters) {
			imw.onMetrics(metrics);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.AbstractMetricWriter#doMetrics(com.heliosapm.streams.metrics.StreamedMetric[])
	 */
	@Override
	protected void doMetrics(StreamedMetric... metrics) {
		for(IMetricWriter imw : subWriters) {
			imw.onMetrics(metrics);
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.AbstractMetricWriter#onMetrics(io.netty.buffer.ByteBuf)
	 */
	@Override
	public void onMetrics(final ByteBuf metrics) {		
		for(IMetricWriter imw : subWriters) {
			imw.onMetrics(metrics);
			metrics.resetReaderIndex();
		}		
	}

	/**
	 * {@inheritDoc}
	 * @see com.google.common.util.concurrent.AbstractIdleService#startUp()
	 */
	@Override
	protected void startUp() throws Exception {
		for(IMetricWriter imw : subWriters) {
			imw.startAsync();
		}
	}

	/**
	 * {@inheritDoc}
	 * @see com.google.common.util.concurrent.AbstractIdleService#shutDown()
	 */
	@Override
	protected void shutDown() throws Exception {
		for(IMetricWriter imw : subWriters) {
			imw.stopAsync();
		}
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.AbstractMetricWriter#getConfirmedMetricsSent()
	 */
	@Override
	public long getConfirmedMetricsSent() {
		long m = 0;
		for(IMetricWriter imw : subWriters) {
			m += imw.getConfirmedMetricsSent();
		}
		return m;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.AbstractMetricWriter#getMetricsPending()
	 */
	@Override
	public long getMetricsPending() {
		long m = 0;
		for(IMetricWriter imw : subWriters) {
			m += imw.getMetricsPending();
		}
		return m;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.AbstractMetricWriter#getMetricsSent()
	 */
	@Override
	public long getMetricsSent() {
		long m = 0;
		for(IMetricWriter imw : subWriters) {
			m += imw.getMetricsSent();
		}
		return m;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.AbstractMetricWriter#getSendErrors()
	 */
	@Override
	public long getSendErrors() {
		long m = 0;
		for(IMetricWriter imw : subWriters) {
			m += imw.getSendErrors();
		}
		return m;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.MetricWriterMXBean#getCustomState()
	 */
	@Override
	public String getCustomState() {
		return "No state.";
	}
	
	
	
	

}
