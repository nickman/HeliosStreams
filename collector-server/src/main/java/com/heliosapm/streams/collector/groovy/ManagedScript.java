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
package com.heliosapm.streams.collector.groovy;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.heliosapm.streams.collector.cache.GlobalCacheService;
import com.heliosapm.streams.collector.execution.CollectorExecutionService;
import com.heliosapm.streams.common.metrics.SharedMetricsRegistry;
import com.heliosapm.utils.enums.TimeUnitSymbol;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.jmx.SharedScheduler;
import com.heliosapm.utils.tuples.NVP;

import groovy.lang.Binding;
import groovy.lang.GroovyClassLoader;
import groovy.lang.Script;
import jsr166e.LongAdder;
import jsr166y.ForkJoinTask;

/**
 * <p>Title: ManagedScript</p>
 * <p>Description: A groovy {@link Script} extension to provide JMX management for each script instance</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.groovy.ManagedScript</code></p>
 */

public abstract class ManagedScript extends Script implements ManagedScriptMBean, Closeable, Callable<Void>, UncaughtExceptionHandler {

	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** This script's dedicated class loader */
	protected GroovyClassLoader gcl = null;
	/** This script's source file */
	protected File sourceFile = null;
	/** This script's ObjectName */
	protected ObjectName objectName = null;
	/** The scheduler handle if this script is scheduled */
	protected ScheduledFuture<?> scheduleHandle = null;
	/** The scheduled execution period */
	protected Long scheduledPeriod = null;
	/** The scheduled execution period unit */
	protected TimeUnit scheduledPeriodUnit = null;
	/** The map underlying the binding */
	protected final Map<String, Object> bindingMap = new HashMap<String, Object>();
	/** This script's binding */
	protected final Binding binding = new Binding(bindingMap);
	/** The fork join pool to execute collections in */
	protected final CollectorExecutionService executionService;
	
	/** A timer to measure collection times */
	protected Timer collectionTimer = null;
	/** A cached gauge for the collection timer's snapshot */
	protected final CachedGauge<Snapshot> timerSnap = new CachedGauge<Snapshot>(5, TimeUnit.SECONDS) {
		@Override
		protected Snapshot loadValue() {			
			return collectionTimer.getSnapshot();
		}
	};
	/** A counter for the number of consecutive collection errors */ 
	protected final LongAdder consecutiveErrors = new LongAdder();
	/** A counter for the total number of collection errors */
	protected final LongAdder totalErrors = new LongAdder();
	/** The timestamp of the most recent collection error */
	protected final AtomicLong lastError = new AtomicLong(-1L);
	
	/** Regex pattern to determine if a schedule directive is build into the source file name */
	public static final Pattern PERIOD_PATTERN = Pattern.compile(".*\\-(\\d++[s|m|h|d]){1}\\.groovy$", Pattern.CASE_INSENSITIVE);
	
	/**
	 * Creates a new ManagedScript
	 */
	public ManagedScript() {
		setBinding(binding);
		executionService = CollectorExecutionService.getInstance();
	}

	/**
	 * Creates a new ManagedScript
	 * @param binding The script bindings
	 */
	public ManagedScript(final Binding binding) {
		super(binding);
		setBinding(this.binding);
		executionService = CollectorExecutionService.getInstance();
	}
	
	/**
	 * Initializes this script
	 * @param gcl The class loader
	 * @param sourceFile The source file
	 */
	void initialize(final GroovyClassLoader gcl, final File sourceFile, final String rootDirectory) {
		this.gcl = gcl;
		this.sourceFile = sourceFile;
		final String name = sourceFile.getName().replace(".groovy", "");
		final String dir = sourceFile.getParent().replace(rootDirectory, "").replace("\\", "/").replace("/./", "/").replace("/collectors/", "");
		final Matcher m = PERIOD_PATTERN.matcher(this.sourceFile.getAbsolutePath());
		if(m.matches()) {
			final String sch = m.group(1);
			final NVP<Long, TimeUnitSymbol> schedule = TimeUnitSymbol.period(sch);
			scheduledPeriod = schedule.getKey();
			scheduledPeriodUnit = schedule.getValue().unit;
			scheduleHandle = SharedScheduler.getInstance().scheduleWithFixedDelay(this, scheduledPeriod, scheduledPeriod, scheduledPeriodUnit, this);
			
		} else {
			log.info("No schedule found for collector script [{}]", this.sourceFile);
		}
		objectName = JMXHelper.objectName(new StringBuilder()
				.append("com.heliosapm.streams.collector.scripts:dir=")
				.append(dir)
				.append(",name=")
				.append(name)
		);
		collectionTimer = SharedMetricsRegistry.getInstance().timer("collection.dir=" + dir + ".name=" + name);		
		bindingMap.put("globalCache", GlobalCacheService.getInstance());
		bindingMap.put("log", LogManager.getLogger("collectors." + dir.replace('/', '.') + "." + name));
		if(JMXHelper.isRegistered(objectName)) {
			try { JMXHelper.unregisterMBean(objectName); } catch (Exception x) {/* No Op */}
		}
		try { JMXHelper.registerMBean(this, objectName); } catch (Exception ex) {
			log.warn("Failed to register MBean for ManagedScript [{}]", objectName, ex);
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.util.concurrent.Callable#call()
	 */
	@Override
	public Void call() throws Exception {
		try {
			final Context ctx = collectionTimer.time();
			final ForkJoinTask<Void> task = executionService.submit(new Callable<Void>(){
				@Override
				public Void call() throws Exception {		
					run();
					return null;
				}
			});
			try {
				task.get(5, TimeUnit.SECONDS);
				final long elapsed = ctx.stop();
				log.info("Elapsed: [{}]", TimeUnit.NANOSECONDS.toMillis(elapsed));				
			} catch (Exception ex) {
				log.error("Task Execution Failed", ex);
				try { task.cancel(true); } catch (Exception x) {/* No Op */}
			}
			consecutiveErrors.reset();
		} catch (Exception ex) {
			consecutiveErrors.increment();
			totalErrors.increment();
			lastError.set(System.currentTimeMillis());
			log.warn("Failed collection on [{}]", sourceFile, ex);
		}
		
		return null;
	}
	
	public Map<String, String> getBindings() {
		final Map<String, String> bind = new HashMap<String, String>(bindingMap.size());
		for(Map.Entry<String, Object> entry: bindingMap.entrySet()) {
			final String value = entry.getValue()==null ? null : entry.getValue().toString();
			bind.put(entry.getKey(), value);
		}
		return bind;
	}
	
	/**
	 * <p>Handles exceptions thrown during the collection operation</p>
	 * {@inheritDoc}
	 * @see java.lang.Thread.UncaughtExceptionHandler#uncaughtException(java.lang.Thread, java.lang.Throwable)
	 */
	@Override
	public void uncaughtException(final Thread t, final Throwable e) {
		log.error("Exception thrown in collector [{}]", sourceFile);
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() throws IOException {
		if(scheduleHandle!=null) {
			scheduleHandle.cancel(true);
		}
		bindingMap.clear();
		if(gcl!=null) {
			try { gcl.close(); } catch (Exception x) {/* No Op */}
			gcl = null;
			if(JMXHelper.isRegistered(objectName)) {
				try { JMXHelper.unregisterMBean(objectName); } catch (Exception x) {/* No Op */}
			}
		}
	}
	
	
	
	/**
	 * {@inheritDoc}
	 * @see javax.management.MBeanRegistration#preRegister(javax.management.MBeanServer, javax.management.ObjectName)
	 */
	@Override
	public ObjectName preRegister(final MBeanServer server, final ObjectName name) throws Exception {
		return name;
	}

	/**
	 * {@inheritDoc}
	 * @see javax.management.MBeanRegistration#postRegister(java.lang.Boolean)
	 */
	@Override
	public void postRegister(final Boolean registrationDone) {
		/* No Op */		
	}

	/**
	 * {@inheritDoc}
	 * @see javax.management.MBeanRegistration#preDeregister()
	 */
	@Override
	public void preDeregister() throws Exception {
		/* No Op */		
	}

	/**
	 * {@inheritDoc}
	 * @see javax.management.MBeanRegistration#postDeregister()
	 */
	@Override
	public void postDeregister() {
		try { close(); } catch (Exception x) {/* No Op */}		
	}
	

	/**
	 * @return
	 * @see com.codahale.metrics.Snapshot#getMedian()
	 */
	@Override
	public double getMedianCollectTime() {
		return timerSnap.getValue().getMedian();
	}

	/**
	 * @return
	 * @see com.codahale.metrics.Snapshot#get75thPercentile()
	 */
	@Override
	public double get75PctCollectTime() {
		return timerSnap.getValue().get75thPercentile();
	}

	/**
	 * @return
	 * @see com.codahale.metrics.Snapshot#get95thPercentile()
	 */
	@Override
	public double get95PctCollectTime() {
		return timerSnap.getValue().get95thPercentile();
	}

	/**
	 * @return
	 * @see com.codahale.metrics.Snapshot#get98thPercentile()
	 */
	@Override
	public double get98PctCollectTime() {
		return timerSnap.getValue().get98thPercentile();
	}

	/**
	 * @return
	 * @see com.codahale.metrics.Snapshot#get99thPercentile()
	 */
	@Override
	public double get99PctCollectTime() {
		return timerSnap.getValue().get99thPercentile();
	}

	/**
	 * @return
	 * @see com.codahale.metrics.Snapshot#get999thPercentile()
	 */
	@Override
	public double get999PctCollectTime() {
		return timerSnap.getValue().get999thPercentile();
	}

	/**
	 * @return
	 * @see com.codahale.metrics.Snapshot#getMax()
	 */
	@Override
	public long getMaxCollectTime() {
		return timerSnap.getValue().getMax();
	}

	/**
	 * @return
	 * @see com.codahale.metrics.Snapshot#getMean()
	 */
	@Override
	public double getMeanCollectTime() {
		return timerSnap.getValue().getMean();
	}

	/**
	 * @return
	 * @see com.codahale.metrics.Snapshot#getMin()
	 */
	@Override
	public long getMinCollectTime() {
		return timerSnap.getValue().getMin();
	}
	

	/**
	 * Returns the total number of completed collections
	 * @return the total number of completed collections
	 */
	@Override
	public long getCollectionCount() {
		return collectionTimer.getCount();
	}

	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#getLastCollectionErrorTime()
	 */
	@Override
	public Date getLastCollectionErrorTime() {
		final long t = lastError.get();
		if(t==-1L) return null;
		return new Date(t);
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#getConsecutiveCollectionErrors()
	 */
	@Override
	public long getConsecutiveCollectionErrors() {
		return consecutiveErrors.longValue();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#getTotalCollectionErrors()
	 */
	@Override
	public long getTotalCollectionErrors() {
		return totalErrors.longValue();
	}
}
