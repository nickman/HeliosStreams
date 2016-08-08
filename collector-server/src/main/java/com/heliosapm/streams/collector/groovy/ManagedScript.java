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
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.management.MBeanRegistration;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashSet;
import org.codehaus.groovy.reflection.ClassInfo;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.heliosapm.streams.collector.TimeoutService;
import com.heliosapm.streams.collector.cache.GlobalCacheService;
import com.heliosapm.streams.collector.execution.CollectorExecutionService;
import com.heliosapm.streams.common.metrics.SharedMetricsRegistry;
import com.heliosapm.streams.tracing.ITracer;
import com.heliosapm.streams.tracing.TracerFactory;
import com.heliosapm.utils.enums.TimeUnitSymbol;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.jmx.SharedScheduler;
import com.heliosapm.utils.ref.MBeanProxy;
import com.heliosapm.utils.ref.ReferenceService.ReferenceType;
import com.heliosapm.utils.reflect.PrivateAccessor;
import com.heliosapm.utils.tuples.NVP;

import groovy.lang.Binding;
import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovySystem;
import groovy.lang.Script;
import io.netty.util.Timeout;
import jsr166e.LongAdder;

/**
 * <p>Title: ManagedScript</p>
 * <p>Description: A groovy {@link Script} extension to provide JMX management for each script instance</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.groovy.ManagedScript</code></p>
 */

public abstract class ManagedScript extends Script implements MBeanRegistration, ManagedScriptMBean, Closeable, Callable<Void>, UncaughtExceptionHandler {

	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** This script's dedicated class loader */
	protected GroovyClassLoader gcl = null;
	/** This script's source file */
	protected ByteBufReaderSource sourceReader = null;
	/** This script's ObjectName */
	protected ObjectName objectName = null;
	/** The scheduler handle if this script is scheduled */
	protected ScheduledFuture<?> scheduleHandle = null;
	/** The scheduled execution period */
	protected Long scheduledPeriod = null;
	/** The scheduled execution period unit */
	protected TimeUnit scheduledPeriodUnit = null;
	/** The map underlying the binding */
	protected Map<String, Object> bindingMap;
	/** This script's binding */
	protected Binding binding;
	/** The fork join pool to execute collections in */
	protected final CollectorExecutionService executionService;
	/** The names of pending dependencies */
	protected final NonBlockingHashSet<String> pendingDependencies = new NonBlockingHashSet<String>();
	/** The collection runner callable */
	protected final CollectionRunnerCallable runCallable = new CollectionRunnerCallable();
	/** The dependency manager for this script */
	protected final DependencyManager<ManagedScript> dependencyManager;
	/** The source file */
	protected File sourceFile = null;
	/** The linked source file */
	protected File linkedSourceFile = null; 
	
	/** The compile time for this script */
	protected long compileTime = -1L;
	
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
	protected final AtomicLong consecutiveErrors = new AtomicLong();
	/** A counter for the total number of collection errors */
	protected final LongAdder totalErrors = new LongAdder();
	/** The timestamp of the most recent collection error */
	protected final AtomicLong lastError = new AtomicLong(-1L);
	/** The timestamp of the most recent collection at completion */
	protected final AtomicLong lastCompleteCollection = new AtomicLong(-1L);
	/** The elapsed time of the most recent collection */
	protected final AtomicLong lastCollectionElapsed = new AtomicLong(-1L);
	/** The current script state */
	protected final AtomicReference<ScriptState> state = new AtomicReference<ScriptState>(ScriptState.INIT);
	/** flag indicating if rescheduling can occur */
	protected final AtomicBoolean canReschedule = new AtomicBoolean(false);
	
	/** The deployment sequence id */
	protected int deploymentId = 0;
	
	/** Regex pattern to determine if a schedule directive is build into the source file name */
	public static final Pattern PERIOD_PATTERN = Pattern.compile(".*\\-(\\d++[s|m|h|d]){1}\\.groovy$", Pattern.CASE_INSENSITIVE);
	
	/** The UTF8 char set */
	public static final Charset UTF8 = Charset.forName("UTF8");
	
	private static final double NS2MS = 1D / (double)TimeUnit.NANOSECONDS.convert(1, TimeUnit.MILLISECONDS);

	
	/**
	 * Creates a new ManagedScript
	 */
	@SuppressWarnings("unchecked")
	public ManagedScript() {
		dependencyManager = new DependencyManager<ManagedScript>(this, (Class<ManagedScript>) this.getClass());
		executionService = CollectorExecutionService.getInstance();
	}

//	/**
//	 * Creates a new ManagedScript
//	 * @param binding The script bindings
//	 */
//	@SuppressWarnings("unchecked")
//	public ManagedScript(final Binding binding) {
//		super(binding);
//		this.binding = binding;
//		this.bindingMap = binding.getVariables();
//		dependencyManager = new DependencyManager<ManagedScript>(this, (Class<ManagedScript>) this.getClass());
//		executionService = CollectorExecutionService.getInstance();
//	}
	
	/**
	 * Initializes this script
	 * @param gcl The class loader
	 * @param sourceReader The source file
	 * @param compileTime The compile time for this script in ms.
	 */
	void initialize(final GroovyClassLoader gcl, final ByteBufReaderSource sourceReader, final String rootDirectory, final long compileTime) {
		this.gcl = gcl;
		this.compileTime = compileTime;
		this.sourceReader = sourceReader;
		sourceFile = sourceReader.getSourceFile();
		linkedSourceFile = getLinkedFile();
		final String name = sourceReader.getSourceFile().getName().replace(".groovy", "");
		final String dir = sourceReader.getSourceFile().getParent().replace(rootDirectory, "").replace("\\", "/").replace("/./", "/").replace("/collectors/", "");
		bindingMap = sourceReader.getBindingMap();
		binding = new Binding(this.bindingMap);
		bindingMap.putAll(super.getBinding().getVariables());		
		super.setBinding(this.binding);
		final Matcher m = PERIOD_PATTERN.matcher(this.sourceReader.getSourceFile().getAbsolutePath());
		if(m.matches()) {
			final String sch = m.group(1);
			final NVP<Long, TimeUnitSymbol> schedule = TimeUnitSymbol.period(sch);
			scheduledPeriod = schedule.getKey();
			scheduledPeriodUnit = schedule.getValue().unit;			
			
		} else {
			log.info("No schedule found for collector script [{}]", this.sourceReader);
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
			carryOverAndClose();
			try { JMXHelper.unregisterMBean(objectName); } catch (Exception x) {/* No Op */}
		}
//		MBeanProxy proxy = MBeanProxy.proxyMBean(ReferenceType.WEAK, ManagedScriptMBean.class, this); 
		
		try { 
			//JMXHelper.registerMBean(proxy, objectName);
			MBeanProxy.register(ReferenceType.WEAK, objectName, ManagedScriptMBean.class, this);
		} catch (Exception ex) {
			log.warn("Failed to register MBean for ManagedScript [{}]", objectName, ex);
		}
		
		// Script is ready, now schedule it if a schedule was specified
		// FIXME: need to do something with the initial delay
		if(scheduledPeriod!=null && pendingDependencies.isEmpty()) {
			canReschedule.set(true);
			scheduleHandle = SharedScheduler.getInstance().schedule(this, scheduledPeriod, scheduledPeriodUnit);			
			log.info("Collection Script scheduled");
		}
	}
	
	
	
//	/** Cache injection substitution pattern */
//	public static final Pattern CACHE_PATTERN = Pattern.compile("\\$cache\\{(.*?)(?::(\\d+))?(?::(nanoseconds|microseconds|milliseconds|seconds|minutes|hours|days))??\\}");
//	/** Injected field template */
//	public static final String INJECT_TEMPLATE = "@Dependency(value=\"%s\", timeout=%s, unit=%s) def %s;"; 
	
	
	/**
	 * Returns the source file's linked file if the file is a link, otherwise returns null
	 * @return the linked file or null
	 */
	protected File getLinkedFile() {
		try {
			final Path sourcePath = sourceFile.toPath().normalize();
			final Path linkedPath = sourcePath.toRealPath();
			return (sourcePath.equals(linkedPath)) ? null : linkedPath.toFile();
		} catch (Exception ex) {
			return null;
		}
	}
	
	/**
	 * Reads the counters from the prior instance of this class, increments this instances counters and closes the prior.
	 */
	protected void carryOverAndClose() {
		final ManagedScriptMBean oldScript = MBeanServerInvocationHandler.newProxyInstance(JMXHelper.getHeliosMBeanServer(), objectName, ManagedScriptMBean.class, false);
		deploymentId = oldScript.getDeploymentId()+1;
		totalErrors.add(oldScript.getTotalCollectionErrors());
		Date dt = oldScript.getLastCollectionErrorDate();
		if(dt!=null) {
			lastError.set(dt.getTime());
		}
		dt = oldScript.getLastCollectionDate();
		if(dt!=null) {
			lastCompleteCollection.set(dt.getTime());
		}
		final Long lastElapsed = oldScript.getLastCollectionElapsed();
		if(lastElapsed!=null) {
			lastCollectionElapsed.set(lastElapsed);
		}
		try { oldScript.close(); } catch (Exception x) {/* No Op */}
	}
	
	private class CollectionRunnerCallable implements Callable<Void> {
		final TimeoutService timeoutService = TimeoutService.getInstance();
		
		@Override
		public Void call() throws Exception {
			try {
				final Timeout txout = timeoutService.timeout(5, TimeUnit.SECONDS, new Runnable(){
					final Thread me = Thread.currentThread();
					@Override
					public void run() {
						me.interrupt();
						log.warn("Task interrupted after timeout");
					}
				});
				final long elapsed = scriptExec();
				txout.cancel();
				lastCollectionElapsed.set(elapsed);
				collectionTimer.update(elapsed, TimeUnit.MILLISECONDS);
				consecutiveErrors.set(0L);
				lastCompleteCollection.set(System.currentTimeMillis());
			} catch (Exception iex) {
				consecutiveErrors.incrementAndGet();
				totalErrors.increment();
				lastError.set(System.currentTimeMillis());
				if(iex instanceof InterruptedException) {
					log.warn("Collect Task Execution Interrupted");
				} else {
					log.error("Task Execution Failed", iex);
				}
			} finally {
				if(pendingDependencies.isEmpty()) {
					canReschedule.set(true);
					scheduleHandle = SharedScheduler.getInstance().schedule(this, scheduledPeriod, scheduledPeriodUnit);					
				} else {
					log.warn("Script scheduling waiting on dependencies {}", pendingDependencies);
				}
			}
			return null;
		}
		
		protected long scriptExec() {
			final ITracer tracer = TracerFactory.getInstance().getTracer();			
			bindingMap.put("tracer", tracer);
			try {
				final long start = System.currentTimeMillis();
				run();			
				return System.currentTimeMillis() - start;
			} finally {
				tracer.flush();
			}
		}
	}
	
	
	
	/**
	 * Adds a pending dependency
	 * @param cacheKey the cache key of the value we're waiting on
	 */
	void addPendingDependency(final String cacheKey) {
		pendingDependencies.add(cacheKey);
		canReschedule.set(false);
		if(scheduleHandle != null) {
			scheduleHandle.cancel(true);
			scheduleHandle = null;
		}

	}
	
	/**
	 * Removes a pending dependency
	 * @param cacheKey the cache key of the value that has been injected
	 */
	void removePendingDependency(final String cacheKey) {
		pendingDependencies.remove(cacheKey);
		if(pendingDependencies.isEmpty()) {
			canReschedule.set(true);
			scheduleHandle = SharedScheduler.getInstance().schedule(this, scheduledPeriod, scheduledPeriodUnit);			
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.util.concurrent.Callable#call()
	 */
	@Override
	public Void call() throws Exception {
		executionService.submit(runCallable);
		return null;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#getBindings()
	 */
	@Override
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
		log.error("Exception thrown in collector [{}]", sourceReader);
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() throws IOException {
		if(scheduleHandle!=null) {
			scheduleHandle.cancel(true);
			scheduleHandle = null;
		}
		bindingMap.clear();
		dependencyManager.close();
		if(gcl!=null) {
			final Class[] classes = gcl.getLoadedClasses();
			
			
			try { gcl.close(); } catch (Exception x) {/* No Op */}			
			gcl = null;			
			int unloaded = 0;
			//ClassInfo ci = ClassInfo.getClassInfo(this.getClass());
//			try {
//				removeClassFromGlobalClassSet(Class.forName("org.codehaus.groovy.reflection.ClassInfo", true, this.getClass().getClassLoader()));
//			} catch (Exception x) {
//				x.printStackTrace(System.err);
//			}
			final StringBuffer b = new StringBuffer("======= Unloaded Meta Classes");
			for(Class<?> clazz: classes) {
				
				GroovySystem.getMetaClassRegistry().removeMetaClass(clazz);
				b.append("\n\t").append(clazz.getName());
				unloaded++;
				try {
					final ClassInfo classInfo = ClassInfo.getClassInfo(clazz);
					final HashMap<Class,ClassInfo> map = (HashMap<Class,ClassInfo>)PrivateAccessor.invokeStatic(classInfo.getClass(), "getLocalClassInfoMap", new Object[0]);
					if(map!=null) {
						ClassInfo ci = map.remove(clazz);
						if(ci!=null) {
							log.info("Removed ClassInfo for [{}]", clazz.getName());
						}
					}
				} catch (Throwable t) {
					t.printStackTrace(System.err);
				}
			}
			log.info("Removed [{}] meta classes for GCL for [{}]\n{}", unloaded, sourceReader, b);
//			gcl.clearCache();
			if(JMXHelper.isRegistered(objectName)) {
				try { JMXHelper.unregisterMBean(objectName); } catch (Exception x) {/* No Op */}
			}
			System.gc();
		}
	}
	
	static void removeClassFromGlobalClassSet(Class<?> classInfoClass) throws Exception {
        Field globalClassValueField = classInfoClass.getDeclaredField("globalClassValue");
        globalClassValueField.setAccessible(true);
        Object globalClassValue = globalClassValueField.get(null);
        Method removeFromGlobalClassValue = globalClassValueField.getType().getDeclaredMethod("remove", Class.class);
        removeFromGlobalClassValue.setAccessible(true);

        Field globalClassSetField = classInfoClass.getDeclaredField("globalClassSet");
        globalClassSetField.setAccessible(true);
        Object globalClassSet = globalClassSetField.get(null);
        globalClassSetField = globalClassSet.getClass().getDeclaredField("items");
        globalClassSetField.setAccessible(true);
        Object globalClassSetItems = globalClassSetField.get(globalClassSet);

        Field clazzField = classInfoClass.getDeclaredField("klazz");
        clazzField.setAccessible(true);


        Iterator it = (Iterator) globalClassSetItems.getClass().getDeclaredMethod("iterator").invoke(globalClassSetItems);

        while (it.hasNext()) {
            it.next();
            Object clazz = clazzField.get("ClassInfo");
            removeFromGlobalClassValue.invoke(globalClassValue, clazz);
        }

    }
	

/*	
-XX:+UnlockDiagnosticVMOptions 
-XX:+UnlockExperimentalVMOptions 
-XX:+CMSClassUnloadingEnabled 
-XX:+ExplicitGCInvokesConcurrentAndUnloadsClasses 
-XX:+TraceClassUnloading  
-XX:+UseConcMarkSweepGC 
-XX:+ExplicitGCInvokesConcurrent	
-XX:SoftRefLRUPolicyMSPerMB=0
*/		
	
	
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
	
	private static double tms(final double val) {
		return TimeUnit.NANOSECONDS.toMillis((long)val);
	}
	

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#getMedianCollectTime()
	 */
	@Override
	public double getMedianCollectTime() {
		return tms(timerSnap.getValue().getMedian());
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#get75PctCollectTime()
	 */
	@Override
	public double get75PctCollectTime() {
		return tms(timerSnap.getValue().get75thPercentile());
	}
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#get95PctCollectTime()
	 */
	@Override
	public double get95PctCollectTime() {
		return tms(timerSnap.getValue().get95thPercentile());
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#get98PctCollectTime()
	 */
	@Override
	public double get98PctCollectTime() {
		return tms(timerSnap.getValue().get98thPercentile());
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#get99PctCollectTime()
	 */
	@Override
	public double get99PctCollectTime() {
		return tms(timerSnap.getValue().get99thPercentile());
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#get999PctCollectTime()
	 */
	@Override
	public double get999PctCollectTime() {
		return tms(timerSnap.getValue().get999thPercentile());
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#getMaxCollectTime()
	 */
	@Override
	public long getMaxCollectTime() {
		return TimeUnit.NANOSECONDS.toMillis(timerSnap.getValue().getMax());
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#getMeanCollectTime()
	 */
	@Override
	public double getMeanCollectTime() {
		return tms(timerSnap.getValue().getMean());
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#getMinCollectTime()
	 */
	@Override
	public long getMinCollectTime() {
		return TimeUnit.NANOSECONDS.toMillis(timerSnap.getValue().getMin());
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
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#getLastCollectionErrorDate()
	 */
	@Override
	public Date getLastCollectionErrorDate() {
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
		return consecutiveErrors.get();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#getTotalCollectionErrors()
	 */
	@Override
	public long getTotalCollectionErrors() {
		return totalErrors.longValue();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#getDeploymentId()
	 */
	@Override
	public int getDeploymentId() {
		return deploymentId;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#getPendingDependencies()
	 */
	@Override
	public Set<String> getPendingDependencies() {
		return new HashSet<String>(pendingDependencies); 
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#getLastCollectionDate()
	 */
	@Override
	public Date getLastCollectionDate() {
		final long t = lastCompleteCollection.get();
		if(t==-1L) return null;
		return new Date(t);
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#getLastCollectionElapsed()
	 */
	@Override
	public Long getLastCollectionElapsed() {
		final long t = lastCollectionElapsed.get();
		if(t==-1L) return null;
		return t;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#getState()
	 */
	@Override
	public String getState() {
		return state.get().name();
	}
	
	
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#printFieldValues()
	 */
	@Override
	public Map<String, String> printFieldValues() {
		final Field[] fields = getClass().getDeclaredFields();
		final Map<String, String> map = new HashMap<String, String>(fields.length);
		for(Field f: fields) {
			final String name = f.getName();
			final boolean stat = Modifier.isStatic(f.getModifiers());
			String val = null;
			try {
				Object o = stat ? 
						PrivateAccessor.getStaticFieldValue(getClass(), name) :
						PrivateAccessor.getFieldValue(f, this);
				val = o==null ? "<null>" : o.toString();
			} catch (Exception ex) {
				val = ex.toString();
			}
			map.put(name, val);
		}
		return map;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#getScheduledPeriod()
	 */
	@Override
	public Long getScheduledPeriod() {
		return scheduledPeriod;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#getScheduledPeriodUnit()
	 */
	@Override
	public String getScheduledPeriodUnit() {
		return scheduledPeriodUnit==null ? null : scheduledPeriodUnit.name();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#getSourceFile()
	 */
	@Override
	public File getSourceFile() {
		return sourceFile;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#getLinkedSourceFile()
	 */
	@Override
	public File getLinkedSourceFile() {
		return linkedSourceFile;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#printOriginalSource()
	 */
	@Override
	public String printOriginalSource() {
		return sourceReader.getOriginalSource();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#printPrejectedSource()
	 */
	@Override
	public String printPrejectedSource() {
		return sourceReader.getPrejectedSource();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#isPrejected()
	 */
	@Override
	public boolean isPrejected() {		
		return sourceReader.isPrejected();
	}


	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#getCompileTime()
	 */
	@Override
	public long getCompileTime() {
		return compileTime;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#isScheduleActive()
	 */
	@Override
	public boolean isScheduleActive() {
		return scheduleHandle != null;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#getTimeUntilNextCollect()
	 */
	@Override
	public Long getTimeUntilNextCollect() {
		return scheduleHandle != null ? scheduleHandle.getDelay(TimeUnit.SECONDS) : null; 
	}
	
}
