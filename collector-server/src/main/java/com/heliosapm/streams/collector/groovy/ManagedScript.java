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
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Collections;
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

import javax.management.AttributeChangeNotification;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanRegistration;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationEmitter;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.cliffc.high_scale_lib.NonBlockingHashSet;
import org.codehaus.groovy.control.CompilationFailedException;
import org.codehaus.groovy.reflection.ClassInfo;
import org.codehaus.groovy.runtime.NullObject;
//import org.springframework.beans.BeansException;
//import org.springframework.context.ApplicationContext;
//import org.springframework.context.ApplicationContextAware;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.heliosapm.streams.collector.CollectorServer;
import com.heliosapm.streams.collector.cache.GlobalCacheService;
import com.heliosapm.streams.collector.execution.CollectorExecutionService;
import com.heliosapm.streams.collector.timeout.TimeoutService;
import com.heliosapm.streams.common.metrics.SharedMetricsRegistry;
import com.heliosapm.streams.hystrix.HystrixCommandFactory;
import com.heliosapm.streams.hystrix.HystrixCommandProvider;
import com.heliosapm.streams.tracing.ITracer;
import com.heliosapm.streams.tracing.TracerFactory;
import com.heliosapm.streams.tracing.deltas.DeltaManager;
import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.enums.TimeUnitSymbol;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.jmx.SharedNotificationExecutor;
import com.heliosapm.utils.jmx.SharedScheduler;
import com.heliosapm.utils.lang.StringHelper;
import com.heliosapm.utils.ref.ReferenceService;
import com.heliosapm.utils.reflect.PrivateAccessor;
import com.heliosapm.utils.tuples.NVP;

import groovy.lang.Binding;
import groovy.lang.Closure;
import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyShell;
import groovy.lang.GroovySystem;
import groovy.lang.MissingPropertyException;
import groovy.lang.Script;
import io.netty.util.Timeout;
import jsr166e.LongAdder;

/**
 * <p>Title: ManagedScript</p>
 * <p>Description: A groovy {@link Script} extension to provide JMX management for each script instance</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.groovy.ManagedScript</code></p>
 * FIXME:
 * 	Race condition on dependency management when datasource is redeployed. 
 */

public abstract class ManagedScript extends Script implements NotificationEmitter, MBeanRegistration, ManagedScriptMBean, Closeable, Callable<Void>, UncaughtExceptionHandler { //, ApplicationContextAware {
	/** The JMX notification handler */
	protected final NotificationBroadcasterSupport broadcaster;
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
	protected final DependencyManager<? extends ManagedScript> dependencyManager;
	/** The number of traces issued in the last execution */
	protected final AtomicLong lastTraceCount = new AtomicLong(0L);
	

	/** The number of metrics flushed in the last execution */
	protected final AtomicLong lastFlushCount = new AtomicLong(0L);
	
	/** The source file */
	protected File sourceFile = null;
	/** The linked source file */
	protected File linkedSourceFile = null; 
	
//	/** The spring app context if we're running in spring boot */
//	protected ApplicationContext appCtx = null;
	/** The Spring exported interface of this instance */
	protected ManagedScriptMBean springInstance = null;
	
	
	/** The jmx notification serial factory */
	protected final AtomicLong notifSerial = new AtomicLong(0L);
	
	/** The compile time for this script */
	protected long compileTime = -1L;
	
	/** A reference to the global cache service */
	protected final GlobalCacheService cache;
	/** The global cache keys put by this script */
	protected final NonBlockingHashSet<String> globalCacheKeys = new NonBlockingHashSet<String>();
	/** The delta keys put by this script */
	protected final NonBlockingHashSet<String> deltaKeys = new NonBlockingHashSet<String>();
	
	/** The cache key prefix */
	protected final String cacheKeyPrefix = getClass().getName();
	/** The delta service */
	protected final DeltaManager deltaManager = DeltaManager.getInstance();
	
	protected final boolean springMode;
	
	/** The ThreadMXBean */
	protected static final ThreadMXBean tmx = ManagementFactory.getThreadMXBean();
	
	
	/** A timer to measure collection times */
	protected Timer collectionTimer = null;
	/** A cached gauge for the collection timer's snapshot */
	protected final CachedGauge<Snapshot> timerSnap = new CachedGauge<Snapshot>(5, TimeUnit.SECONDS) {
		@Override
		protected Snapshot loadValue() {			
			return collectionTimer.getSnapshot();
		}
	};
	/** Indicates if hystrix circuit breakers should be used for jmx clients */
	protected final AtomicBoolean hystrixEnabled = new AtomicBoolean(false);
	
	/** The config key for jmx-clients hystrix circuit breaker commands */
	public static final String CONFIG_HYSTRIX = "component.managedscript.hystrix";
	/** The config key for hystrix circuit breaker enablement */
	public static final String CONFIG_HYSTRIX_ENABLED = CONFIG_HYSTRIX + ".enabled";
	/** The default hystrix circuit breaker enablement */
	public static final boolean DEFAULT_HYSTRIX_ENABLED = false;
	
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
	/** The number of times that the init-check has failed. */
	protected final AtomicLong initCheckFails = new AtomicLong(0L);
	/** The number of times that the pre-exec-check has failed. */
	protected final AtomicLong preExecCheckFails = new AtomicLong(0L);
	
	
	
	/** The current script state */
	protected final AtomicReference<ScriptState> state = new AtomicReference<ScriptState>(ScriptState.INIT);
	/** flag indicating if rescheduling can occur */
	protected final AtomicBoolean canReschedule = new AtomicBoolean(false);
	/** The hystrix command factory to use if hystrix is enabled */
	protected HystrixCommandProvider<Object> commandBuilder = null;
	
	/** The deployment sequence id */
	protected int deploymentId = 0;
	
	
	/** Regex pattern to determine if a schedule directive is build into the source file name */
	public static final Pattern PERIOD_PATTERN = Pattern.compile(".*\\-(\\d++[s|m|h|d]){1}\\.groovy$", Pattern.CASE_INSENSITIVE);
	/** The pattern identifying a property value that should be resolved post-compilation */
	public static final Pattern POST_COMPILE = Pattern.compile("\\$post\\{(.*?)\\}$");
	
	/** The UTF8 char set */
	public static final Charset UTF8 = Charset.forName("UTF8");
	
	/** A null object to put in bindings to avoid getting an NPE */
	public static final NullObject NULL_OBJECT = NullObject.getNullObject();
	
	/** The jmx notification infos */
	private static final MBeanNotificationInfo[] NOTIF_INFOS;
	
	
	/** Serial number to create unique wrapped closeable binding names */
	protected static final AtomicLong wrappedCloseableSerial = new AtomicLong(0L);
	
	/** A map of wek-ref referenced wrapped closeables keyed by the generated key */
	protected final Map<String, WeakReference<Closeable>> wrappedCloseables = new NonBlockingHashMap<String, WeakReference<Closeable>>();
	
	static {
		final MBeanNotificationInfo[] STATE_INFOS = ScriptState.NOTIF_INFOS.values().toArray(new MBeanNotificationInfo[ScriptState.values().length]);
		
		final Set<MBeanNotificationInfo> infoSet = new HashSet<MBeanNotificationInfo>();
		Collections.addAll(infoSet, STATE_INFOS);
		NOTIF_INFOS = infoSet.toArray(new MBeanNotificationInfo[infoSet.size()]);
	}
	
	/** A map of the declared fields of this class keyed by the field name */
	protected final NonBlockingHashMap<String, Field> fields = new NonBlockingHashMap<String, Field>();
	
	private static final ThreadLocal<Map<String, Object>> ctorBindings = new ThreadLocal<Map<String, Object>>(); 
	
	static ManagedScript instantiate(final Class<ManagedScript> clazz, final Map<String, Object> initialBindings) throws Exception {
		ctorBindings.set(initialBindings);
		if(initialBindings!=null) System.out.println(":::  Initial Bindings: " + initialBindings.keySet());
		try {
			return clazz.newInstance();
		} finally {
			ctorBindings.remove();
		}
	}

	/**
	 * Creates a new ManagedScript
	 */
	public ManagedScript() {
		this(ctorBindings.get());
	}

	
	/**
	 * Creates a new ManagedScript
	 * @param initialBindings Initial bindings required for instantiation
	 */
	@SuppressWarnings("unchecked")
	public ManagedScript(final Map<String, Object> initialBindings) {
		super(new Binding(initialBindings==null ? new HashMap<String, Object>() : initialBindings));
		springMode = CollectorServer.isSpringMode();
		cache = GlobalCacheService.getInstance();
		dependencyManager = new DependencyManager<ManagedScript>(this, (Class<ManagedScript>) this.getClass());
		executionService = CollectorExecutionService.getInstance();
		broadcaster = new NotificationBroadcasterSupport(SharedNotificationExecutor.getInstance(), NOTIF_INFOS);
		for(Field f: getClass().getDeclaredFields()) {
			fields.put(f.getName(), f);
//			final groovy.transform.Field fieldAnn = f.getAnnotation(groovy.transform.Field.class);
			final Dependency fieldAnn = f.getAnnotation(Dependency.class);
			if(fieldAnn!=null) {
				f.setAccessible(true);
			}
		}
		packageSegs = StringHelper.splitString(getClass().getPackage().getName(), '.');
		packageElems = packageSegs.length;
		packageKey = getClass().getPackage().getName();	
		regionKey = packageKey.substring(packageKey.indexOf('.')+1);
		classKey = getClass().getName();		
		hystrixEnabled.set(ConfigurationHelper.getBooleanSystemThenEnvProperty(CONFIG_HYSTRIX_ENABLED, DEFAULT_HYSTRIX_ENABLED));
		if(hystrixEnabled.get()) {
			commandBuilder = HystrixCommandFactory.getInstance().builder(CONFIG_HYSTRIX, packageSegs[0] + packageSegs[1])
				.andCommandKey(classKey)
				.andThreadPoolKey(regionKey.replace('.', '-'))
				.build();
		}
	}
	
	private final int packageElems;
	private final String[] packageSegs;
	private final String packageKey;	
	private final String regionKey;
	private final String classKey;	

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
	 * @param baseBindings A map of objects to add to the bindings for this script
	 * @param sourceReader The source file
	 * @param compileTime The compile time for this script in ms.
	 */
	void initialize(final GroovyClassLoader gcl, final Map<String, Object> baseBindings, final ByteBufReaderSource sourceReader, final String rootDirectory, final long compileTime) {		
		final Map<String, Object> postCompileBindingMap = new HashMap<String, Object>(getBinding().getVariables());
		this.gcl = gcl;
		this.compileTime = compileTime;
		this.sourceReader = sourceReader;
		sourceFile = sourceReader.getSourceFile();
		linkedSourceFile = getLinkedFile();
		final String name = sourceReader.getSourceFile().getName().replace(".groovy", "");
		final String dir = sourceReader.getSourceFile().getParent().replace(rootDirectory, "").replace("\\", "/").replace("/./", "/").replace("/collectors/", "");
		bindingMap = sourceReader.getBindingMap();
		binding = new Binding(this.bindingMap) {
        	@Override
        	public Object getProperty(final String name) {
        		if(bindingMap.containsKey(name)) return bindingMap.get(name);
        		final Field f = fields.get(name);
        		if(f!=null) {
        			//return PrivateAccessor.getFieldValue(f, ManagedScript.this);
        			if(!f.isAccessible()) f.setAccessible(true);  // FIXME: get this outa here.
        			try {
        				return f.get(ManagedScript.this);
        			} catch (Exception ex) {
        				throw new RuntimeException("Failed to read field [" + f.getName() + "]", ex);
        			}
        		}
        		throw new MissingPropertyException("No such property: [" + name + "]");
        	}
        	
        	@Override
        	public Object getVariable(final String name) {
        		return getProperty(name);
        	}
        	
        	@Override
        	public void setProperty(final String property, final Object newValue) {
        		super.setProperty(property, newValue==null ? NULL_OBJECT : newValue);
        	}
        	
        	@Override
        	public void setVariable(final String name, final Object newValue) {
        		super.setVariable(name, newValue==null ? NULL_OBJECT : newValue);
        	}
			
		};
		bindingMap.putAll(super.getBinding().getVariables());		
		bindingMap.putAll(baseBindings);
		final ITracer tracer = TracerFactory.getInstance().getNewTracer();			
		bindingMap.put("tracer", tracer);

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
		
//		bindingMap.put("globalCache", GlobalCacheService.getInstance());
		bindingMap.put("log", LogManager.getLogger("collectors." + dir.replace('/', '.') + "." + name));
		
		if(JMXHelper.isRegistered(objectName)) {
			carryOverAndClose();
			try { JMXHelper.unregisterMBean(objectName); } catch (Exception x) {/* No Op */}
		}
//		MBeanProxy proxy = MBeanProxy.proxyMBean(ReferenceType.WEAK, ManagedScriptMBean.class, this); 
		
		try { 
			//JMXHelper.registerMBean(proxy, objectName);
			final ClassLoader cl = Thread.currentThread().getContextClassLoader();
			try {
				Thread.currentThread().setContextClassLoader(this.gcl);
				//MBeanProxy.register(ReferenceType.WEAK, objectName, ManagedScriptMBean.class, this);
//				MBeanProxyBuilder.register(ReferenceType.WEAK, objectName, ManagedScriptMBean.class, this);
				JMXHelper.registerMBean(objectName, this);
			} finally {
				Thread.currentThread().setContextClassLoader(cl);
			}
		} catch (Exception ex) {
			log.warn("Failed to register MBean for ManagedScript [{}]", objectName, ex);
		}		
		this.executionService.execute(scheduledReInitCheck());
	}

	
	/**
	 * Executed as soon as the init-check confirms readiness
	 */
	protected void onInitCheckComplete() {
		canReschedule.set(true);
		setState(ScriptState.SCHEDULED);
		scheduleHandle = SharedScheduler.getInstance().schedule((Callable<Void>)this, scheduledPeriod, scheduledPeriodUnit);			
		log.info("Collection Script scheduled");
	}
	
	/**
	 * Creates a runnable that is executed once a re-init-check succeeds.
	 * @return a runnable
	 */
	protected Runnable scheduledReInitCheck() {
		return new Runnable(){
			public void run() {
				boolean initCheck = false;
				try { initCheck = initCheck(); } catch (Exception ex) { initCheck = false; }
				if(initCheck) {
					initCheckFails.set(0L);
					if(!pendingDependencies.isEmpty()) {
						setState(ScriptState.WAITING);
						log.info("\n ================================ \n Script [{}} not scheduled. \nWaiting on {}", sourceFile, pendingDependencies);
					} else {
						//  SCRIPT IS READY !
						onInitCheckComplete();
					}					
				} else {
					initCheckFails.incrementAndGet();
					if(scheduledPeriod!=null) {
						setState(ScriptState.NOINIT);
						onInitCheckIncomplete();
					} else {
						setState(ScriptState.PASSIVE);
					}
				}
			}
		};
	}
	
	/**
	 * Called when init checks fail.
	 * This starts a loop where the init-check is re-executed
	 * on the same period as the script execution. As soon as
	 * the init-check confirms readiness, the script execution
	 * will be scheduled. If the script is not a scheduled script,
	 * then this will not happen and the script goes passive.
	 */
	protected void onInitCheckIncomplete() {
		SharedScheduler.getInstance().schedule(new Runnable(){
			public void run() {
				scheduledReInitCheck();
			}
		}, scheduledPeriod, scheduledPeriodUnit);
	}
	
	/**
	 * Sets the state of this script.
	 * If the state has changed, an attribute change notification will be emitted.
	 * @param stateToSet The state to set the script to
	 * @return true if state was changed as directed, false otherwise
	 */
	protected boolean setState(final ScriptState stateToSet) {
		return setState(stateToSet, null);
	}
	
	
	/**
	 * Sets the state of this script.
	 * If the state has changed, an attribute change notification will be emitted.
	 * @param stateToSet The state to set the script to
	 * @param ifState Optional conditional that the script must be in in order to change states 
	 * @return true if state was changed as directed, false otherwise
	 */
	protected boolean setState(final ScriptState stateToSet, final ScriptState ifState) {
		if(stateToSet==null) throw new IllegalArgumentException("The passed ScriptState to set was null");
		if(state.get()==stateToSet) return true;
		for(;;) {			
			if(ifState!=null && state.get()!=ifState) return false;
			final ScriptState current = state.get();
			if(current.canTransitionTo(stateToSet)) {
				if(state.compareAndSet(current, stateToSet)) {
					final AttributeChangeNotification acn = new AttributeChangeNotification(objectName, notifSerial.incrementAndGet(), System.currentTimeMillis(), "State change to: [" + stateToSet + "] from [" + current + "]", "State", String.class.getName(), current.name(), stateToSet.name());
					broadcaster.sendNotification(acn);					
					return true;
				}
			} else {
				return false;
			}
		}
	}	
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#pause()
	 */
	@Override
	public void pause() {
		if(state.get()!=ScriptState.PAUSED && scheduledPeriod!=null && scheduleHandle!=null) {
			canReschedule.set(false);
			scheduleHandle.cancel(true);
			scheduleHandle = null;
			setState(ScriptState.PAUSED);
			log.info("Collection Script paused");			
		}
	}
	
	/**
	 * Executed when the script first completes initialization
	 * @return true if the script is ready to go, false otherwise
	 */
	protected boolean initCheck() {
		log.info("\n\t  ############# No initCheck re-implemented for [{}]", getClass().getName());
		return true;
	}
	
	/**
	 * Executed before script execution, suppressing execution if it returns false or throws an exception
	 * @return true if the script should be executed, false otherwise
	 */
	protected boolean preExecCheck() {		
		return true;
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#resume()
	 */
	@Override
	public void resume() {
		if(state.get()==ScriptState.PAUSED && scheduledPeriod!=null && scheduleHandle==null) {
			canReschedule.set(true);
			scheduleHandle = SharedScheduler.getInstance().schedule((Callable<Void>)this, scheduledPeriod, scheduledPeriodUnit);
			setState(ScriptState.SCHEDULED);
			log.info("Collection Script Resumed");			
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
			if(state.get()==ScriptState.EXECUTING) {
				log.info("Script version [{}] is already executing. Skipping execution", deploymentId);
				return null;
			}
			boolean execute = false;
			try { 
				execute = preExecCheck();
				preExecCheckFails.set(0L);
			} catch (Exception ex) {
				execute = false;
			}
			if(!execute) {
				preExecCheckFails.incrementAndGet();
				return null;
			}
			if(!setState(ScriptState.EXECUTING, ScriptState.SCHEDULED)) {
				if(state.get()!=ScriptState.SCHEDULED) {
					log.info("Script version [{}] is [{}] not SCHEDULED. Stopping schedule.", deploymentId, state.get());
					if(scheduleHandle!=null) {
						if(!scheduleHandle.isCancelled()) {
							scheduleHandle.cancel(true);
						}
					}
					return null;
				}				
			}
			try {
				final long timeout = JMXHelper.isDebugAgentLoaded() ? 10000 : scheduledPeriod;
				final Thread me = Thread.currentThread();
				final Timeout txout = timeoutService.timeout(timeout, TimeUnit.SECONDS, new Runnable(){					
					@Override
					public void run() {					
						final String stackTrace = StringHelper.formatStackTrace(me, true);
						me.interrupt();						
						log.warn("Script execution interrupted after timeout [{}]. Stack of interrupted task:\n{}", timeout, stackTrace);
					}
				});
				long elapsed = -1L;
				try {
					elapsed = scriptExec();
				} finally {
					final boolean cancelled = txout.cancel();
//					if(cancelled && txout.isCancelled()) {
//						// TODO: ??
//					}
					if(Thread.interrupted()) {
						// TODO ??
						Thread.interrupted();
					}
				}
				if(elapsed!=-1L) {
					lastCollectionElapsed.set(elapsed);
					collectionTimer.update(elapsed, TimeUnit.MILLISECONDS);
					consecutiveErrors.set(0L);
					lastCompleteCollection.set(System.currentTimeMillis());
				}
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
				if(setState(ScriptState.SCHEDULED, ScriptState.EXECUTING)) {
					if(pendingDependencies.isEmpty()) {
						canReschedule.set(true);
						updateProps();  // FIXME: too heavyweight
						scheduleHandle = SharedScheduler.getInstance().schedule(ManagedScript.this, scheduledPeriod, scheduledPeriodUnit);					
					} else {
						log.warn("Script scheduling waiting on dependencies {}", pendingDependencies);
					}
				} else {
					log.debug("State not EXECUTING on return from execution");
				}
			}
			return null;
		}
		
		/**
		 * Executes the script 
		 * @return the elapsed time or -1 if pre-exec fails or returns false.
		 */
		protected long scriptExec() {
			try {
				log.debug("Starting collect");
//				try {
//					final Object preExec = invokeLifecycleClosure(LIFECYCLE_PREEXEC);
//					if(preExec!=null && (preExec instanceof Boolean)) {
//						if(!((Boolean)preExec).booleanValue()) {
//							log.warn("Pre-Exec return false. Skipping execution");
//							return -1L;
//						}
//					}
//				} catch (Exception ex) {
//					log.warn("Pre-Exec Failed: {}", ex);					
//					return -1L;
//				}
				final long start = System.currentTimeMillis();
//				if(hystrixEnabled.get()) {
//					runInCircuitBreaker();
//				} else {
					run();
//				}
				final long elapsed = System.currentTimeMillis() - start;
				return elapsed;
			} finally {
				final ITracer itracer = (ITracer)bindingMap.get("tracer");
				if(itracer!=null) {
					try { itracer.flush(); } catch (Exception x) {/* No Op */}
					lastTraceCount.set(itracer.getTracedCount());
					lastFlushCount.set(itracer.getFlushedCount());
				} else {
					log.warn("No tracer found in binding");
				}				
			}
		}
	}
	

	
	
	
//	
//	/**
//	 * Executes this script through the hystrix circuit breaker
//	 */
//	protected void runInCircuitBreaker() {
//		try {
//			commandBuilder.commandFor(new Callable<Object>(){
//				@Override
//				public Object call() throws Exception {					
//					return ManagedScript.this.run();
//				}
//			}).execute();
//		} catch (Exception ex) {
//			log.error("Failed to execute ScriptCommand", ex);
//			throw new RuntimeException("Failed to execute ScriptCommand", ex);
//		}
//	}
	
//	@Override
//	//@HystrixCommand(fallbackMethod="pause",  commandKey="getBeanName", groupKey="Foo", threadPoolKey="CollectorThreadPool")
//	public void doRunX() {
//		run();		
//	}
	
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
			log.warn("\n\t ================================ \n\t Waiting script [{}} unscheduled. Dependencies incomplete: {}", this.sourceFile, pendingDependencies);			
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
			updateProps();
			scheduleHandle = SharedScheduler.getInstance().schedule(this, scheduledPeriod, scheduledPeriodUnit);
			log.info("\n\t ================================ \n\t Waiting script [{}} scheduled. All dependencies complete.", this.sourceFile);
		}
	}
	
	
	/**
	 * Examines all the bindings looking for a value that looks like a post-compile 
	 * expression which is one that matches {@link #POST_COMPILE}.
	 * For each one found, the post-compile value is evaluated as a groovy script
	 * and the result replaces the binding value.
	 */
	protected void updateProps() {
		for(Map.Entry<String, Object> bind: bindingMap.entrySet()) {
			try {
				final Object o = bind.getValue();
				if(o!=null && (o instanceof CharSequence)) {
					final String v = o.toString();
					final Matcher m = POST_COMPILE.matcher(v);
					if(m.matches()) {
						final String expr = m.group(1);
						final Object evaled = evaluate("\"" + expr + "\"");
						bind.setValue(evaled);
						log.info("Completed post-compile for key [{}]: [{}] --> [{}]", bind.getKey(), o, evaled);
					}
				}
			} catch (Exception ex) {
				log.error("Failed to process post-compile on bind [{}:{}]", bind.getKey(), bind.getValue(), ex);
			}
		}
	}
	

	/**
	 * Creates a new on-enqueue runnable for a wrapped closeable
	 * @param key The index key
	 * @param map The map to remove the ref from when enqueued
	 * @return the runnable
	 */
	protected static Runnable onEnqueued(final String key, final Map<String, WeakReference<Closeable>> map) {
		return new Runnable() {
			public void run() {
				map.remove(key);
			}
		};
	}
	
	/**
	 * This provides a way of ensuring that objects created by the script that need to be closed,
	 * are closed, even if they are of classes the developer neglected to make them implement {@link Closeable}.
	 * @param nonCloseable The object to close
	 * @param closure A closure which will be passed the non closeable instance to dispose of.
	 */
	protected Object deferredClose(final Object nonCloseable, final Closure<?> closure) {
		if(nonCloseable != null) {
			final String key = "Closeable#" + wrappedCloseableSerial.incrementAndGet();
			final Runnable onEnqueued = onEnqueued(key, wrappedCloseables);
			final Closeable closer = (nonCloseable instanceof Closeable) ? (Closeable)nonCloseable : new Closeable() {
				public void close() throws IOException {
					try {
						closure.call(nonCloseable);
					} catch (Exception ex) {
						log.error("Failed to issue close on wrapped closeable [{}]", nonCloseable);
					}
				}				
			};
			wrappedCloseables.put("Closeable#" + wrappedCloseableSerial.incrementAndGet(), ReferenceService.getInstance().newWeakReference(closer, onEnqueued));
		}
		return nonCloseable;
	}
	
    /**
     * A helper method to allow the dynamic evaluation of groovy expressions using this
     * scripts binding as the variable scope
     *
     * @param expression is the Groovy script expression to evaluate
     * @return The return value of the script
     */
	@Override
    public Object evaluate(String expression) throws CompilationFailedException {
        GroovyShell shell = new GroovyShell(binding);        
        return shell.evaluate(expression);
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
	 * Adds the passed bindings to this script's binding map
	 * @param additionalBindings the bindings to add
	 */
	public void addBindings(final Map<String, Object> additionalBindings) {
		if(additionalBindings==null) throw new IllegalArgumentException("The passed additional bindings map was null");
		bindingMap.putAll(additionalBindings);
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
		setState(ScriptState.DESTROY);
		if(scheduleHandle!=null) {
			scheduleHandle.cancel(true);
			scheduleHandle = null;
		}		
		for(Object o: bindingMap.values()) {
			if(o!=null && (o instanceof Closeable)) {
				try {
					((Closeable)o).close();
				} catch (Exception x) {/* No Op */}
			}
		}
		bindingMap.clear();
		for(WeakReference<Closeable> cref: wrappedCloseables.values()) {
			final Closeable c = cref.get();
			if(c!=null) try { c.close(); } catch (Exception x) {/* No Op */}
		}
		wrappedCloseables.clear();
		fields.clear();
		try { dependencyManager.close(); } catch (Exception x) {/* No Op */}
		if(gcl!=null) {
			final Class<?>[] classes = gcl.getLoadedClasses();
			
			
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
					@SuppressWarnings("unchecked")
					final HashMap<Class<?>,ClassInfo> map = (HashMap<Class<?>,ClassInfo>)PrivateAccessor.invokeStatic(classInfo.getClass(), "getLocalClassInfoMap", new Object[0]);
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
//			System.gc();
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
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#getNavMap()
	 */
	@Override
	public String[] getNavMap() {
		return (String[])bindingMap.get("navmap");
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
	
	//====================================================================================================
	//			Cache Delegate Methods
	//====================================================================================================
	
//	protected final NonBlockingHashSet<String> globalCacheKeys = new NonBlockingHashSet<String>();
//	protected final String cacheKeyPrefix = getClass().getName();

	protected String ck(final String key) {
		final String ckey = cacheKeyPrefix + ":" +  key.trim();
		globalCacheKeys.add(ckey);
		return ckey;
	}
	
	protected String dk(final String key) {
		final String ckey = cacheKeyPrefix + ":" +  key.trim();
		deltaKeys.add(ckey);
		return ckey;
	}
	

	/**
	 * Retrieves a value from cache
	 * @param key The key to retrieve by
	 * @param createIfNotFound A closure that will create the value if not found in cache
	 * @return The value or null if the key was not bound and the closure returned null
	 * @param <T> The expected type of the object being retrieved
	 */
	public <T> T get(final String key, final Closure<T> createIfNotFound) {
		return cache.get(ck(key), createIfNotFound);
	}

	/**
	 * Retrieves a value from cache
	 * @param key The key to retrieve by
	 * @param expiryPeriod The expiry period for the newly created cache value if created
	 * @param unit The expiry period unit
	 * @param createIfNotFound A closure that will create the value if not found in cache
	 * @return The value or null if the key was not bound and the closure returned null
	 * @param <T> The expected type of the object being retrieved
	 */
	public <T> T get(final String key, final long expiryPeriod, final TimeUnit unit, final Closure<T> createIfNotFound) {
		return cache.get(ck(key), expiryPeriod, unit, createIfNotFound);
	}

	/**
	 * Retrieves a value from cache
	 * @param key The key to retrieve by
	 * @param expiryPeriod The expiry period for the newly created cache value if created (ms)
	 * @param createIfNotFound A closure that will create the value if not found in cache
	 * @return The value or null if the key was not bound and the closure returned null
	 * @param <T> The expected type of the object being retrieved
	 */
	public <T> T get(final String key, final long expiryPeriod, final Closure<T> createIfNotFound) {
		return cache.get(ck(key), expiryPeriod, createIfNotFound);
	}
	
	/**
	 * Removes and returns the named cache entry
	 * @param key The cache key
	 * @return the formerly bound object or null
	 */
	public <T> T remove(final String key) {
		return cache.remove(key);
	}

	/**
	 * Retrieves a value from cache
	 * @param key The key to retrieve by
	 * @return The value or null if the key was not bound and the closure returned null
	 * @param <T> The expected type of the object being retrieved
	 */
	public <T> T get(final String key) {
		return cache.get(ck(key));
	}

	/**
	 * Puts a value into the cache
	 * @param key The key to bind the value under
	 * @param value The value to bind
	 * @param expiryPeriod The expiry period for this cache item. Ignored if less than 1.
	 * @param unit The unit of the expiry period. Ignored if expiry period is less than 1. Defaults to {@link TimeUnit#MILLISECONDS} if null.
	 * @param onRemove An optional closure to be called when bound cache entry is removed or replaced
	 * @return the unbound value that was replaced or null
	 * @param <T> The type of the object being put
	 */
	public <T> T put(final String key, final T value, final long expiryPeriod, final TimeUnit unit, final Closure<Void> onRemove) {
		return cache.put(ck(key), value, expiryPeriod, unit, onRemove);
	}

	/**
	 * Puts a value into the cache
	 * @param key The key to bind the value under
	 * @param value The value to bind
	 * @param expiryPeriod The expiry period for this cache item. Ignored if less than 1.
	 * @param unit The unit of the expiry period. Ignored if expiry period is less than 1. Defaults to {@link TimeUnit#MILLISECONDS} if null.
	 * @return the unbound value that was replaced or null
	 * @param <T> The type of the object being put
	 */
	public <T> T put(final String key, final T value, final long expiryPeriod, final TimeUnit unit) {
		return cache.put(ck(key), value, expiryPeriod, unit);
	}

	/**
	 * Puts a value into the cache
	 * @param key The key to bind the value under
	 * @param value The value to bind
	 * @param expiryPeriod The expiry period for this cache item. Ignored if less than 1.
	 * @return the unbound value that was replaced or null
	 * @param <T> The type of the object being put
	 */
	public <T> T put(final String key, final T value, final long expiryPeriod) {
		return cache.put(ck(key), value, expiryPeriod);
	}

	/**
	 * Puts a value into the cache
	 * @param key The key to bind the value under
	 * @param value The value to bind
	 * @param onRemove An optional closure to be called when bound cache entry is removed or replaced
	 * @return the unbound value that was replaced or null
	 * @param <T> The type of the object being put
	 */
	public <T> T put(final String key, final T value, final Closure<Void> onRemove) {
		return cache.put(ck(key), value, onRemove);
	}

	/**
	 * Puts a value into the cache
	 * @param key The key to bind the value under
	 * @param value The value to bind
	 * @param expiryPeriod The expiry period for this cache item. Ignored if less than 1.
	 * @param onRemove An optional closure to be called when bound cache entry is removed or replaced
	 * @return the unbound value that was replaced or null
	 * @param <T> The type of the object being put
	 */
	public <T> T put(final String key, final T value, final long expiryPeriod, final Closure<Void> onRemove) {
		return cache.put(ck(key), value, expiryPeriod, onRemove);
	}

	/**
	 * Puts a value into the cache
	 * @param key The key to bind the value under
	 * @param value The value to bind
	 * @return the unbound value that was replaced or null
	 * @param <T> The type of the object being put
	 */
	public <T> T put(final String key, final T value) {
		return cache.put(ck(key), value);
	}
	
	/**
	 * Flushes all global cache entries for this script
	 */
	public void flushCache() {
		cache.flush(globalCacheKeys);
		globalCacheKeys.clear();
	}

	/**
	 * Registers a sample value and returns the delta between this sample and the prior
	 * @param key The delta sample key
	 * @param value The absolute sample value
	 * @return The delta or null if this was the first sample, or the last sample caused a reset
	 * @see com.heliosapm.streams.tracing.deltas.DeltaManager#delta(java.lang.String, long)
	 */			
	public Long delta(final String key, final long value) {
		return deltaManager.delta(dk(key), value);
	}
	
	/**
	 * Acquires the delta for the passed key for the passed value
	 * and passes the key and delta value to the passed closure if the delta is not null.
	 * @param key The delta key
	 * @param value The delta value
	 * @param closure The result handling closure
	 */
	public void delta(final String key, final long value, final Closure<?> closure) {
		final String dkey = dk(key);
		final Long d = deltaManager.delta(dkey, value);
		if(d!=null && closure != null) {
			closure.call(key, d);
		}
	}

	/**
	 * Registers a sample value and returns the delta between this sample and the prior
	 * @param key The delta sample key
	 * @param value The absolute sample value
	 * @return The delta or null if this was the first sample, or the last sample caused a reset
	 * @see com.heliosapm.streams.tracing.deltas.DeltaManager#delta(java.lang.String, double)
	 */			
	public Double delta(final String key, final double value) {
		return deltaManager.delta(dk(key), value);
	}
	
	/**
	 * Acquires the delta for the passed key for the passed value
	 * and passes the key and delta value to the passed closure if the delta is not null.
	 * @param key The delta key
	 * @param value The delta value
	 * @param closure The result handling closure
	 */
	public void delta(final String key, final double value, final Closure<?> closure) {
		final String dkey = dk(key);
		final Double d = deltaManager.delta(dkey, value);
		if(d!=null && closure != null) {
			closure.call(key, d);
		}
	}
	

	/**
	 * Registers a sample value and returns the delta between this sample and the prior
	 * @param key The delta sample key
	 * @param value The absolute sample value
	 * @return The delta or null if this was the first sample, or the last sample caused a reset
	 * @see com.heliosapm.streams.tracing.deltas.DeltaManager#delta(java.lang.String, int)
	 */	
	public Integer delta(final String key, final int value) {
		return deltaManager.delta(dk(key), value);
	}
	
	/**
	 * Acquires the delta for the passed key for the passed value
	 * and passes the key and delta value to the passed closure if the delta is not null.
	 * @param key The delta key
	 * @param value The delta value
	 * @param closure The result handling closure
	 */
	public void delta(final String key, final int value, final Closure<?> closure) {
		final String dkey = dk(key);
		final Integer d = deltaManager.delta(dkey, value);
		if(d!=null && closure != null) {
			closure.call(key, d);
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#resetDeltas()
	 */
	@Override
	public void resetDeltas() {
		for(String key: deltaKeys) {
			deltaManager.resetDouble(key);
			deltaManager.resetInt(key);
			deltaManager.resetLong(key);
		}
		deltaKeys.clear();
	}
	
	/**
	 * {@inheritDoc}
	 * @see javax.management.NotificationBroadcaster#addNotificationListener(javax.management.NotificationListener, javax.management.NotificationFilter, java.lang.Object)
	 */
	@Override
	public void addNotificationListener(final NotificationListener listener, final NotificationFilter filter, final Object handback) throws IllegalArgumentException {
		broadcaster.addNotificationListener(listener, filter, handback);		
	}
	
	/**
	 * {@inheritDoc}
	 * @see javax.management.NotificationBroadcaster#removeNotificationListener(javax.management.NotificationListener)
	 */
	@Override
	public void removeNotificationListener(final NotificationListener listener) throws ListenerNotFoundException {
		broadcaster.removeNotificationListener(listener);		
	}
	
	/**
	 * {@inheritDoc}
	 * @see javax.management.NotificationBroadcaster#getNotificationInfo()
	 */
	@Override
	public MBeanNotificationInfo[] getNotificationInfo() {		
		return NOTIF_INFOS;
	}
	
	/**
	 * {@inheritDoc}
	 * @see javax.management.NotificationEmitter#removeNotificationListener(javax.management.NotificationListener, javax.management.NotificationFilter, java.lang.Object)
	 */
	@Override
	public void removeNotificationListener(final NotificationListener listener, final NotificationFilter filter, final Object handback) throws ListenerNotFoundException {
		broadcaster.removeNotificationListener(listener, filter, handback);
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#getObjectName()
	 */
	@Override
	public ObjectName getObjectName() {
		return objectName;
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.springframework.beans.factory.NamedBean#getBeanName()
	 */
	public String getBeanName() {
		return getClass().getName();
	}
	
//	/**
//	 * {@inheritDoc}
//	 * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
//	 */
//	@Override
//	public void setApplicationContext(final ApplicationContext appCtx) throws BeansException {
//		this.appCtx = appCtx;				
//	}
	
	@Override
	public boolean isHystrixEnabled() {
		return hystrixEnabled.get();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#setCircuitBreaker(boolean)
	 */
	@Override
	public void setHystrixEnabled(final boolean enabled) {
		hystrixEnabled.set(enabled);
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#getLastTraceCount()
	 */
	@Override
	public long getLastTraceCount() {
		return lastTraceCount.get();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.groovy.ManagedScriptMBean#getLastFlushCount()
	 */
	@Override
	public long getLastFlushCount() {
		return lastFlushCount.get();
	}

	/**
	 * Returns the number of times that init-check has returned false.
	 * Resets to zero once the init-check succeeds.
	 * @return the number of times that init-check has returned false
	 */
	public long getInitCheckFails() {
		return initCheckFails.get();
	}
	
	/**
	 * Returns the number of times that pre-exec check has returned false or thrown an exception.
	 * Resets to zero once the pre-exec check succeeds.
	 * @return the number of times that pre-exec-check has returned false or thrown an exception.
	 */
	public long getPreExecCheckFails() {
		return preExecCheckFails.get();
	}
	
}
