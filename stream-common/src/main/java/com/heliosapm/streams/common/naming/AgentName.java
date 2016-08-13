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
package com.heliosapm.streams.common.naming;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.ObjectName;
import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.SimpleBindings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.io.NIOHelper;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.jmx.SharedNotificationExecutor;
import com.heliosapm.utils.net.LocalHost;


/**
 * <p>Title: AgentName</p>
 * <p>Description: Discovers the host and app name and notifies if they change</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.common.naming.AgentName</code></p>
 */

public class AgentName extends NotificationBroadcasterSupport  implements AgentNameMBean {
	/** The singleton instance */
	private static volatile AgentName instance = null;
	/** The singleton instance ctor lock */
	private static final Object lock = new Object();
	
	private final AtomicReference<ByteBuffer[]> bufferizedAgentName = new AtomicReference<ByteBuffer[]>(null);
	
	
	/*
	 * Looks like this:
	 * ================
	 * <total size>	 
	 * <app tag size>
	 * <app tag bytes>
	 * <hots tag size>
	 * <hots tag bytes>
	 */
	
	/*
	 * Remote Name:
	 * 	 - supplied app prop: remote.tsdb.id.app
	 *   - supplied app prop: remote.tsdb.id.app.key, value is the name of the sys prop on the remote
	 * 	 - remote system prop: tsdb.id.app
	 * 	 - remote sun.java.command  (needs parsing)
	 *   - if attach: jvm display   (needs parsing)
	 * Remote Host:
	 * 	 - supplied app prop: remote.tsdb.id.host
	 * 
	 * SysProps:  sun.java.command  (needs parsing)
	 */
	
	/** A set of agent name change listeners */
	private final Set<AgentNameChangeListener> listeners = new CopyOnWriteArraySet<AgentNameChangeListener>();
  /** The global tags */
  final Map<String, String> GLOBAL_TAGS = new ConcurrentHashMap<String, String>(6);
	
	/** The cached app name */
	private volatile String appName = null;
	/** The cached host name */
	private volatile String hostName = null;
	
	/** The app name source */
	private static volatile String appNameSource = null;
	/** The host name source */
	private static volatile String hostNameSource = null;
	
	
	/** Instance logger */
	private final Logger log = LoggerFactory.getLogger(getClass());
	
	/** Notification serial number source */
	final AtomicLong notifSerial = new AtomicLong(0L);
	
	private volatile boolean forceLowerCase = true;
	private volatile boolean shortHostName = true;

	private static final Charset UTF8 = Charset.forName("UTF8");
  /** Comma separated Key/Value pattern splitter */
  public static final Pattern KVP_PATTERN = Pattern.compile("\\s*?([^,\\s].*?)\\s*?=\\s*?([^,\\s].*?)\\s*?");
  /** The JVM's PID */
	public static final String SPID = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
	/** The JVM's host name according to the RuntimeMXBean */
	public static final String HOST = ManagementFactory.getRuntimeMXBean().getName().split("@")[1];
	/** The available core count */
	public static final int CORES = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
	/** Indicates if we're running on Windows */
	public static final boolean IS_WIN = System.getProperty("os.name", "").toLowerCase().contains("windows");
  

	
	/**
	 * Acquires the AgentName singleton instancecs
	 * @return the AgentName singleton instance
	 */
	public static AgentName getInstance() {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					instance = new AgentName();
					//ChangeNotifyingProperties.systemInstall();
					//ExtendedThreadManager.install();					
				}
			}
		}
		return instance;
	}
	
	private AgentName() {
		super(SharedNotificationExecutor.getInstance(), NOTIF_INFOS);
		forceLowerCase = ConfigurationHelper.getBooleanSystemThenEnvProperty(PROP_FORCE_LOWER_CASE, DEFAULT_FORCE_LOWER_CASE);
		shortHostName = ConfigurationHelper.getBooleanSystemThenEnvProperty(PROP_USE_SHORT_HOSTNAMES, DEFAULT_USE_SHORT_HOSTNAMES); 				
		loadExtraTags();
		getAppName();
		getHostName();
		initBufferized();
		JMXHelper.registerMBean(this, OBJECT_NAME);
		sendInitialNotif();
	}
	
	/**  */
	private static final byte[] COMMA_BYTE = ",".getBytes(UTF8); 
	
	/**
	 * Implements case conversion in accordance with {@code forceLowerCase}.
	 * @param input The string to case
	 * @return the possibly lowered case string
	 */
	private String c(final String input) {
		if(input==null) return null;
		if(forceLowerCase) {
			return input.toLowerCase();
		}
		return input;
	}
	
	private String h(final String hostName) {
		if(shortHostName && hostName.indexOf('.')!=-1) {
			return hostName.split("\\.")[0];
		}
		return hostName;
	}
	
	private synchronized void initBufferized() {				
		ByteBuffer[] buffs = new ByteBuffer[3];		
		final byte[] appTag = ("\"" + APP_TAG + "\":\"" + appName + "\"").getBytes(UTF8);
		final byte[] hostTag = ("\"" + HOST_TAG + "\":\"" + hostName + "\"").getBytes(UTF8);
		buffs[0] = (ByteBuffer)ByteBuffer.allocateDirect(appTag.length).put(appTag).flip();
		buffs[1] = (ByteBuffer)ByteBuffer.allocateDirect(hostTag.length).put(hostTag).flip();
		buffs[2] = (ByteBuffer)ByteBuffer.allocateDirect(appTag.length + hostTag.length + COMMA_BYTE.length).put(appTag).put(COMMA_BYTE).put(hostTag).flip();		
		final ByteBuffer[] oldBuff = bufferizedAgentName.getAndSet(buffs);
		NIOHelper.clean(oldBuff);
	}

	private void sendInitialNotif() {
		final Notification n = new Notification(NOTIF_ASSIGNED, OBJECT_NAME, notifSerial.incrementAndGet(), System.currentTimeMillis(), "AgentName assigned [" + appName + "@" + hostName + "]");
		Map<String, String> userData = new HashMap<String, String>(2);
		userData.put(APP_TAG, appName);
		userData.put(HOST_TAG, hostName);
		n.setUserData(userData);
		sendNotification(n);
	}
	
	/**
	 * Loads sysprop defined extra tags 
	 */
	private void loadExtraTags() {
		String v = System.getProperty(PROP_EXTRA_TAGS, "").replace(" ", "");
		if(!v.isEmpty()) {
			Matcher m = KVP_PATTERN.matcher(v);
			while(m.find()) {
				String key = clean(m.group(1), "group1");
				if(HOST_TAG.equalsIgnoreCase(key) || APP_TAG.equalsIgnoreCase(key)) continue;
				String value = clean(m.group(2), "group2");
				GLOBAL_TAGS.put(c(key), c(value));
			}
			log.info("Initial Global Tags:{}", GLOBAL_TAGS);
		}
	}
	
	/**
	 * Standard tag key, tag value and metric name string cleaner
	 * @param s The string to clean
	 * @param field The name of the field being cleaned for exception reporting
	 * @return the cleaned string
	 */
	public static String clean(final String s, final String field) {
		if(s==null || s.trim().isEmpty()) throw new IllegalArgumentException("The passed " + field + " was null or empty");
		return s.trim().toLowerCase();
	}	
	
	
	
	/**
	 * Returns the agent's global tags
	 * @return the agent's global tags
	 */
	public Map<String, String> getGlobalTags() {
		return new TreeMap<String, String>(GLOBAL_TAGS);
	}

	/**
	 * Attempts a series of methods of divining the host name
	 * @return the determined host name
	 */
	@Override
	public String getHostName() {
		if(hostName!=null) {
			return c(h(hostName));
		}		
		hostName = c(h(hostName()));		
		System.setProperty(PROP_HOST_NAME, hostName);
		GLOBAL_TAGS.put(HOST_TAG, hostName);
		return hostName;
	}
	
	/**
	 * Returns an id string displaying the host and app name
	 * @return the id string
	 */
	public String getId() {
		return getAppName() + "@" + getHostName();
	}
	
	/**
	 * Returns the agent name app tag, already prepped for serialization
	 * @return the agent name app tag buffer
	 */
	public ByteBuffer getAgentNameAppTagBuffer() {
		return bufferizedAgentName.get()[0].asReadOnlyBuffer();
	}
	
	/**
	 * Returns the agent name host tag, already prepped for serialization
	 * @return the agent name host tag buffer
	 */
	public ByteBuffer getAgentNameHostTagBuffer() {
		return bufferizedAgentName.get()[1].asReadOnlyBuffer();
	}
	
	/**
	 * Returns the agent name app and host tags, already prepped for serialization
	 * @return the agent name app and host tags buffer
	 */
	public ByteBuffer getAgentNameTagsBuffer() {
		return bufferizedAgentName.get()[2].asReadOnlyBuffer();
	}

	
	/**
	 * Attempts to find a reliable app name
	 * @return the app name
	 */
	@Override
	public String getAppName() {
		if(appName!=null) {
			return c(appName);
		}
		appName = c(appName());
		System.setProperty(PROP_APP_NAME, appName);
		GLOBAL_TAGS.put(APP_TAG, appName);
		return appName;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.common.naming.AgentNameMBean#getAppNameSource()
	 */
	@Override
	public String getAppNameSource() {		
		return appNameSource;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.common.naming.AgentNameMBean#getHostNameSource()
	 */
	@Override
	public String getHostNameSource() {
		return hostNameSource;
	}
	
	/**
	 * Adds an AgentName change listener 
	 * @param listener the listener to add
	 */
	public void addAgentNameChangeListener(final AgentNameChangeListener listener) {
		if(listener!=null) listeners.add(listener);
	}
	
	/**
	 * Removes an AgentName change listener 
	 * @param listener the listener to remove
	 */
	public void removeAgentNameChangeListener(final AgentNameChangeListener listener) {
		if(listener!=null) listeners.remove(listener);
	}
	
	/**
	 * Fires an AgentName change event
	 * @param app The new app name, or null if only the host changed
	 * @param host The new host name, or null if only the app changed
	 */
	private void fireAgentNameChange(final String app, final String host) {
		if(app==null && host==null) return;
		Map<String, String> userData = new HashMap<String, String>(2);
		String notifType = null;
		if(app!=null && host!=null) {
			notifType = NOTIF_BOTH_NAME_CHANGE;
			userData.put(APP_TAG, app);
			userData.put(HOST_TAG, host);
			
		} else {
			if(app==null) {
				notifType = NOTIF_HOST_NAME_CHANGE;				
				userData.put(HOST_TAG, host);				
			} else {
				notifType = NOTIF_APP_NAME_CHANGE;				
				userData.put(APP_TAG, app);				
			}
		}
		final Notification n = new Notification(notifType, OBJECT_NAME, notifSerial.incrementAndGet(), System.currentTimeMillis(), "AgentName reset. New Id: [" + appName + "@" + hostName + "]");		
		n.setUserData(userData);
		sendNotification(n);
		for(final AgentNameChangeListener listener: listeners) {
			SharedNotificationExecutor.getInstance().execute(new Runnable(){
				@Override
				public void run() {
					listener.onAgentNameChange(app, host);
				}
			});
			
		}
	}
	
	/**
	 * Resets the cached app and host names. If a new name is set, the corresponding
	 * system property {@link #PROP_HOST_NAME} and/or {@link #PROP_APP_NAME}
	 * will be updated. 
	 * @param newAppName The new app name to set. Ignored if null or empty.
	 * @param newHostName The new host name to set. Ignored if null or empty.
	 */
	@Override
	public void resetNames(final String newAppName, final String newHostName) {
		boolean hostUpdated = false;
		boolean appUpdated = false;
		if(newHostName!=null && newHostName.trim().isEmpty()) {
			hostName = newHostName.trim();
			System.setProperty(PROP_HOST_NAME, hostName);
			GLOBAL_TAGS.put(HOST_TAG, hostName);
			hostUpdated = true;
		}
		if(newAppName!=null && newAppName.trim().isEmpty()) {
			appName = newAppName.trim();
			System.setProperty(PROP_APP_NAME, appName);
			GLOBAL_TAGS.put(APP_TAG, appName);
			appUpdated = true;
		}
		initBufferized();
		log.info("Names reset: app:[{}], host:[{}]", newAppName, newHostName);
		fireAgentNameChange(appUpdated ? newAppName : null, hostUpdated ? newHostName : null); 
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.common.naming.AgentNameMBean#resetAppName(java.lang.String)
	 */
	@Override
	public void resetAppName(final String newAppName) {
		if(newAppName!=null && newAppName.trim().isEmpty() && !newAppName.trim().equals(appName)) {
			appName = newAppName.trim();
			initBufferized();
			System.setProperty(PROP_APP_NAME, appName);
			GLOBAL_TAGS.put(APP_TAG, appName);
			log.info("AppName reset: app:[{}]", newAppName);
			fireAgentNameChange(newAppName, null);
		}

	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.common.naming.AgentNameMBean#resetHostName(java.lang.String)
	 */
	@Override
	public void resetHostName(final String newHostName) {
		if(newHostName!=null && newHostName.trim().isEmpty() && !newHostName.trim().equals(hostName)) {
			hostName = newHostName.trim();
			initBufferized();
			System.setProperty(PROP_HOST_NAME, hostName);
			GLOBAL_TAGS.put(HOST_TAG, hostName);
			log.info("HostName reset: host:[{}]", newHostName);
			fireAgentNameChange(null, newHostName);
		}

	}
	
	
	/**
	 * Attempts a series of methods of divining the host name
	 * @return the determined host name
	 */
	private static String hostName() {	
		String host = System.getProperty(PROP_HOST_NAME, "").trim();
		if(host!=null && !host.isEmpty()) {
			hostNameSource = "PROP_HOST_NAME:" + PROP_HOST_NAME + ":" + host.trim();
			return host.trim();
		}
		host = LocalHost.getHostNameByNic();
		if(host!=null) {
			hostNameSource = "HostNameByNic:" + host.trim();
			return host.trim();		
		}
		host = LocalHost.getHostNameByInet();
		if(host!=null) {
			hostNameSource = "HostNameByInet:" + host.trim();
			return host.trim();
		}
		final String env = IS_WIN ? "COMPUTERNAME" : "HOSTNAME";
		host = System.getenv(env);
		if(host!=null && !host.trim().isEmpty()) {
			hostNameSource = "Env:" + env + ":" + host.trim();
			return host.trim();
		}
		hostNameSource = "Runtime:" + HOST;
		return HOST;
	}	
	
	/**
	 * The real host name (no configs consulted)
	 * @return the determined host name
	 */
	public static String realHostName() {	
		String host = LocalHost.getHostNameByNic();
		if(host!=null) return host;		
		host = LocalHost.getHostNameByInet();
		if(host!=null) return host;
		host = System.getenv(IS_WIN ? "COMPUTERNAME" : "HOSTNAME");
		if(host!=null && !host.trim().isEmpty()) return host;
		return HOST;
	}	
	
	/**
	 * Returns the app and host as default tags
	 * @return a tag map
	 */
	public Map<String, String> defaultTags() {
		final Map<String, String> map = new LinkedHashMap<String, String>(2);
		map.put("app", getAppName());
		map.put("host", getHostName());
		return map;
	}
	
	/**
	 * Attempts to find a reliable app name
	 * @return the app name
	 */
	private static String appName() {
		String appName = System.getProperty(PROP_APP_NAME, "").trim();
		if(appName!=null && !appName.isEmpty()) {
			appNameSource = "PROP_APP_NAME:" + PROP_APP_NAME + ":" + appName;
			return appName;
		}
		appName = getSysPropAppName();
		if(appName!=null && !appName.trim().isEmpty()) {
			appNameSource = "SysPropAppName:" + System.getProperty(SYSPROP_APP_NAME, "").trim() + ":" + appName.trim();			
			return appName.trim();
		}
		appName = getJSAppName();
		if(appName!=null && !appName.trim().isEmpty()) {
			appNameSource = "JSAppName:" + appName.trim();
			return appName.trim();		
		}
		appName = System.getProperty("spring.boot.admin.client.name", null);
		if(appName!=null && !appName.trim().isEmpty()) {
			String app = cleanAppName(appName);
			if(app!=null && !app.trim().isEmpty()) {
				appNameSource = "SpringBootAdminClient:" + app.trim();
				return app.trim();
			}
		}
		appName = getVMSupportAppName();
		if(appName!=null && !appName.trim().isEmpty()) {			
			return appName.trim();
		}
		//  --main from args ?
		appNameSource = "PID:" + SPID;
		return SPID;
	}
	
	/**
	 * Attempts to find the remote app name
	 * @param remote An MBeanServer connection to the remote app
	 * @return the remote app name
	 */
	public static String remoteAppName(final MBeanServerConnection remote) {
		final ObjectName runtimeMXBean = JMXHelper.objectName(ManagementFactory.RUNTIME_MXBEAN_NAME);
		try {
			RuntimeMXBean rt = JMX.newMXBeanProxy(remote, runtimeMXBean, RuntimeMXBean.class, false);
			final Properties sysProps = mapToProps(rt.getSystemProperties());
			String appName = sysProps.getProperty(PROP_APP_NAME, "").trim();
			if(appName!=null && !appName.isEmpty()) return appName;
			appName = sysProps.getProperty(REMOTE_PROP_APP_NAME, "").trim();
			if(appName!=null && !appName.isEmpty()) return appName;
			appName = sysProps.getProperty("spring.boot.admin.client.name", "").trim();
			if(appName!=null && !appName.isEmpty()) return appName;			
			appName = getRemoteJSAppName(remote);
			if(appName!=null && !appName.isEmpty()) return appName;
			appName = sysProps.getProperty("sun.java.command").trim();
			if(appName!=null && !appName.isEmpty()) {
				return cleanAppName(appName);
			}
			appName = sysProps.getProperty("sun.rt.javaCommand").trim();
			if(appName!=null && !appName.isEmpty()) {
				return cleanAppName(appName);
			}
			return rt.getName().split("@")[0];
		} catch (Exception ex) {
			throw new RuntimeException("Failed to get any app name", ex);
		}		
	}
	
	/**
	 * Converts a map to properties
	 * @param map the map
	 * @return the properties
	 */
	protected static Properties mapToProps(final Map<?, ?> map) {
		final Properties p = new Properties();
		if(map!=null && !map.isEmpty()) {
			for(Map.Entry<?, ?> entry: map.entrySet()) {
				if(entry.getKey()==null || entry.getValue()==null) continue;
				p.setProperty(entry.getKey().toString(), entry.getValue().toString());
			}
		}
		return p;
	}
	
	/**
	 * Attempts to find the remote host name
	 * @param remote An MBeanServer connection to the remote app
	 * @return the remote host name
	 */
	public static String remoteHostName(final MBeanServerConnection remote) {
		final ObjectName runtimeMXBean = JMXHelper.objectName(ManagementFactory.RUNTIME_MXBEAN_NAME);
		final boolean forceLowerCase = ConfigurationHelper.getBooleanSystemThenEnvProperty(PROP_FORCE_LOWER_CASE, DEFAULT_FORCE_LOWER_CASE);
		final boolean shortHost = ConfigurationHelper.getBooleanSystemThenEnvProperty(PROP_USE_SHORT_HOSTNAMES, DEFAULT_USE_SHORT_HOSTNAMES); 				

		try {
			RuntimeMXBean rt = JMX.newMXBeanProxy(remote, runtimeMXBean, RuntimeMXBean.class, false);
			final Properties sysProps = mapToProps(rt.getSystemProperties());
			String hostName = sysProps.getProperty(PROP_HOST_NAME, "").trim();
			if(hostName==null || hostName.isEmpty()) {
				hostName = sysProps.getProperty(REMOTE_PROP_HOST_NAME, "").trim();
			}
			if(hostName==null || hostName.isEmpty()) {
				hostName = getRemoteJSHostName(remote);
			}								
			if(hostName==null || hostName.isEmpty()) {
				hostName = rt.getName().split("@")[1];
			}
			if(forceLowerCase) hostName = hostName.toLowerCase();
			if(shortHost && hostName.indexOf('.')!=-1) hostName = hostName.split("\\.")[0]; 
			return hostName;
		} catch (Exception ex) {
			throw new RuntimeException("Failed to get any host name", ex);
		}		
	}
	
	
	
	/**
	 * Attempts to determine the app name by looking up the value of the 
	 * system property named in the value of the system prop {@link #SYSPROP_APP_NAME}
	 * @return The app name or null if {@link #SYSPROP_APP_NAME} was not defined
	 * or did not resolve.
	 */
	public static String getSysPropAppName() {
		String appProp = System.getProperty(SYSPROP_APP_NAME, "").trim();
		if(appProp==null || appProp.isEmpty()) return null;
		return ConfigurationHelper.getSystemThenEnvProperty(appProp, null);
	}
	
	/**
	 * Attempts to determine the app name by looking up the value of the 
	 * system property {@link #JS_APP_NAME}, and compiling its value
	 * as a JS script, then returning the value of the evaluation of the script.
	 * The following binds are passed to the script: <ul>
	 * 	<li><b>sysprops</b>: The system properties</li>
	 * 	<li><b>agprops</b>: The agent properties which will be an empty properties instance if {@link JMXHelper#getAgentProperties()} failed.</li>
	 *  <li><b>envs</b>: A map of environment variables</li>
	 *  <li><b>mbs</b>: The platform MBeanServer</li>
	 *  <li><b>cla</b>: The command line arguments as an array of strings</li>
	 * </ul>
	 * @return The app name or null if {@link #JS_APP_NAME} was not defined
	 * or did not compile, or did not return a valid app name
	 */
	public static String getJSAppName() {
		String js = System.getProperty(JS_APP_NAME, "").trim();
		if(js==null || js.isEmpty()) return null;
		try {
			ScriptEngine se = new ScriptEngineManager().getEngineByExtension("js");
			Bindings b = new SimpleBindings();
			b.put("sysprops", System.getProperties());
			b.put("envs", System.getenv());
			b.put("mbs", JMXHelper.getHeliosMBeanServer());
			b.put("cla", ManagementFactory.getRuntimeMXBean().getInputArguments().toArray(new String[0]));
			Properties p = JMXHelper.getAgentProperties();
			b.put("agprops", p);
			Object value = se.eval(js, b);
			if(value!=null && !value.toString().trim().isEmpty()) return value.toString().trim();
			return null;
		} catch (Exception ex) {
			return null;
		}
	}
	
	/**
	 * Attempts to determine a remote app name by looking up the value of the 
	 * system property {@link #REMOTE_JS_APP_NAME}, and compiling its value
	 * as a JS script, then returning the value of the evaluation of the script.
	 * The following binds are passed to the script: <ul>
	 *  <li><b>mbs</b>: The remote MBeanServerConnection</li>
	 * </ul>
	 * @param remote The remote MBeanServer connection
	 * @return The app name or null if {@link #REMOTE_JS_APP_NAME} was not defined
	 * or did not compile, or did not return a valid app name
	 */
	public static String getRemoteJSAppName(final MBeanServerConnection remote) {
		String js = System.getProperty(REMOTE_JS_APP_NAME, "").trim();
		if(js==null || js.isEmpty()) return null;
		try {
			ScriptEngine se = new ScriptEngineManager().getEngineByExtension("js");
			Bindings b = new SimpleBindings();
			b.put("mbs", remote);
			Object value = se.eval(js, b);
			if(value!=null && !value.toString().trim().isEmpty()) return value.toString().trim();
			return null;
		} catch (Exception ex) {
			return null;
		}
	}
	
	/**
	 * Attempts to determine a remote host name by looking up the value of the 
	 * system property {@link #REMOTE_JS_HOST_NAME}, and compiling its value
	 * as a JS script, then returning the value of the evaluation of the script.
	 * The following binds are passed to the script: <ul>
	 *  <li><b>mbs</b>: The remote MBeanServerConnection</li>
	 * </ul>
	 * @param remote The remote MBeanServer connection
	 * @return The host name or null if {@link #REMOTE_JS_HOST_NAME} was not defined
	 * or did not compile, or did not return a valid app name
	 */
	public static String getRemoteJSHostName(final MBeanServerConnection remote) {
		String js = System.getProperty(REMOTE_JS_HOST_NAME, "").trim();
		if(js==null || js.isEmpty()) return null;
		try {
			ScriptEngine se = new ScriptEngineManager().getEngineByExtension("js");
			Bindings b = new SimpleBindings();
			b.put("mbs", remote);
			Object value = se.eval(js, b);
			if(value!=null && !value.toString().trim().isEmpty()) return value.toString().trim();
			return null;
		} catch (Exception ex) {
			return null;
		}
	}
	
	
	
	
	/**
	 * Attempts to find an app name from a few different properties
	 * found in <b><code>sun.misc.VMSupport.getAgentProperties()</code></b>.
	 * Current properties are: <ul>
	 * 	<li>sun.java.command</li>
	 *  <li>sun.rt.javaCommand</li>
	 * 	<li>program.name</li>
	 * </ul>
	 * @return an app name or null if the reflective invocation fails,
	 * or no property was found, or if the clean of the found app names
	 * did not return an acceptable name.
	 */
	public static String getVMSupportAppName() {
		Properties p = JMXHelper.getAgentProperties();
		String app = p.getProperty("sun.java.command", null);
		if(app!=null && !app.trim().isEmpty()) {
			app = cleanAppName(app);			
			if(app!=null && !app.trim().isEmpty()) {
				appNameSource = "VMSupport(sun.java.command):" + app.trim(); 
				return app.trim();
			}
		}
		app = p.getProperty("sun.rt.javaCommand", null);
		if(app!=null && !app.trim().isEmpty()) {
			app = cleanAppName(app);			
			if(app!=null && !app.trim().isEmpty()) {
				appNameSource = "VMSupport(sun.rt.javaCommand):" + app.trim();
				return app.trim();				
			}
		}		
		app = p.getProperty("program.name", null);
		if(app!=null && !app.trim().isEmpty()) {
			app = cleanAppName(app);			
			if(app!=null && !app.trim().isEmpty()) {
				appNameSource = "VMSupport(program.name):" + app.trim();
				return app.trim();				
			}
		}
		return null;
	}
	
	/**
	 * Cleans an app name
	 * @param appName The app name
	 * @return the cleaned name or null if the result is no good
	 */
	public static String cleanAppName(final String appName) {
		final String[] frags = appName.split("\\s+");
		if(appName.contains(".jar")) {
			
			for(String s: frags) {
				if(s.endsWith(".jar")) {
					String[] jfrags = s.split("\\.");
					return jfrags[jfrags.length-1];
				}
			}
		} else {
			String className = frags[0];
			Class<?> clazz = loadClassByName(className, null);
			if(clazz!=null) {
				return clazz.getSimpleName();
			}
		}
		
		
		return null;
	}
	
	/**
	 * Loads a class by name
	 * @param className The class name
	 * @param loader The optional class loader
	 * @return The class of null if the name could not be resolved
	 */
	public static Class<?> loadClassByName(final String className, final ClassLoader loader) {
		try {
			if(loader!=null) {
				return Class.forName(className, true, loader);
			} 
			return Class.forName(className);
		} catch (Exception ex) {
			return null;
		}
	}	
	

}
