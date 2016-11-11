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
package com.heliosapm.streams.collector.jmx;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Pattern;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.InvalidAttributeValueException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServerConnection;
import javax.management.NotCompliantMBeanException;
import javax.management.Notification;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.QueryExp;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnectionNotification;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashSet;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.heliosapm.streams.collector.jmx.protocol.tunnel.ClientProvider;
import com.heliosapm.streams.collector.ssh.SSHTunnelManager;
import com.heliosapm.streams.collector.timeout.TimeoutService;
import com.heliosapm.streams.common.naming.AgentName;
import com.heliosapm.streams.hystrix.HystrixCommandFactory;
import com.heliosapm.streams.hystrix.HystrixCommandProvider;
import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.lang.StringHelper;

import io.netty.util.Timeout;

/**
 * <p>Title: JMXClient</p>
 * <p>Description: A managed JMX client for data collection</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.jmx.JMXClient</code></p>
 */

public class JMXClient implements MBeanServerConnection, Closeable {
	/** The JMX URL */
	protected final String jmxUrl;
	/** The JMX Service URL */
	protected final JMXServiceURL jmxServiceUrl;
	/** The JMXConnector */
	protected final JMXConnector jmxConnector;
	/** The JMXConnector env */
	protected final Map<String, Object> env = new HashMap<String, Object>();
	/** The mbean server connection acquired from the JMXConnector */
	protected volatile MBeanServerConnection server = null;
	/** The timeout on connecting and acquiring a connection */
	protected final long connectTimeoutSecs;
	/** Instance logger */
	protected final Logger log;
	/** Accumulator for total jmx remoting time */
	protected final LongAdder remotingTime = new LongAdder();
	
	/** The tracing app name */
	protected final String remoteApp;
	/** The tracing host name */
	protected final String remoteHost;
	
	/** The hystrix command factory to use if hystrix is enabled */
	protected final HystrixCommandProvider<Object> commandBuilder;
	/** Indicates if hystrix circuit breakers should be used for jmx clients */
	protected final AtomicBoolean hystrixEnabled = new AtomicBoolean(false);
	
	
	/** The default connect timeout in seconds */
	public static final long DEFAULT_CONNECT_TIMEOUT = 10;
	/** The URL path query arg for the tracing app name we're connecting to */
	public static final String APP_QUERY_ARG = "app";
	/** The URL path query arg for the tracing host name we're connecting to */
	public static final String HOST_QUERY_ARG = "host";
	
	/** The config key for jmx-clients hystrix circuit breaker commands */
	public static final String CONFIG_HYSTRIX = "component.jmxclient.hystrix";
	/** The config key for hystrix circuit breaker enablement */
	public static final String CONFIG_HYSTRIX_ENABLED = CONFIG_HYSTRIX + ".enabled";
	/** The default hystrix circuit breaker enablement */
	public static final boolean DEFAULT_HYSTRIX_ENABLED = false;
	
	/** Listener registrations that should be saved an re-applied on re-connect */
	protected final NonBlockingHashSet<SavedNotificationEvent> registrations = new NonBlockingHashSet<SavedNotificationEvent>();
	
	private static Set<Method> instrumentedMethods;
	
	static {
		try {
			HashSet<Method> tmpMethods = new HashSet<Method>();
			tmpMethods.add(MBeanServerConnection.class.getDeclaredMethod("getAttributes", ObjectName.class, String[].class));
			tmpMethods.add(MBeanServerConnection.class.getDeclaredMethod("getAttribute", ObjectName.class, String.class));
			tmpMethods.add(MBeanServerConnection.class.getDeclaredMethod("invoke", ObjectName.class, String.class, Object[].class, String[].class));
			instrumentedMethods = Collections.unmodifiableSet(new HashSet<Method>(tmpMethods));
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			throw new RuntimeException(ex);
		}
	}
	
	/** Client cache */
	protected static final Cache<Object, JMXClient> clients = CacheBuilder.newBuilder()
		.concurrencyLevel(Runtime.getRuntime().availableProcessors())
		.initialCapacity(1028)
		.weakKeys()
		.removalListener(new RemovalListener<Object, JMXClient>() {
			@Override
			public void onRemoval(final RemovalNotification<Object, JMXClient> notification) {
				if(notification!=null) {
					final JMXClient client = notification.getValue();
					if(client!=null) {
						client.log.info("Closing JMXClient [{}], Key: [{}], Removal Cause: [{}], Cache Size: [{}]", client.jmxServiceUrl, notification.getKey(), notification.getCause().name(), clients.size());
						try { client.jmxConnector.close(); } catch (Exception ex) {/* No Op */}
					}
				}
			}
		})
		.build();
	
	/**
	 * Creates a new JMXClient
	 * @param key A key that will uniquely identify the created client
	 * @param cs A string parsed as <b><code>&lt;JMX URL&gt;[[,&lt;Connect Timeout (sec)&gt;],&lt;Credentials&gt;]</code></b>
	 * @return the JMXClient
	 */
	public static JMXClient newInstance(final Object key, final CharSequence cs) {
		if(cs==null) throw new IllegalArgumentException("The passed char sequence was null");
		if(key==null) throw new IllegalArgumentException("The passed client key was null");
		final String str = cs.toString().trim();
		if(str.isEmpty()) throw new IllegalArgumentException("The passed char sequence was empty");
		final String[] frags = StringHelper.splitString(str, ',', true);
		final String url = frags[0];
		final long connectTimeoutSecs;
		final String[] credentials;
		if(frags.length > 1) {
			long tmp = -1;
			try { 
				tmp = Long.parseLong(frags[1]);
			} catch (Exception ex) {
				tmp = DEFAULT_CONNECT_TIMEOUT;
			}
			connectTimeoutSecs = tmp;
		} else {
			connectTimeoutSecs = DEFAULT_CONNECT_TIMEOUT;
		}
		if(frags.length > 2) {
			if(frags.length == 3) {
				credentials = new String[]{frags[2], ""};
			} else {
				credentials = new String[]{frags[2], frags[3]};
			}
		} else {
			credentials = new String[0];
		}
		try {
			return clients.get(key, new Callable<JMXClient>(){
				@Override
				public JMXClient call() throws Exception {
					final JMXClient client = new JMXClient(url, connectTimeoutSecs, credentials);
					client.addConnectionNotificationListener(new NotificationListener(){
						@Override
						public void handleNotification(final Notification n, final Object handback) {
							if(n instanceof JMXConnectionNotification) {
								if(JMXConnectionNotification.CLOSED.equals(n.getType()) || JMXConnectionNotification.FAILED.equals(n.getType())) {
									try { client.removeConnectionNotificationListener(this); } catch (Exception x) {/* No Op */}
									client.log.warn("In cache JMXClient received close notif: [{}]", n.getType());
									try { client.jmxConnector.close(); } catch (Exception x) {/* No Op */}
									clients.invalidate(key);
								}
							}
							
						}
					},null , null);
					return client;
				}
			});
		} catch (Exception ex) {
			throw new RuntimeException("Failed to acquire JMXClient for [" + cs + "]", ex);
		}
		
	}
	
	/**
	 * Creates a new JMXClient
	 * @param jmxUrl The JMX URL
	 * @param connectTimeoutSecs The timeout on connecting and acquiring a connection
	 * @param credentials The optional credentials
	 */
	public JMXClient(final String jmxUrl, final long connectTimeoutSecs, final String...credentials) {
		super();
		this.jmxUrl = jmxUrl;
		this.connectTimeoutSecs = connectTimeoutSecs;
		env.put(ClientProvider.RECONNECT_TIMEOUT_KEY, this.connectTimeoutSecs);
		JMXServiceURL jurl = JMXHelper.serviceUrl(jmxUrl);
		final String rhost = jurl.getHost();
		final int rport = jurl.getPort();
		if("tunnel".equals(jurl.getProtocol())) {
			final int localPort = SSHTunnelManager.getInstance().getPortForward(rhost, rport);
			jurl = JMXHelper.serviceUrl("service:jmx:jmxmp://localhost:" + localPort);
		}
		jmxServiceUrl = jurl;
		log = LogManager.getLogger(getClass().getName() + "-" + rhost.replace('.', '_') + "-" + rport);
		if(credentials!=null && credentials.length > 1) {
			final String[] creds = new String[2];
			System.arraycopy(credentials, 0, creds, 0, 2);
			env.put(JMXConnector.CREDENTIALS, creds);
		}
		final Map<String, String> qArgs = queryArgsToMap(jmxServiceUrl);
		remoteApp = qArgs.get(APP_QUERY_ARG);
		remoteHost = qArgs.get(HOST_QUERY_ARG);
		hystrixEnabled.set(ConfigurationHelper.getBooleanSystemThenEnvProperty(CONFIG_HYSTRIX_ENABLED, DEFAULT_HYSTRIX_ENABLED));
		if(hystrixEnabled.get()) {
			commandBuilder = HystrixCommandFactory.getInstance().builder(CONFIG_HYSTRIX, "jmx-remote-" + jmxServiceUrl.getProtocol())
					.andCommandKey(jmxServiceUrl.getHost().replace('.', '-') + "." + jmxServiceUrl.getPort())
					.andThreadPoolKey("jmxremoting")
					.build();
		} else {
			commandBuilder = null;
		}
		
		
		try {
			jmxConnector = JMXConnectorFactory.newJMXConnector(jmxServiceUrl, env);
		} catch (IOException iex) {
			throw new RuntimeException("Failed to create JMXConnector", iex);
		}
	}
	
	public static final Pattern QARG_SPLITTER = Pattern.compile("\\W");
	
	public static Map<String, String> queryArgsToMap(final JMXServiceURL jmxUrl) {
		final String urlPath = jmxUrl.getURLPath();
		final String[] frags = StringHelper.splitString(urlPath, '&', true);		
		final Map<String, String> map = new HashMap<String, String>();
		for(String pair: frags) {
			String[] keyValue = StringHelper.splitString(pair, '=', true);
			if(keyValue.length==2) {
				if(!keyValue[0].isEmpty() && !keyValue[1].isEmpty()) {
					map.put(keyValue[0], keyValue[1]);
				}
			}
		}
		return map;
	}
	
	@Override
	public void close() throws IOException {
		try { jmxConnector.close(); } catch (Exception x) {/* No Op */}
		env.clear();
		registrations.clear();
		server = null;		
	}
	
	/**
	 * Acquires the MBeanServerConnection server
	 * @return the MBeanServerConnection server
	 */
	public MBeanServerConnection server() {
		if(server==null) {
			synchronized(this) {
				if(server==null) {
					try {
						final Thread me = Thread.currentThread();
						final Timeout txout = TimeoutService.getInstance().timeout(connectTimeoutSecs, TimeUnit.SECONDS, new Runnable(){							
							@Override
							public void run() {
								log.warn("Connect Timeout !!!\n{}", StringHelper.formatStackTrace(me));
								me.interrupt();
								log.warn("JMXConnector interrupted after timeout");
							}
						});
//						if(hystrixEnabled.get()) {
//							commandBuilder.commandFor(new Callable<Object>(){
//								@Override
//								public Void call() throws Exception {
//									jmxConnector.connect();
//									return null;
//								}
//							}).execute();
//						} else {
							jmxConnector.connect();
//						}
						
//						server = jmxConnector.getMBeanServerConnection();
						final MBeanServerConnection conn = jmxConnector.getMBeanServerConnection();
						try {
							server = (MBeanServerConnection)Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[]{MBeanServerConnection.class}, new InvocationHandler(){
								@Override
								public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
									// FIXME: add a timeout watcher here.
									final long start = System.currentTimeMillis();
									try {
										return method.invoke(conn, args); 
									} finally {
										remotingTime.add(System.currentTimeMillis() - start);
									}
								}
							});
						} catch (Exception ex) {
							server = conn;
							log.warn("Failed to create Instrumented Proxy for JMX MBeanConnection Server [{}]. Continuing with native", this.jmxServiceUrl, ex);
						}
						txout.cancel();
						for(SavedNotificationEvent n: registrations) {							
							try {
								if(n.listener!=null) {
									server.addNotificationListener(n.name, n.listener, n.filter, n.handback);
								} else {
									server.addNotificationListener(n.name, n.listenerObjectName, n.filter, n.handback);
								}
							} catch (Exception ex) {
								registrations.remove(n);
								log.error("Failed to apply notif registration [" + n + "]", ex);
							}
						}
					} catch (Exception ex) {
						throw new RuntimeException("Failed to get MBeanServerConnection for [" + jmxServiceUrl + "]", ex);
					}
				}
			}
		}
		return server;
	}
	
	/**
	 * Returns the accumulated remoting time and resets the counter
	 * @return the accumulated remoting time  in ms.
	 */
	public long getJMXRemotingTime() {
		return remotingTime.sumThenReset();
	}

	/**
	 * @param className
	 * @param name
	 * @return
	 * @throws ReflectionException
	 * @throws InstanceAlreadyExistsException
	 * @throws MBeanRegistrationException
	 * @throws MBeanException
	 * @throws NotCompliantMBeanException
	 * @throws IOException
	 * @see javax.management.MBeanServerConnection#createMBean(java.lang.String, javax.management.ObjectName)
	 */
	public ObjectInstance createMBean(String className, ObjectName name)
			throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException,
			NotCompliantMBeanException, IOException {
		return server().createMBean(className, name);
	}

	/**
	 * @param className
	 * @param name
	 * @param loaderName
	 * @return
	 * @throws ReflectionException
	 * @throws InstanceAlreadyExistsException
	 * @throws MBeanRegistrationException
	 * @throws MBeanException
	 * @throws NotCompliantMBeanException
	 * @throws InstanceNotFoundException
	 * @throws IOException
	 * @see javax.management.MBeanServerConnection#createMBean(java.lang.String, javax.management.ObjectName, javax.management.ObjectName)
	 */
	public ObjectInstance createMBean(String className, ObjectName name, ObjectName loaderName)
			throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException,
			NotCompliantMBeanException, InstanceNotFoundException, IOException {
		return server().createMBean(className, name, loaderName);
	}

	/**
	 * @param className
	 * @param name
	 * @param params
	 * @param signature
	 * @return
	 * @throws ReflectionException
	 * @throws InstanceAlreadyExistsException
	 * @throws MBeanRegistrationException
	 * @throws MBeanException
	 * @throws NotCompliantMBeanException
	 * @throws IOException
	 * @see javax.management.MBeanServerConnection#createMBean(java.lang.String, javax.management.ObjectName, java.lang.Object[], java.lang.String[])
	 */
	public ObjectInstance createMBean(String className, ObjectName name, Object[] params, String[] signature)
			throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException,
			NotCompliantMBeanException, IOException {
		return server().createMBean(className, name, params, signature);
	}

	/**
	 * @param className
	 * @param name
	 * @param loaderName
	 * @param params
	 * @param signature
	 * @return
	 * @throws ReflectionException
	 * @throws InstanceAlreadyExistsException
	 * @throws MBeanRegistrationException
	 * @throws MBeanException
	 * @throws NotCompliantMBeanException
	 * @throws InstanceNotFoundException
	 * @throws IOException
	 * @see javax.management.MBeanServerConnection#createMBean(java.lang.String, javax.management.ObjectName, javax.management.ObjectName, java.lang.Object[], java.lang.String[])
	 */
	public ObjectInstance createMBean(String className, ObjectName name, ObjectName loaderName, Object[] params,
			String[] signature) throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException,
			MBeanException, NotCompliantMBeanException, InstanceNotFoundException, IOException {
		return server().createMBean(className, name, loaderName, params, signature);
	}

	/**
	 * @param name
	 * @throws InstanceNotFoundException
	 * @throws MBeanRegistrationException
	 * @throws IOException
	 * @see javax.management.MBeanServerConnection#unregisterMBean(javax.management.ObjectName)
	 */
	public void unregisterMBean(ObjectName name)
			throws InstanceNotFoundException, MBeanRegistrationException, IOException {
		server().unregisterMBean(name);
	}

	/**
	 * @param name
	 * @return
	 * @throws InstanceNotFoundException
	 * @throws IOException
	 * @see javax.management.MBeanServerConnection#getObjectInstance(javax.management.ObjectName)
	 */
	public ObjectInstance getObjectInstance(ObjectName name) throws InstanceNotFoundException, IOException {
		return server().getObjectInstance(name);
	}

	/**
	 * @param name
	 * @param query
	 * @return
	 * @throws IOException
	 * @see javax.management.MBeanServerConnection#queryMBeans(javax.management.ObjectName, javax.management.QueryExp)
	 */
	public Set<ObjectInstance> queryMBeans(ObjectName name, QueryExp query) throws IOException {
		return server().queryMBeans(name, query);
	}

	/**
	 * @param name
	 * @param query
	 * @return
	 * @throws IOException
	 * @see javax.management.MBeanServerConnection#queryNames(javax.management.ObjectName, javax.management.QueryExp)
	 */
	public Set<ObjectName> queryNames(ObjectName name, QueryExp query) throws IOException {
		return server().queryNames(name, query);
	}

	/**
	 * @param name
	 * @return
	 * @throws IOException
	 * @see javax.management.MBeanServerConnection#isRegistered(javax.management.ObjectName)
	 */
	public boolean isRegistered(ObjectName name) throws IOException {
		return server().isRegistered(name);
	}

	/**
	 * @return
	 * @throws IOException
	 * @see javax.management.MBeanServerConnection#getMBeanCount()
	 */
	public Integer getMBeanCount() throws IOException {
		return server().getMBeanCount();
	}

	/**
	 * @param name
	 * @param attribute
	 * @return
	 * @throws MBeanException
	 * @throws AttributeNotFoundException
	 * @throws InstanceNotFoundException
	 * @throws ReflectionException
	 * @throws IOException
	 * @see javax.management.MBeanServerConnection#getAttribute(javax.management.ObjectName, java.lang.String)
	 */
	public Object getAttribute(ObjectName name, String attribute) throws MBeanException, AttributeNotFoundException,
			InstanceNotFoundException, ReflectionException, IOException {
		return server().getAttribute(name, attribute);
	}

	/**
	 * @param name
	 * @param attributes
	 * @return
	 * @throws InstanceNotFoundException
	 * @throws ReflectionException
	 * @throws IOException
	 * @see javax.management.MBeanServerConnection#getAttributes(javax.management.ObjectName, java.lang.String[])
	 */
	public AttributeList getAttributes(final ObjectName name, final String...attributes)
			throws InstanceNotFoundException, ReflectionException, IOException {
		return server().getAttributes(name, attributes);
	}

	/**
	 * @param name
	 * @param attribute
	 * @throws InstanceNotFoundException
	 * @throws AttributeNotFoundException
	 * @throws InvalidAttributeValueException
	 * @throws MBeanException
	 * @throws ReflectionException
	 * @throws IOException
	 * @see javax.management.MBeanServerConnection#setAttribute(javax.management.ObjectName, javax.management.Attribute)
	 */
	public void setAttribute(ObjectName name, Attribute attribute)
			throws InstanceNotFoundException, AttributeNotFoundException, InvalidAttributeValueException,
			MBeanException, ReflectionException, IOException {
		server().setAttribute(name, attribute);
	}

	/**
	 * @param name
	 * @param attributes
	 * @return
	 * @throws InstanceNotFoundException
	 * @throws ReflectionException
	 * @throws IOException
	 * @see javax.management.MBeanServerConnection#setAttributes(javax.management.ObjectName, javax.management.AttributeList)
	 */
	public AttributeList setAttributes(ObjectName name, AttributeList attributes)
			throws InstanceNotFoundException, ReflectionException, IOException {
		return server().setAttributes(name, attributes);
	}

	/**
	 * @param name
	 * @param operationName
	 * @param params
	 * @param signature
	 * @return
	 * @throws InstanceNotFoundException
	 * @throws MBeanException
	 * @throws ReflectionException
	 * @throws IOException
	 * @see javax.management.MBeanServerConnection#invoke(javax.management.ObjectName, java.lang.String, java.lang.Object[], java.lang.String[])
	 */
	public Object invoke(ObjectName name, String operationName, Object[] params, String[] signature)
			throws InstanceNotFoundException, MBeanException, ReflectionException, IOException {
		return server().invoke(name, operationName, params, signature);
	}

	/**
	 * @return
	 * @throws IOException
	 * @see javax.management.MBeanServerConnection#getDefaultDomain()
	 */
	public String getDefaultDomain() throws IOException {
		return server().getDefaultDomain();
	}

	/**
	 * @return
	 * @throws IOException
	 * @see javax.management.MBeanServerConnection#getDomains()
	 */
	public String[] getDomains() throws IOException {
		return server().getDomains();
	}

	/**
	 * @param name
	 * @param listener
	 * @param filter
	 * @param handback
	 * @throws InstanceNotFoundException
	 * @throws IOException
	 * @see javax.management.MBeanServerConnection#addNotificationListener(javax.management.ObjectName, javax.management.NotificationListener, javax.management.NotificationFilter, java.lang.Object)
	 */
	public void addNotificationListener(ObjectName name, NotificationListener listener, NotificationFilter filter, Object handback) throws InstanceNotFoundException, IOException {
		server().addNotificationListener(name, listener, filter, handback);
		registrations.add(new SavedNotificationEvent(name, listener, filter, handback));
	}

	/**
	 * @param name
	 * @param listener
	 * @param filter
	 * @param handback
	 * @throws InstanceNotFoundException
	 * @throws IOException
	 * @see javax.management.MBeanServerConnection#addNotificationListener(javax.management.ObjectName, javax.management.ObjectName, javax.management.NotificationFilter, java.lang.Object)
	 */
	public void addNotificationListener(ObjectName name, ObjectName listener, NotificationFilter filter,
			Object handback) throws InstanceNotFoundException, IOException {
		server().addNotificationListener(name, listener, filter, handback);
		registrations.add(new SavedNotificationEvent(name, listener, filter, handback));
	}

	/**
	 * @param name
	 * @param listener
	 * @throws InstanceNotFoundException
	 * @throws ListenerNotFoundException
	 * @throws IOException
	 * @see javax.management.MBeanServerConnection#removeNotificationListener(javax.management.ObjectName, javax.management.ObjectName)
	 */
	public void removeNotificationListener(ObjectName name, ObjectName listener)
			throws InstanceNotFoundException, ListenerNotFoundException, IOException {
		server().removeNotificationListener(name, listener);
	}

	/**
	 * @param name
	 * @param listener
	 * @param filter
	 * @param handback
	 * @throws InstanceNotFoundException
	 * @throws ListenerNotFoundException
	 * @throws IOException
	 * @see javax.management.MBeanServerConnection#removeNotificationListener(javax.management.ObjectName, javax.management.ObjectName, javax.management.NotificationFilter, java.lang.Object)
	 */
	public void removeNotificationListener(ObjectName name, ObjectName listener, NotificationFilter filter,
			Object handback) throws InstanceNotFoundException, ListenerNotFoundException, IOException {
		server().removeNotificationListener(name, listener, filter, handback);
	}

	/**
	 * @param name
	 * @param listener
	 * @throws InstanceNotFoundException
	 * @throws ListenerNotFoundException
	 * @throws IOException
	 * @see javax.management.MBeanServerConnection#removeNotificationListener(javax.management.ObjectName, javax.management.NotificationListener)
	 */
	public void removeNotificationListener(ObjectName name, NotificationListener listener)
			throws InstanceNotFoundException, ListenerNotFoundException, IOException {
		server().removeNotificationListener(name, listener);
	}

	/**
	 * @param name
	 * @param listener
	 * @param filter
	 * @param handback
	 * @throws InstanceNotFoundException
	 * @throws ListenerNotFoundException
	 * @throws IOException
	 * @see javax.management.MBeanServerConnection#removeNotificationListener(javax.management.ObjectName, javax.management.NotificationListener, javax.management.NotificationFilter, java.lang.Object)
	 */
	public void removeNotificationListener(ObjectName name, NotificationListener listener, NotificationFilter filter,
			Object handback) throws InstanceNotFoundException, ListenerNotFoundException, IOException {
		server().removeNotificationListener(name, listener, filter, handback);
	}

	/**
	 * @param name
	 * @return
	 * @throws InstanceNotFoundException
	 * @throws IntrospectionException
	 * @throws ReflectionException
	 * @throws IOException
	 * @see javax.management.MBeanServerConnection#getMBeanInfo(javax.management.ObjectName)
	 */
	public MBeanInfo getMBeanInfo(ObjectName name)
			throws InstanceNotFoundException, IntrospectionException, ReflectionException, IOException {
		return server().getMBeanInfo(name);
	}

	/**
	 * @param name
	 * @param className
	 * @return
	 * @throws InstanceNotFoundException
	 * @throws IOException
	 * @see javax.management.MBeanServerConnection#isInstanceOf(javax.management.ObjectName, java.lang.String)
	 */
	public boolean isInstanceOf(ObjectName name, String className) throws InstanceNotFoundException, IOException {
		return server().isInstanceOf(name, className);
	}

	/**
	 * @param listener
	 * @param filter
	 * @param handback
	 * @see javax.management.remote.JMXConnector#addConnectionNotificationListener(javax.management.NotificationListener, javax.management.NotificationFilter, java.lang.Object)
	 */
	public void addConnectionNotificationListener(NotificationListener listener, NotificationFilter filter,
			Object handback) {
		jmxConnector.addConnectionNotificationListener(listener, filter, handback);
	}

	/**
	 * @param listener
	 * @throws ListenerNotFoundException
	 * @see javax.management.remote.JMXConnector#removeConnectionNotificationListener(javax.management.NotificationListener)
	 */
	public void removeConnectionNotificationListener(NotificationListener listener) throws ListenerNotFoundException {
		jmxConnector.removeConnectionNotificationListener(listener);
	}

	/**
	 * @param l
	 * @param f
	 * @param handback
	 * @throws ListenerNotFoundException
	 * @see javax.management.remote.JMXConnector#removeConnectionNotificationListener(javax.management.NotificationListener, javax.management.NotificationFilter, java.lang.Object)
	 */
	public void removeConnectionNotificationListener(NotificationListener l, NotificationFilter f, Object handback)
			throws ListenerNotFoundException {
		jmxConnector.removeConnectionNotificationListener(l, f, handback);
	}

	/**
	 * @return
	 * @throws IOException
	 * @see javax.management.remote.JMXConnector#getConnectionId()
	 */
	public String getConnectionId() throws IOException {
		return jmxConnector.getConnectionId();
	}
	
	public String getAppName() {
		if(remoteApp!=null) return remoteApp;
		return AgentName.remoteAppName(this);
	}
	public String getHostName() {
		if(remoteHost!=null) return remoteHost;
		return AgentName.remoteHostName(this);
	}
	
	
	private class SavedNotificationEvent {
		/** The object name to register the listener on */
		private final ObjectName name;
		/** The listener */		
		private final NotificationListener listener;
		/** The object name listener */		
		private final ObjectName listenerObjectName;

		/** The filter */
		private final NotificationFilter filter;
		/** The handback */
		private final Object handback;
		
		
		/**
		 * Creates a new SavedNotificationEvent
		 * @param name The object name to register the listener on 
		 * @param listener The listener
		 * @param filter The filter
		 * @param handback the handback
		 */
		public SavedNotificationEvent(final ObjectName name, final NotificationListener listener, final NotificationFilter filter, final Object handback) {
			this.name = name;
			this.listener = listener;
			this.filter = filter;
			this.handback = handback;
			this.listenerObjectName = null;
		}
		
		/**
		 * Creates a new SavedNotificationEvent
		 * @param name The object name to register the listener on 
		 * @param listener The object name of the listener
		 * @param filter The filter
		 * @param handback the handback
		 */
		public SavedNotificationEvent(final ObjectName name, final ObjectName listener, final NotificationFilter filter, final Object handback) {
			this.name = name;
			this.listenerObjectName = listener;
			this.filter = filter;
			this.handback = handback;
			this.listener = null;
		}

		/**
		 * {@inheritDoc}
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + ((filter == null) ? 0 : filter.hashCode());
			result = prime * result + ((handback == null) ? 0 : handback.hashCode());
			result = prime * result + ((listener == null) ? 0 : listener.hashCode());
			result = prime * result + ((listenerObjectName == null) ? 0 : listenerObjectName.hashCode());
			result = prime * result + ((name == null) ? 0 : name.hashCode());
			return result;
		}

		/**
		 * {@inheritDoc}
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			SavedNotificationEvent other = (SavedNotificationEvent) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (filter == null) {
				if (other.filter != null)
					return false;
			} else if (!filter.equals(other.filter))
				return false;
			if (handback == null) {
				if (other.handback != null)
					return false;
			} else if (!handback.equals(other.handback))
				return false;
			if (listener == null) {
				if (other.listener != null)
					return false;
			} else if (!listener.equals(other.listener))
				return false;
			if (listenerObjectName == null) {
				if (other.listenerObjectName != null)
					return false;
			} else if (!listenerObjectName.equals(other.listenerObjectName))
				return false;
			if (name == null) {
				if (other.name != null)
					return false;
			} else if (!name.equals(other.name))
				return false;
			return true;
		}

		private JMXClient getOuterType() {
			return JMXClient.this;
		}

		/**
		 * {@inheritDoc}
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("SavedNotificationEvent [\\n\\t");
			if (name != null)
				builder.append("name:[").append(name).append("] ");
			if (listener != null)
				builder.append("listener:[").append(listener).append("] ");
			if (listenerObjectName != null)
				builder.append("listenerObjectName:[").append(listenerObjectName).append("] ");
			if (filter != null)
				builder.append("filter:[").append(filter).append("] ");
			if (handback != null)
				builder.append("handback:[").append(handback);
			builder.append("\\n]");
			return builder.toString();
		}
		
	}
	
}
