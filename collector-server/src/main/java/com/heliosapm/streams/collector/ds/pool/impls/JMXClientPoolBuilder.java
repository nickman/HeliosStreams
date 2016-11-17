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
package com.heliosapm.streams.collector.ds.pool.impls;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXServiceURL;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;

import com.heliosapm.streams.collector.ds.pool.PoolAwareFactory;
import com.heliosapm.streams.collector.ds.pool.PooledObjectFactoryBuilder;
import com.heliosapm.streams.collector.jmx.JMXClient;
import com.heliosapm.utils.lang.StringHelper;

/**
 * <p>Title: JMXClientPoolBuilder</p>
 * <p>Description: A pool supplier for JMXConnector MBeanServerConnections</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.ds.pool.impls.JMXClientPoolBuilder</code></p>
 */

public class JMXClientPoolBuilder implements PooledObjectFactoryBuilder<MBeanServerConnection>, PoolAwareFactory<MBeanServerConnection> {
	/** The JMXServiceURL */
	protected final JMXServiceURL url;
	/** The optional JMX authentication credentials */
	protected final String[] credentials;
	/** The environment map */
	protected final Map<String, Object> env = new HashMap<String, Object>();
	
	/** The pool using this factory */
	protected GenericObjectPool<MBeanServerConnection> pool = null;
	
	/** The JMX connector service url config key */
	public static final String JMX_URL_KEY = "jmx.serviceurl";
	/** The JMX connector user name config key */
	public static final String JMX_USER_KEY = "jmx.username";
	/** The JMX connector password config key */
	public static final String JMX_PW_KEY = "jmx.password";
	/** The JMX connector environment map entry config prefix */
	public static final String JMX_ENVMAP_PREFIX = "jmx.env.";

	/**
	 * Creates a new JMXClientPoolBuilder
	 * @param config The configuration properties
	 */
	public JMXClientPoolBuilder(final Properties config) {
		final String jmxUrl = config.getProperty(JMX_URL_KEY, "").trim();
		if(jmxUrl.isEmpty()) throw new IllegalArgumentException("The passed JMXServiceURL was null or empty");
		try {
			url = new JMXServiceURL(jmxUrl);
		} catch (Exception ex) {
			throw new IllegalArgumentException("The passed JMXServiceURL was invalid", ex);
		}
		final String user = config.getProperty(JMX_USER_KEY, "").trim();
		final String password = config.getProperty(JMX_PW_KEY, "").trim();
		if(!user.isEmpty() && !password.isEmpty()) {
			credentials = new String[]{user, password};
			env.put(JMXConnector.CREDENTIALS, credentials);
		} else {
			credentials = null;
		}
		if(config.size()>1) {
			for(final String key: config.stringPropertyNames()) {
				final String _key = key.trim();
				if(_key.startsWith(JMX_ENVMAP_PREFIX)) {
					final String _envKey = _key.replace(JMX_ENVMAP_PREFIX, "");
					if(_envKey.isEmpty()) continue;
					final String _rawValue = config.getProperty(key, "").trim();
					if(_rawValue.isEmpty()) continue;
					final Object _convertedValue = StringHelper.convertTyped(_rawValue);
					env.put(_envKey, _convertedValue);
				}
			}
		}
	}
	
//	public static void main(String[] args) {
//		log("JMXConnector Pool Test");
//		final Properties p = new Properties();
//		p.setProperty(JMX_URL_KEY, "service:jmx:jmxmp://0.0.0.0:1423");
//		JMXClientPoolBuilder poolBuilder = new JMXClientPoolBuilder(p);
//		PooledObject<MBeanServerConnection> pooledObject = null;
//		WrappedMBeanServerConnection o = null;
//		try {
//			
//			o = (WrappedMBeanServerConnection)poolBuilder.create();
//			log("Created Object: [" + o + "]");
//			pooledObject = new DefaultPooledObject<MBeanServerConnection>(o);
//			log("Validating Object....");
//			poolBuilder.validateObject(pooledObject);
//			//SystemClock.sleep(100000);
//			log("Done");
//			
//		} finally {
//			if(pooledObject!=null) try { poolBuilder.destroyObject(pooledObject); } catch (Exception x) {}
//		}
//	}
//	
//	public static void log(final Object msg) {
//		System.out.println(msg);
//	}
	
	
	
	@Override
	public void setPool(final GenericObjectPool<MBeanServerConnection> pool) {
		this.pool = pool;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.ds.pool.PooledObjectFactoryBuilder#create()
	 */
	@SuppressWarnings("resource")
	@Override
	public MBeanServerConnection create() {
		try {
			return new WrappedJMXClient(url.toString(), 10, credentials).server();
//			final JMXConnector connector = JMXConnectorFactory.connect(url, env);
//			return new WrappedMBeanServerConnection(connector);
		} catch (Exception ex) {
			throw new RuntimeException("Failed to create JMXConnector for [" + url + "]", ex);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.ds.pool.PooledObjectFactoryBuilder#destroyObject(org.apache.commons.pool2.PooledObject)
	 */
	@Override
	public void destroyObject(final PooledObject<MBeanServerConnection> p) {
		final WrappedJMXClient wcon = (WrappedJMXClient)p.getObject();
		if(wcon!=null) {
			try {
				wcon.realClose();
			} catch (Exception x) {/* No Op */}
		}
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.ds.pool.PooledObjectFactoryBuilder#validateObject(org.apache.commons.pool2.PooledObject)
	 */
	@Override
	public boolean validateObject(final PooledObject<MBeanServerConnection> p) {
		final MBeanServerConnection wcon = p.getObject();
		if(wcon==null) return false;
		try {
			final String[] domains = wcon.getDomains();
			return domains!=null && domains.length > 0;
		}  catch (Exception x) {
			return false;
		}
	}
	
	/**
	 * <p>Title: WrappedJMXClient</p>
	 * <p>Description: A wrapped JMXClient such that calling {@link JMXClient#close()} will return the client to the pool</p> 
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>com.heliosapm.streams.collector.ds.pool.impls.WrappedJMXClient</code></p>
	 */
	private class WrappedJMXClient extends JMXClient {

		private WrappedJMXClient(final String jmxUrl, final long connectTimeoutSecs, final String...credentials) {
			super(jmxUrl, connectTimeoutSecs, credentials);			
		}
		
		/**
		 * Returns this client to the pool.
		 * {@inheritDoc}
		 * @see com.heliosapm.streams.collector.jmx.JMXClient#close()
		 */
		@Override
		public void close() throws IOException {			
			pool.returnObject(this);
		}
		
		/**
		 * Really closes the jmx client
		 * @throws IOException  Will not be thrown
		 */
		private void realClose() throws IOException {
			super.close();
		}
		
	}

//	private class WrappedMBeanServerConnection implements MBeanServerConnection, Closeable {
//		private final MBeanServerConnection server;
//		private final JMXConnector conn;
//		
//		private WrappedMBeanServerConnection(final JMXConnector conn) {			
//			this.conn = conn;
//			try {
//				this.server = conn.getMBeanServerConnection();
//			} catch (Exception ex) {
//				throw new RuntimeException("Failed to get MBeanServerConnection from JMXConnector [" + conn + "]", ex);
//			}
//		}
//		
//		public void close() throws IOException {
//			pool.returnObject(this);
//		}
//		
//		public ObjectInstance createMBean(String className, ObjectName name)
//				throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException,
//				NotCompliantMBeanException, IOException {
//			return server.createMBean(className, name);
//		}
//
//		public ObjectInstance createMBean(String className, ObjectName name, ObjectName loaderName)
//				throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException,
//				NotCompliantMBeanException, InstanceNotFoundException, IOException {
//			return server.createMBean(className, name, loaderName);
//		}
//
//		public ObjectInstance createMBean(String className, ObjectName name, Object[] params, String[] signature)
//				throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException,
//				NotCompliantMBeanException, IOException {
//			return server.createMBean(className, name, params, signature);
//		}
//
//		public ObjectInstance createMBean(String className, ObjectName name, ObjectName loaderName, Object[] params,
//				String[] signature)
//				throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException,
//				NotCompliantMBeanException, InstanceNotFoundException, IOException {
//			return server.createMBean(className, name, loaderName, params, signature);
//		}
//
//		public void unregisterMBean(ObjectName name)
//				throws InstanceNotFoundException, MBeanRegistrationException, IOException {
//			server.unregisterMBean(name);
//		}
//
//		public ObjectInstance getObjectInstance(ObjectName name) throws InstanceNotFoundException, IOException {
//			return server.getObjectInstance(name);
//		}
//
//		public Set<ObjectInstance> queryMBeans(ObjectName name, QueryExp query) throws IOException {
//			return server.queryMBeans(name, query);
//		}
//
//		public Set<ObjectName> queryNames(ObjectName name, QueryExp query) throws IOException {
//			return server.queryNames(name, query);
//		}
//
//		public boolean isRegistered(ObjectName name) throws IOException {
//			return server.isRegistered(name);
//		}
//
//		public Integer getMBeanCount() throws IOException {
//			return server.getMBeanCount();
//		}
//
//		public Object getAttribute(ObjectName name, String attribute) throws MBeanException, AttributeNotFoundException,
//				InstanceNotFoundException, ReflectionException, IOException {
//			return server.getAttribute(name, attribute);
//		}
//
//		public AttributeList getAttributes(ObjectName name, String[] attributes)
//				throws InstanceNotFoundException, ReflectionException, IOException {
//			return server.getAttributes(name, attributes);
//		}
//
//		public void setAttribute(ObjectName name, Attribute attribute)
//				throws InstanceNotFoundException, AttributeNotFoundException, InvalidAttributeValueException,
//				MBeanException, ReflectionException, IOException {
//			server.setAttribute(name, attribute);
//		}
//
//		public AttributeList setAttributes(ObjectName name, AttributeList attributes)
//				throws InstanceNotFoundException, ReflectionException, IOException {
//			return server.setAttributes(name, attributes);
//		}
//
//		public Object invoke(ObjectName name, String operationName, Object[] params, String[] signature)
//				throws InstanceNotFoundException, MBeanException, ReflectionException, IOException {
//			return server.invoke(name, operationName, params, signature);
//		}
//
//		public String getDefaultDomain() throws IOException {
//			return server.getDefaultDomain();
//		}
//
//		public String[] getDomains() throws IOException {
//			return server.getDomains();
//		}
//
//		public void addNotificationListener(ObjectName name, NotificationListener listener, NotificationFilter filter,
//				Object handback) throws InstanceNotFoundException, IOException {
//			server.addNotificationListener(name, listener, filter, handback);
//		}
//
//		public void addNotificationListener(ObjectName name, ObjectName listener, NotificationFilter filter,
//				Object handback) throws InstanceNotFoundException, IOException {
//			server.addNotificationListener(name, listener, filter, handback);
//		}
//
//		public void removeNotificationListener(ObjectName name, ObjectName listener)
//				throws InstanceNotFoundException, ListenerNotFoundException, IOException {
//			server.removeNotificationListener(name, listener);
//		}
//
//		public void removeNotificationListener(ObjectName name, ObjectName listener, NotificationFilter filter,
//				Object handback) throws InstanceNotFoundException, ListenerNotFoundException, IOException {
//			server.removeNotificationListener(name, listener, filter, handback);
//		}
//
//		public void removeNotificationListener(ObjectName name, NotificationListener listener)
//				throws InstanceNotFoundException, ListenerNotFoundException, IOException {
//			server.removeNotificationListener(name, listener);
//		}
//
//		public void removeNotificationListener(ObjectName name, NotificationListener listener,
//				NotificationFilter filter, Object handback)
//				throws InstanceNotFoundException, ListenerNotFoundException, IOException {
//			server.removeNotificationListener(name, listener, filter, handback);
//		}
//
//		public MBeanInfo getMBeanInfo(ObjectName name)
//				throws InstanceNotFoundException, IntrospectionException, ReflectionException, IOException {
//			return server.getMBeanInfo(name);
//		}
//
//		public boolean isInstanceOf(ObjectName name, String className) throws InstanceNotFoundException, IOException {
//			return server.isInstanceOf(name, className);
//		}
//		
//	}
	
}
