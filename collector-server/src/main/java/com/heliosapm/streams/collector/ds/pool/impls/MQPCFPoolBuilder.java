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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Properties;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import com.heliosapm.streams.collector.ds.pool.PooledObjectFactoryBuilder;

/**
 * <p>Title: MQPCFPoolBuilder</p>
 * <p>Description: A reflective pool supplier for IBM MQ PCF Message Agents</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.ds.pool.impls.MQPCFPoolBuilder</code></p>
 */

public class MQPCFPoolBuilder implements PooledObjectFactoryBuilder<Object> {
	/** The MQ Server host name */
	protected final String host;
	/** The MQ Server channel name */
	protected final String channel;
	/** The MQ Server listening port */
	protected final int port;
	
	/** The MQ Server Host name config key */
	public static final String HOST_KEY = "mq.host";
	/** The MQ Server Channel name config key */
	public static final String CHANNEL_KEY = "mq.channel";
	/** The MQ Server port number config key */
	public static final String PORT_KEY = "mq.port";
	
	// new PCFMessageAgent(host, port, channel);

	private static final MethodHandles.Lookup lookup = MethodHandles.lookup();
	
	/** The class name of the PCF Message Agent */
	public static final String PCF_CLASS_NAME = "com.ibm.mq.pcf.PCFMessageAgent";	
	/** The PCF Message Agent class */
	public static final Class<?> PCF_CLASS;
	/** The PCF Message Agent class ctor */
	public static final MethodHandle PCF_CTOR;
	/** The PCF Message Agent closer method */
	public static final MethodHandle PCF_CLOSER;
	/** The PCF Message Agent validator method */
	public static final MethodHandle PCF_VALIDATE;
	
	static {
		try {
			PCF_CLASS = Class.forName(PCF_CLASS_NAME);
			final MethodHandle ctorMh = lookup.findConstructor(PCF_CLASS, MethodType.methodType(void.class, String.class, int.class, String.class));
			PCF_CTOR = ctorMh.asType(ctorMh.type().changeReturnType(Object.class));
			final MethodHandle closeMh = lookup.findVirtual(PCF_CLASS, "disconnect", MethodType.methodType(void.class));
			PCF_CLOSER = closeMh.asType(closeMh.type().changeParameterType(0, Object.class));
			final MethodHandle validateMh = lookup.findVirtual(PCF_CLASS, "getQManagerName", MethodType.methodType(String.class));
			PCF_VALIDATE = validateMh.asType(validateMh.type().changeParameterType(0, Object.class));			
		} catch (Exception ex) {
			throw new RuntimeException("Failed to initialize MQPCFPoolBuilder", ex);
		}
	}
	
	public static void main(String[] args) {
		log("PCFMessageAgent Test");
		final Properties p = new Properties();
		p.setProperty(HOST_KEY, "localhost");
		p.setProperty(PORT_KEY, "1430");
		p.setProperty(CHANNEL_KEY, "JBOSS.SVRCONN");
		MQPCFPoolBuilder poolBuilder = new MQPCFPoolBuilder(p);
		log("Created");
		log("PCF_CTOR: [" + PCF_CTOR + "]");
		Object o = null;
		PooledObject<Object> pooledObject = null;
		try {
			o = poolBuilder.create();
			log("Created Object: [" + o + "]");
			pooledObject = new DefaultPooledObject<Object>(o);
			log("Validating Object....");
			poolBuilder.validateObject(pooledObject);
			//SystemClock.sleep(100000);
			
		} finally {
			if(pooledObject!=null) try { poolBuilder.destroyObject(pooledObject); } catch (Exception x) {}
		}
		
		
	}
	
	public static void log(final Object msg) {
		System.out.println(msg);
	}
	
	
	
	/**
	 * Creates a new MQPCFPoolBuilder
	 * @param config The configuration properties
	 */
	public MQPCFPoolBuilder(final Properties config) {
		host = config.getProperty(HOST_KEY).trim();
		channel = config.getProperty(CHANNEL_KEY).trim();
		port = Integer.parseInt(config.getProperty(PORT_KEY).trim());
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.ds.pool.PooledObjectFactoryBuilder#create()
	 */
	@Override
	public Object create() {
		try {
			return PCF_CTOR.invoke(host, port, channel);
		} catch (Throwable e) {
			throw new RuntimeException("Failed to create PCFMessageAgent for [" + host + "/" + port + "/" + channel + "]", e);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.ds.pool.PooledObjectFactoryBuilder#destroyObject(org.apache.commons.pool2.PooledObject)
	 */
	@Override
	public void destroyObject(final PooledObject<Object> p) {
		try {
			PCF_CLOSER.invokeExact(p.getObject());
			log("Closed OK");
		} catch (Throwable e) {
			e.printStackTrace(System.err);
			/* No Op */
		}		
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.collector.ds.pool.PooledObjectFactoryBuilder#validateObject(org.apache.commons.pool2.PooledObject)
	 */
	@Override
	public boolean validateObject(PooledObject<Object> p) {
		String qManagerName = null;
		try {
			qManagerName = (String)PCF_VALIDATE.invokeExact(p.getObject());			
			return (qManagerName!=null && !qManagerName.isEmpty());
		} catch (Throwable e) {
			e.printStackTrace(System.err);
			return false;
		}		
	}

}
