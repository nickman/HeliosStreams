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
package com.heliosapm.streams.tracing;

import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.heliosapm.streams.tracing.writers.LoggingWriter;
<<<<<<< HEAD
import com.heliosapm.utils.concurrency.ExtendedThreadManager;
=======
import com.heliosapm.streams.tracing.writers.NetWriter;
>>>>>>> 0e6207fa0085250ab5c78b59e10c804ed5608490
import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.reflect.PrivateAccessor;

/**
 * <p>Title: TracerFactory</p>
 * <p>Description: A factory for configured tracers</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tracing.TracerFactory</code></p>
 */

public class TracerFactory {
	/** The singleton instance */
	private static volatile TracerFactory instance = null;
	/** The singleton instance ctor lock */
	private static final Object lock = new Object();
	
	/** The config key name for the writer class name */
	public static final String CONFIG_WRITER_CLASS = "tracing.writer.class";
	/** The default writer class name */
	public static final String DEFAULT_WRITER_CLASS = LoggingWriter.class.getName();
	
	
	/** A cache of {@link ITracer}s keyed by the thread that owns the tracer */
	private final Cache<Thread, ITracer> threadTracers = CacheBuilder.newBuilder()
		.concurrencyLevel(Runtime.getRuntime().availableProcessors())
		.initialCapacity(Runtime.getRuntime().availableProcessors() * 4)
		.recordStats()
		.weakKeys()
		.build();
	
	/** The configured writer */
	private final IMetricWriter writer;
	
	/**
	 * Acquires and returns the singleton instance
	 * @param config The tracer factory configuration properties 
	 * @return the singleton instance
	 */
	public static TracerFactory getInstance(final Properties config) {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					instance = new TracerFactory(config);
				}
			}
		}
		return instance;
	}
	
	/**
	 * Acquires and returns the singleton instance
	 * @return the singleton instance
	 */
	public static TracerFactory getInstance() {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					throw new IllegalStateException("The tracer factory has not been initialized yet");
				}
			}
		}
		return instance;
	}
	
	
	private TracerFactory(final Properties config) {
		try {
			final String writerClassName = ConfigurationHelper.getSystemThenEnvProperty(CONFIG_WRITER_CLASS, DEFAULT_WRITER_CLASS, config);
			writer = (IMetricWriter)PrivateAccessor.createNewInstance(Class.forName(writerClassName), new Object[0]);
			writer.configure(config);
			writer.start();
			writer.awaitRunning(10, TimeUnit.SECONDS);
			
		} catch (Exception ex) {
			throw new IllegalArgumentException("Failed to configure TracerFactory", ex);
		}
	}
	
	
	/**
	 * Returns a tracer for the calling thread.
	 * Tracers are cached so after the first call, this should be quick.
	 * @return a tracer for the calling thread
	 */
	public ITracer getTracer() {
		try {
			return threadTracers.get(Thread.currentThread(), new Callable<ITracer>(){
				@Override
				public ITracer call() throws Exception {				
					return new DefaultTracerImpl(writer);
				}
			});
		} catch (Exception ex) {
			throw new RuntimeException("Failed to create an ITracer", ex);
		}
	}
	
	public static void main(String[] args) {
		log("Tracer Test");
		JMXHelper.fireUpJMXMPServer(2553);
		ExtendedThreadManager.install();
		System.setProperty(CONFIG_WRITER_CLASS, "com.heliosapm.streams.tracing.writers.TelnetWriter");
		System.setProperty(NetWriter.CONFIG_REMOTE_URIS, "localhost:3333");
		ITracer tracer = TracerFactory.getInstance(null).getTracer();
		tracer.seg("foo.bar");
		for(int x = 0; x < 1000; x++) {
			for(int i = 0; i < 1000; i++) {
				tracer.trace(i, System.currentTimeMillis());
			}
			tracer.flush();
		}
		log("Done");
		StdInCommandHandler.getInstance().run();
	}
	
	public static void log(Object msg) {
		System.out.println(msg);
	}
	
}
