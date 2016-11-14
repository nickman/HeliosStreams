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
package com.heliosapm.streams.collector.execution;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.management.ObjectName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.jmx.JMXManagedThreadPool;


/**
 * <p>Title: CollectorExecutionService</p>
 * <p>Description: A fork join pool for collection executions</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.execution.CollectorExecutionService</code></p>
 */

public class CollectorExecutionService implements UncaughtExceptionHandler, RejectedExecutionHandler {
	/** The singleton instance */
	private static volatile CollectorExecutionService instance;
	/** The singleton instance ctor lock */
	private static final Object lock = new Object();
	/** The fork join pool JMX ObjectName */
	public static final ObjectName OBJECT_NAME = JMXHelper.objectName("com.heliosapm.streams.collector:service=CollectionThreadPool");

	/**
	 * Acquires and returns the CollectorExecutionService singleton instance
	 * @return the CollectorExecutionService singleton instance
	 */
	public static CollectorExecutionService getInstance() {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					instance = new CollectorExecutionService();
				}
			}
		}
		return instance;
	}
	
	
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** The execution pool */
	private final JMXManagedThreadPool threadPool;

	
	
	private CollectorExecutionService() {
		threadPool =  JMXManagedThreadPool.builder()
			.corePoolSize(Runtime.getRuntime().availableProcessors() * 3)
			.maxPoolSize(Runtime.getRuntime().availableProcessors() * 6)
			.keepAliveTimeMs(60000)
			.objectName(OBJECT_NAME)
			.poolName(getClass().getSimpleName())
			.prestart(Runtime.getRuntime().availableProcessors() * 3)
			.publishJMX(true)
			.queueSize(128)
			//.rejectionHandler(new ThreadPoolExecutor.CallerRunsPolicy())
			.rejectionHandler(this)
			.uncaughtHandler(this)
			.build();
			
				//new ManagedForkJoinPool("JMXCollection", Runtime.getRuntime().availableProcessors() * 3, true, OBJECT_NAME);	
		
	}
	

	public void execute(final Runnable task) {
		threadPool.execute(task);
	}

	public <T> Future<T> submit(final Callable<T> task) {
		return threadPool.submit(task);
	}


	public Future<?> submit(final Runnable task) {
		return threadPool.submit(task);
	}

	public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks) throws InterruptedException {
		return threadPool.invokeAll(tasks);
	}

	public <T> T invokeAny(final Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
		return threadPool.invokeAny(tasks);
	}

	public <T> T invokeAny(final Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		return threadPool.invokeAny(tasks, timeout, unit);
	}

	public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks, final long timeout, final TimeUnit unit)
			throws InterruptedException {
		return threadPool.invokeAll(tasks, timeout, unit);
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Thread.UncaughtExceptionHandler#uncaughtException(java.lang.Thread, java.lang.Throwable)
	 */
	@Override
	public void uncaughtException(final Thread t, final Throwable e) {
		log.error("Uncaught exception in collection thread [{}}",  t, e);		
	}


	/**
	 * {@inheritDoc}
	 * @see java.util.concurrent.RejectedExecutionHandler#rejectedExecution(java.lang.Runnable, java.util.concurrent.ThreadPoolExecutor)
	 */
	@Override
	public void rejectedExecution(final Runnable r, final ThreadPoolExecutor executor) {
		log.error("\t !!! ---> CollectionThreadPool Task Rejected [{}]\n\tActive Count: [{}], Pool Size: [{}], Q Capacity: [{}]", 
				r, executor.getActiveCount(), executor.getPoolSize(), executor.getQueue().remainingCapacity());
	}
	
	

}
