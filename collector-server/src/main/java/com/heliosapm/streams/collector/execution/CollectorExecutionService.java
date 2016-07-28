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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.management.ObjectName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.jmx.ManagedForkJoinPool;


import jsr166y.ForkJoinTask;


/**
 * <p>Title: CollectorExecutionService</p>
 * <p>Description: A fork join pool for collection executions</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.execution.CollectorExecutionService</code></p>
 */

public class CollectorExecutionService {
	/** The singleton instance */
	private static volatile CollectorExecutionService instance;
	/** The singleton instance ctor lock */
	private static final Object lock = new Object();
	/** The fork join pool JMX ObjectName */
	public static final ObjectName OBJECT_NAME = JMXHelper.objectName("com.heliosapm.streams.collector:service=CollectionForkJoinPool");

	
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
	private final ManagedForkJoinPool forkJoinPool;

	
	
	private CollectorExecutionService() {
		forkJoinPool = new ManagedForkJoinPool("JMXCollection", Runtime.getRuntime().availableProcessors() * 3, true, OBJECT_NAME);	
		
	}
	
	/**
	 * @param task
	 * @return
	 * @see com.heliosapm.utils.jmx.ManagedForkJoinPool#invoke(jsr166y.ForkJoinTask)
	 */
	public <T> T invoke(final ForkJoinTask<T> task) {
		return forkJoinPool.invoke(task);
	}

	/**
	 * @param task
	 * @see com.heliosapm.utils.jmx.ManagedForkJoinPool#execute(jsr166y.ForkJoinTask)
	 */
	public void execute(final ForkJoinTask<?> task) {
		forkJoinPool.execute(task);
	}

	/**
	 * @param task
	 * @see com.heliosapm.utils.jmx.ManagedForkJoinPool#execute(java.lang.Runnable)
	 */
	public void execute(final Runnable task) {
		forkJoinPool.execute(task);
	}

	/**
	 * @param task
	 * @return
	 * @see com.heliosapm.utils.jmx.ManagedForkJoinPool#submit(jsr166y.ForkJoinTask)
	 */
	public <T> ForkJoinTask<T> submit(final ForkJoinTask<T> task) {
		return forkJoinPool.submit(task);
	}

	/**
	 * @param task
	 * @return
	 * @see com.heliosapm.utils.jmx.ManagedForkJoinPool#submit(java.util.concurrent.Callable)
	 */
	public <T> ForkJoinTask<T> submit(final Callable<T> task) {
		return forkJoinPool.submit(task);
	}

	/**
	 * @param task
	 * @param result
	 * @return
	 * @see com.heliosapm.utils.jmx.ManagedForkJoinPool#submit(java.lang.Runnable, java.lang.Object)
	 */
	public <T> ForkJoinTask<T> submit(final Runnable task, final T result) {
		return forkJoinPool.submit(task, result);
	}

	/**
	 * @param task
	 * @return
	 * @see com.heliosapm.utils.jmx.ManagedForkJoinPool#submit(java.lang.Runnable)
	 */
	public ForkJoinTask<?> submit(final Runnable task) {
		return forkJoinPool.submit(task);
	}

	/**
	 * @param tasks
	 * @return
	 * @see com.heliosapm.utils.jmx.ManagedForkJoinPool#invokeAll(java.util.Collection)
	 */
	public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks) {
		return forkJoinPool.invokeAll(tasks);
	}

	/**
	 * @param tasks
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @see com.heliosapm.utils.jmx.ManagedForkJoinPool#invokeAny(java.util.Collection)
	 */
	public <T> T invokeAny(final Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
		return forkJoinPool.invokeAny(tasks);
	}

	/**
	 * @param tasks
	 * @param timeout
	 * @param unit
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws TimeoutException
	 * @see com.heliosapm.utils.jmx.ManagedForkJoinPool#invokeAny(java.util.Collection, long, java.util.concurrent.TimeUnit)
	 */
	public <T> T invokeAny(final Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		return forkJoinPool.invokeAny(tasks, timeout, unit);
	}

	/**
	 * @param tasks
	 * @param timeout
	 * @param unit
	 * @return
	 * @throws InterruptedException
	 * @see com.heliosapm.utils.jmx.ManagedForkJoinPool#invokeAll(java.util.Collection, long, java.util.concurrent.TimeUnit)
	 */
	public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks, final long timeout, final TimeUnit unit)
			throws InterruptedException {
		return forkJoinPool.invokeAll(tasks, timeout, unit);
	}
	
	

}
