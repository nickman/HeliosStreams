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
package com.heliosapm.streams.collector;

import javax.management.ObjectName;

import com.heliosapm.utils.jmx.JMXHelper;

/**
 * <p>Title: TimeoutServiceMBean</p>
 * <p>Description: JMX MBean for the {@link TimeoutService} instance</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.TimeoutServiceMBean</code></p>
 */

public interface TimeoutServiceMBean {
	/** The JMX ObjectName for the timeoutr service MBean */
	public static final ObjectName OBJECT_NAME = JMXHelper.objectName("com.heliosapm.streams:service=TimeoutService");
	
	/**
	 * Returns the total number of completions
	 * @return the total number of completions
	 */
	public long getCancellations();
	
	/**
	 * Returns the total number of timeouts
	 * @return the total number of timeouts
	 */
	public long getTimeouts();
	
	/**
	 * Returns the approximate number of active timeout threads
	 * @return the number of active timeout threads
	 */
	public int getActiveThreads();
	
	/**
	 * Returns the number of pending timeouts
	 * @return the number of pending timeouts
	 */
	public int getPending();
	
}
