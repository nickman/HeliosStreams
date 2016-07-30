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
package com.heliosapm.streams.tracing.deltas;

import javax.management.ObjectName;
import com.heliosapm.utils.jmx.JMXHelper;

/**
 * <p>Title: DeltaManagerMBean</p>
 * <p>Description: JMX MBean interface for {@link DeltaManager}</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tracing.deltas.DeltaManagerMBean</code></p>
 */

public interface DeltaManagerMBean {
	/** The delta service's JMX ObjectName */
	public static final ObjectName OBJECT_NAME = JMXHelper.objectName(DeltaManager.class);
	
	/**
	 * Returns the number of long deltas being tracked
	 * @return the number of long deltas being tracked
	 */
	public int getLongDeltaSize();

	/**
	 * Returns the number of double deltas being tracked
	 * @return the number of double deltas being tracked
	 */
	public int getDoubleDeltaSize();
	
	/**
	 * Returns the number of int deltas being tracked
	 * @return the number of int deltas being tracked
	 */
	public int getIntDeltaSize();
	
	/**
	 * Compacts all the delta series
	 */
	public void compact();

}

