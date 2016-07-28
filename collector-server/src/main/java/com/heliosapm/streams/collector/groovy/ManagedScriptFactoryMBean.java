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

import java.util.Set;

import javax.management.ObjectName;

import com.heliosapm.utils.jmx.JMXHelper;

/**
 * <p>Title: ManagedScriptFactoryMBean</p>
 * <p>Description: JMX MBean interface for the {@link ManagedScriptFactory} instance</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.groovy.ManagedScriptFactoryMBean</code></p>
 */

public interface ManagedScriptFactoryMBean {
	
	/** The ManagedScriptFactory MBean ObjectName */
	public static final ObjectName OBJECT_NAME = JMXHelper.objectName("com.heliosapm.streams.collector:service=ManagedScriptFactory");
	
	/**
	 * Returns the cummulative number of successful compilations
	 * @return the cummulative number of successful compilations
	 */
	public long getSuccessfulCompileCount();
	
	/**
	 * Returns the cummulative number of failed compilations
	 * @return the cummulative number of failed compilations
	 */
	public long getFailedCompileCount();
	
	/**
	 * Returns the names of successfully compiled scripts
	 * @return the names of successfully compiled scripts
	 */
	public Set<String> getCompiledScripts();
	
	/**
	 * Returns the names of scripts that failed compilation
	 * @return the names of scripts that failed compilation
	 */
	public Set<String> getFailedScripts();
	
	/**
	 * Returns the number of successfully compiled scripts
	 * @return the number of successfully compiled scripts
	 */
	public int getCompiledScriptCount();

	/**
	 * Returns the number of scripts that failed compilation
	 * @return the number of scripts that failed compilation
	 */
	public int getFailedScriptCount();

}
