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

import java.io.File;
import java.util.List;
import java.util.Map;
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
	
	/** Compilation event notification type prefix */
	public static String NOTIF_TYPE_COMPILATION_PREFIX = "collector.script.compilation.";
	/** Compilation event notification type prefix for script replacement */
	public static String NOTIF_TYPE_COMPILATION_REPLACEMENT_PREFIX = NOTIF_TYPE_COMPILATION_PREFIX + "replacement.";
	/** Compilation event notification type prefix for a new script */
	public static String NOTIF_TYPE_COMPILATION_NEW_PREFIX = NOTIF_TYPE_COMPILATION_PREFIX + "replacement.";
	
	
	/** Compilation event notification type for a failed script replacement */
	public static String NOTIF_TYPE_REPLACEMENT_FAILED = NOTIF_TYPE_COMPILATION_REPLACEMENT_PREFIX + "failed";
	/** Compilation event notification type for a successful script replacement */
	public static String NOTIF_TYPE_REPLACEMENT_COMPLETE = NOTIF_TYPE_COMPILATION_REPLACEMENT_PREFIX + "complete";
	/** Compilation event notification type for a new script deployment */
	public static String NOTIF_TYPE_NEW_SCRIPT = NOTIF_TYPE_COMPILATION_NEW_PREFIX + "complete";
	/** Compilation event notification type for a new script deployment failure */
	public static String NOTIF_TYPE_NEW_SCRIPT_FAIL = NOTIF_TYPE_COMPILATION_NEW_PREFIX + "failed";
	
	
	/**
	 * Compiles and deploys the script in the passed file
	 * @param source The file to compile the source from
	 * @return the script instance
	 */
	public ManagedScript compileScript(final File source);

	
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
	
	/**
	 * Returns the number of cached groovy classloaders
	 * @return the number of cached groovy classloaders
	 */
	public long getGroovyClassLoaderCount();
	
	/**
	 * Returns the number of managed scripts
	 * @return the number of managed scripts
	 */
	public long getManagedScriptCount();	
	
	/**
	 * Returns the script compiler's auto imports
	 * @return the script compiler's auto imports
	 */
	public Set<String> getAutoImports();
	
	/**
	 * Adds an auto import statement and returns the new config
	 * @param importStatement The import statement to add
	 * @return the new set after this op is invoked
	 */
	public Set<String> addAutoImport(final String importStatement);
	
	/**
	 * Removed an auto import statement and returns the new config
	 * @param importStatement The import statement to remove
	 * @return the new set after this op is invoked
	 */
	public Set<String> removeAutoImport(final String importStatement);
	
	/**
	 * Clears the auto imports
	 */
	public void clearAutoImports();
	
	/**
	 * Launches the swing groovy console
	 */
	public void launchConsole();

	
	/**
	 * Launches the swing groovy console and loads the passed file
	 * @param fileName the file to load
	 */
	public void launchConsole(final String fileName);
	
	/**
	 * @return
	 * @see org.codehaus.groovy.control.CompilerConfiguration#getWarningLevel()
	 */
	public int getWarningLevel();

	/**
	 * @param level
	 * @see org.codehaus.groovy.control.CompilerConfiguration#setWarningLevel(int)
	 */
	public void setWarningLevel(int level);

	/**
	 * @return
	 * @see org.codehaus.groovy.control.CompilerConfiguration#getSourceEncoding()
	 */
	public String getSourceEncoding();

	/**
	 * @return
	 * @see org.codehaus.groovy.control.CompilerConfiguration#getTargetDirectory()
	 */
	public File getTargetDirectory();

	/**
	 * @return
	 * @see org.codehaus.groovy.control.CompilerConfiguration#getClasspath()
	 */
	public List<String> getClasspath();

	/**
	 * @return
	 * @see org.codehaus.groovy.control.CompilerConfiguration#getVerbose()
	 */
	public boolean isVerbose();

	/**
	 * @param verbose
	 * @see org.codehaus.groovy.control.CompilerConfiguration#setVerbose(boolean)
	 */
	public void setVerbose(boolean verbose);

	/**
	 * @return
	 * @see org.codehaus.groovy.control.CompilerConfiguration#getDebug()
	 */
	public boolean isDebug();

	/**
	 * @param debug
	 * @see org.codehaus.groovy.control.CompilerConfiguration#setDebug(boolean)
	 */
	public void setDebug(boolean debug);

	/**
	 * @return
	 * @see org.codehaus.groovy.control.CompilerConfiguration#getTolerance()
	 */
	public int getTolerance();

	/**
	 * @param tolerance
	 * @see org.codehaus.groovy.control.CompilerConfiguration#setTolerance(int)
	 */
	public void setTolerance(int tolerance);


	/**
	 * @return
	 * @see org.codehaus.groovy.control.CompilerConfiguration#getTargetBytecode()
	 */
	public String getTargetBytecode();

	/**
	 * @return
	 * @see org.codehaus.groovy.control.CompilerConfiguration#getOptimizationOptions()
	 */
	public Map<String, Boolean> getOptimizationOptions();
	
	/**
	 * Returns the expected ManagedScript ObjectName for the passed source file
	 * @param sourceName The source file location starting below the root script directory
	 * @return the expected ObjectName
	 */
	public ObjectName sourceNameToObjectName(final String sourceName);

}
