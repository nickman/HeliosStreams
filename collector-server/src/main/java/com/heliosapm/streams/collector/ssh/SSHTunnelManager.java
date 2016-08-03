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
package com.heliosapm.streams.collector.ssh;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * <p>Title: SSHTunnelManager</p>
 * <p>Description: Deployment and management of SSH port tunnels</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.ssh.SSHTunnelManager</code></p>
 */

public class SSHTunnelManager {
	/** The singleton instance */
	private static volatile SSHTunnelManager instance = null;
	/** The singleton instance ctor lock */
	private static final Object lock = new Object();
	
	
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	
	/**
	 * Acquires and returns the SSHTunnelManager singleton instance
	 * @return the SSHTunnelManager singleton instance
	 */
	public static SSHTunnelManager getInstance() {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					instance = new SSHTunnelManager();					
				}
			}
		}
		return instance;
	}
	
	/**
	 * Creates a new SSHTunnelManager
	 */
	private SSHTunnelManager() {
		
	}

}
