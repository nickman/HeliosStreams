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
package com.heliosapm.webrpc;

import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.spec.Reactors;
import reactor.event.dispatch.Dispatcher;

/**
 * <p>Title: SingletonEnvironment</p>
 * <p>Description: Singleton for the reactor Env</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.webrpc.SingletonEnvironment</code></p>
 */

public class SingletonEnvironment {
	/** The singleton instance */
	private static volatile SingletonEnvironment instance = null;
	/** The singleton instance ctor lock */
	private static final Object lock = new Object();	
	/** The environment */
	private final Environment env;
	/** The default reactor */
	private final Reactor defaultReactor;
	
	
	public static SingletonEnvironment getInstance() {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					instance = new SingletonEnvironment();
				}
			}
		}
		return instance;
	}
	
	
	private SingletonEnvironment() {
		env = new Environment();
		defaultReactor = Reactors.reactor(env);
	}
	
	public Reactor getDefaultReactor() {
		return defaultReactor;
	}
	
	public Dispatcher getDefaultAsyncDispatcher() {
		return defaultReactor.getDispatcher();
	}


	/**
	 * Returns the 
	 * @return the env
	 */
	public Environment getEnv() {
		return env;
	}
}
