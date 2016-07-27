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

import groovy.lang.Binding;
import groovy.lang.Script;

/**
 * <p>Title: ManagedScript</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.groovy.ManagedScript</code></p>
 */

public abstract class ManagedScript extends Script {

	/**
	 * Creates a new ManagedScript
	 */
	public ManagedScript() {
		
	}

	/**
	 * Creates a new ManagedScript
	 * @param binding The script bindings
	 */
	public ManagedScript(Binding binding) {
		super(binding);
	}


}
