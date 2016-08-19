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
package com.heliosapm.streams.hystrix;

import java.util.concurrent.Callable;

import com.netflix.hystrix.HystrixCommand;

/**
 * <p>Title: HystrixCommandProvider</p>
 * <p>Description: Defines a hystrix command provider</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.hystrix.HystrixCommandProvider</code></p>
 * @param <T> The expected return type of the hystrix command execution
 */

public interface HystrixCommandProvider<T> {
	/**
	 * Creates a new {@link HystrixCommand} to run the passed executuion
	 * @param executor The execution the {@link HystrixCommand} will wrap 
	 * @param fallback The optional fallback callable
	 * @return the command
	 */
	public HystrixCommand<T> commandFor(final Callable<T> executor, final Callable<T> fallback);
	
	/**
	 * Creates a new {@link HystrixCommand} without a fallback to run the passed executuion
	 * @param executor The execution the {@link HystrixCommand} will wrap 
	 * @return the command
	 */
	public HystrixCommand<T> commandFor(final Callable<T> executor);	

}
