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
package com.heliosapm.streams.tracing.writers;

/**
 * <p>Title: ConnectionStateSupplier</p>
 * <p>Description: Defines a class that supports notifying registered listeners of its connected state</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tracing.writers.ConnectionStateSupplier</code></p>
 */

public interface ConnectionStateSupplier {
	/**
	 * Registers the passed connected state listener
	 * @param listener The connected state listener to register
	 */
	public void registerConnectionStateListener(final ConnectionStateListener listener);

	/**
	 * Unregisters the passed connected state listener
	 * @param listener The connected state listener to unregister
	 */
	public void removeConnectionStateListener(final ConnectionStateListener listener);
}
