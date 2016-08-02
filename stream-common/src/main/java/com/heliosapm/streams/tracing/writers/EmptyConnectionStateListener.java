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
 * <p>Title: EmptyConnectionStateListener</p>
 * <p>Description: Empty connection state listener for extending</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tracing.writers.EmptyConnectionStateListener</code></p>
 */

public class EmptyConnectionStateListener implements ConnectionStateListener {

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.writers.ConnectionStateListener#onConnected()
	 */
	@Override
	public void onConnected() {
		/* No Op */
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.writers.ConnectionStateListener#onDisconnected(boolean)
	 */
	@Override
	public void onDisconnected(final boolean shutdown) {
		/* No Op */
	}

}
