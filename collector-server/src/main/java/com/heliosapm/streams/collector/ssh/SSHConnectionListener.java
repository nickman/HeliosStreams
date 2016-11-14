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
package com.heliosapm.streams.collector.ssh;

/**
 * <p>Title: SSHConnectionListener</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.ssh.SSHConnectionListener</code></p>
 */

public interface SSHConnectionListener {
	/**
	 * Callback when an SSHConnection is started
	 * @param connectionKey the key of the connection
	 */
	public void onStarted(final String connectionKey);

	/**
	 * Callback when an SSHConnection is stopped
	 * @param connectionKey the key of the connection
	 */
	public void onStopped(final String connectionKey);
	
	/**
	 * Callback when an SSHConnection is [re-]connected
	 * @param connectionKey the key of the connection
	 */
	public void onConnected(final String connectionKey);
	
	/**
	 * Callback when an SSHConnection is disconnected
	 * @param connectionKey the key of the connection
	 */
	public void onDisconnected(final String connectionKey);
}
