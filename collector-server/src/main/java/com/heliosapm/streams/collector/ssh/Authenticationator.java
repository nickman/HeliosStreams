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

import java.io.IOException;

/**
 * <p>Title: Authenticationator</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.ssh.Authenticationator</code></p>
 */

public interface Authenticationator {
	/**
	 * Attempts an authentication against the passed connection
	 * If the connection is already fully authenticated, immediately returns true
	 * @param conn The connection to authenticate against
	 * @return true the connection is now authenticated, false otherwise
	 * @throws IOException Thrown on any IO error
	 */
	public boolean authenticate(final SSHConnection conn) throws IOException;

}
