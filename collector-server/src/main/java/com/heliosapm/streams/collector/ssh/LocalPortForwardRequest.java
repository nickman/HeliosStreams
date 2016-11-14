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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonCreator.Mode;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * <p>Title: LocalPortForwardRequest</p>
 * <p>Description: Represents an SSH local port forward configuration</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.ssh.LocalPortForwardRequest</code></p>
 */

public class LocalPortForwardRequest {

	/** The local port, defaults to zero for an ephremeral port */	
	protected final int localPort;
	/** The remote port to tunnel to */	
	protected final int remotePort;
	/** The remote host to tunnel to */	
	protected final String remoteHost;
	/** The request key */
	protected final String key;
	
	/**
	 * Creates a new LocalPortForwardRequest
	 * @param localPort The local port, defaults to zero for an ephremeral port
	 * @param remotePort The remote port to tunnel to
	 * @param remoteHost The remote host to tunnel to
	 */
	@JsonCreator(mode=Mode.PROPERTIES)
	public LocalPortForwardRequest(
			@JsonProperty(value="localport", defaultValue="0") final int localPort, 
			@JsonProperty(value="remoteport") final int remotePort, 
			@JsonProperty(value="remotehost") final String remoteHost) {
		if(localPort < 0 || localPort > 65535) throw new IllegalArgumentException("The passed local port [" + localPort + "] is invalid");
		if(remotePort < 1 || remotePort > 65535) throw new IllegalArgumentException("The passed remote port [" + localPort + "] is invalid");
		if(remoteHost==null || remoteHost.trim().isEmpty()) throw new IllegalArgumentException("The passed remote host was null or empty");
		this.localPort = localPort;
		this.remotePort = remotePort;
		this.remoteHost = remoteHost.trim();
		key = this.remoteHost + "-" + remotePort;
	}
	
	/**
	 * Creates a new LocalPortForwardRequest with an ephemeral local port
	 * @param remotePort The remote port to tunnel to
	 * @param remoteHost The remote host to tunnel to
	 */
	public LocalPortForwardRequest(final int remotePort, final String remoteHost) {
		this(0, remotePort, remoteHost);
	}

	/**
	 * Returns the local port
	 * @return the localPort
	 */
	public int getLocalPort() {
		return localPort;
	}

	/**
	 * Returns the remote port
	 * @return the remotePort
	 */
	public int getRemotePort() {
		return remotePort;
	}

	/**
	 * Returns the remote host
	 * @return the remoteHost
	 */
	public String getRemoteHost() {
		return remoteHost;
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return new StringBuilder("LocalPortForwardRequest [rhost:")
			.append(remoteHost).append(", rport:").append(remotePort)
			.append(", lport:").append(localPort).append("]").toString();
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + localPort;
		result = prime * result + ((remoteHost == null) ? 0 : remoteHost.hashCode());
		result = prime * result + remotePort;
		return result;
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LocalPortForwardRequest other = (LocalPortForwardRequest) obj;
		if (localPort != other.localPort)
			return false;
		if (remoteHost == null) {
			if (other.remoteHost != null)
				return false;
		} else if (!remoteHost.equals(other.remoteHost))
			return false;
		if (remotePort != other.remotePort)
			return false;
		return true;
	}

	/**
	 * Returns the request key
	 * @return the key
	 */
	public String getKey() {
		return key;
	}
	
	
	
	
	

}
