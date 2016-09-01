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
package com.heliosapm.streams.discovery;

import java.util.Arrays;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.heliosapm.streams.common.naming.AgentName;
import com.heliosapm.streams.json.JSONOps;
import com.heliosapm.utils.jmx.JMXHelper;

/**
 * <p>Title: AdvertisedEndpoint</p>
 * <p>Description: Artifact published to zookeep to advertise a monitoring endpoint and associated details.</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.discovery.AdvertisedEndpoint</code></p>
 * 
 */

public class AdvertisedEndpoint {
	/** The JMX URL */
	@JsonProperty("jmx")
	protected String jmxUrl;
	/** The published endpoints indicating to monitors what the categories of data collection available are */
	@JsonProperty("endpoints")
	protected String[] endPoints;
	/** The app name */
	@JsonProperty("app")
	protected String app;
	/** The host name */
	@JsonProperty("host")
	protected String host;
	/** The monitoring port */
	@JsonProperty("port")
	protected int port;
	

	/**
	 * Creates a new AdvertisedEndpoint
	 */
	public AdvertisedEndpoint() {

	}
	
	
	
	/**
	 * Creates a new AdvertisedEndpoint
	 * @param jmxUrl The accessible JMX URL
	 * @param app The application name
	 * @param host The public host name
	 * @param endPoints The monitorable endpoints
	 */
	public AdvertisedEndpoint(final String jmxUrl, final String app, final String host, final String...endPoints) {
		if(jmxUrl==null || jmxUrl.trim().isEmpty()) throw new IllegalArgumentException("The passed JMX URL was null or empty");
		if(app==null || app.trim().isEmpty()) throw new IllegalArgumentException("The passed App Name was null or empty");
		if(host==null || host.trim().isEmpty()) throw new IllegalArgumentException("The passed public Host Name was null or empty");
		if(endPoints==null || endPoints.length==0) throw new IllegalArgumentException("The passed endpoints were empty");
		for(int i = 0; i < endPoints.length; i++) {
			if(endPoints[i]==null || endPoints[i].trim().isEmpty()) throw new IllegalArgumentException("The endpoint [" + i + "] was null or empty");
			endPoints[i] = endPoints[i].trim();
		}
		this.jmxUrl = jmxUrl.trim();
		this.app = app.trim();
		this.host = host.trim();
		this.endPoints = endPoints;
		this.port = JMXHelper.serviceUrl(this.jmxUrl).getPort();
	}

	/**
	 * Creates a new AdvertisedEndpoint using the {@link AgentName} to get the app and host name
	 * @param jmxUrl The accessible JMX URL
	 * @param endPoints The monitorable endpoints
	 */
	public AdvertisedEndpoint(final CharSequence jmxUrl, final String...endPoints) {
		this(jmxUrl.toString(), AgentName.getInstance().getAppName(), AgentName.getInstance().getHostName(), endPoints);
	}
	
	
	/**
	 * Returns the expected ZK path for this node
	 * @param root The publisher service type
	 * @return the expected zk path
	 */
	@JsonIgnore
	public String getZkPath(final String root) {
		return String.format("%s/%s/%s/%s-%s", root, host, app, "jmx", port);
	}
	
	/**
	 * Returns the parent path elements for this endpoint
	 * @param root the publisher service type
	 * @return the parent path elements for this endpoint
	 */
	@JsonIgnore
	public String[] getZkPathElements(final String root) {
		return new String[]{root, host, app};
	}
	
	/**
	 * Returns a byte array containing the JSON representing this endpoint
	 * @return a byte array containing the JSON representing this endpoint
	 */
	public byte[] toByteArray() {
		return JSONOps.serializeToBytes(this);
	}
	/**
	 * Returns this endpoints unique id
	 * @return this endpoints unique id
	 */
	@JsonIgnore
	public String getId() {
		return host + "/" + app + "/" + port + "/jmx"; 
	}
	
	
	/**
	 * Returns the port
	 * @return the port
	 */
	@JsonIgnore
	public int getPort() {
		return port;
	}

	/**
	 * Returns the JMX URL
	 * @return the JMX URL
	 */
	
	public String getJmxUrl() {
		return jmxUrl;
	}

	/**
	 * Returns the advertised monitoring endpoints
	 * @return the advertised monitoring endpoints
	 */
	public String[] getEndPoints() {
		return endPoints.clone();
	}

	/**
	 * Returns the app name
	 * @return the app name
	 */
	public String getApp() {
		return app;
	}

	/**
	 * Returns the public host name
	 * @return the public host name
	 */
	public String getHost() {
		return host;
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("AdvertisedEndpoint [jmxUrl=");
		builder.append(jmxUrl);
		builder.append(", endPoints=");
		builder.append(Arrays.toString(endPoints));
		builder.append(", app=");
		builder.append(app);
		builder.append(", host=");
		builder.append(host);
		builder.append("]");
		return builder.toString();
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((app == null) ? 0 : app.hashCode());
		result = prime * result + ((host == null) ? 0 : host.hashCode());
		result = prime * result + ((jmxUrl == null) ? 0 : jmxUrl.hashCode());
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
		AdvertisedEndpoint other = (AdvertisedEndpoint) obj;
		if (app == null) {
			if (other.app != null)
				return false;
		} else if (!app.equals(other.app))
			return false;
		if (host == null) {
			if (other.host != null)
				return false;
		} else if (!host.equals(other.host))
			return false;
		if (jmxUrl == null) {
			if (other.jmxUrl != null)
				return false;
		} else if (!jmxUrl.equals(other.jmxUrl))
			return false;
		return true;
	}

	
	

}
