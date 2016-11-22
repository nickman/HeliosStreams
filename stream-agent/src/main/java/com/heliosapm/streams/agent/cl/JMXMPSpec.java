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
package com.heliosapm.streams.agent.cl;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import com.heliosapm.streams.agent.endpoint.Endpoint;
import com.heliosapm.utils.lang.StringHelper;

/**
 * <p>Title: JMXMPSpec</p>
 * <p>Description: Definition for a JMXMP Connection Server installation and the advertised endpoints accessible therein</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.agent.cl.JMXMPSpec</code></p>
 */
public class JMXMPSpec {
	/** The specs delimeter */
	public static final char MULTI_DELIM = ',';	
	/** The spec delimeter */
	public static final char DELIM = ':';
	/** The default bind interface */
	public static final String DEFAULT_BIND = "127.0.0.1";
	/** The default jmx domain */
	public static final String DEFAULT_DOMAIN = "DefaultDomain";
	/** The default JMXMP listening port */
	public static final int DEFAULT_PORT = 1818;
	
	/** The configured bind interface */
	protected String bindInterface = DEFAULT_BIND;
	/** The configured JMX domain */
	protected String jmxDomain = DEFAULT_DOMAIN;
	/** The configured JMXMP listening port */
	protected int port = DEFAULT_PORT;
	
	/**
	 * Creates a new JMXMPSpec
	 */
	public JMXMPSpec() {
	
	}
	
	/**
	 * Parses the passed stringy into an array of JMXSpecs
	 * @param cs The stringy to parse
	 * @return an array of JMXSpecs
	 */
	public static JMXMPSpec[] parse(final CharSequence cs) {
		if(cs==null) throw new IllegalArgumentException("The passed spec was null");
		final String spec = cs.toString().trim();
		if(spec.isEmpty()) throw new IllegalArgumentException("The passed spec was empty");
		final String[] frags = StringHelper.splitString(spec, MULTI_DELIM, true);
		final Set<JMXMPSpec> specs = new LinkedHashSet<JMXMPSpec>(frags.length);
		for(final String s: frags) {
			final String[] specFrags = StringHelper.splitString(s, DELIM, true);
			final JMXMPSpec jspec = new JMXMPSpec();
			if(specFrags.length > 1) {
				if(specFrags[0]!=null && !specFrags[0].trim().isEmpty()) {
					jspec.setPort(Integer.parseInt(specFrags[0].trim()));
				}
			}
			if(specFrags.length > 1) {
				if(specFrags[1]!=null && !specFrags[1].trim().isEmpty()) {
					jspec.setBindInterface(specFrags[1].trim());
				}
			}
			if(specFrags.length > 2) {
				if(specFrags[2]!=null && !specFrags[2].trim().isEmpty()) {
					jspec.setJmxDomain(specFrags[2].trim());
				}					
			}				
			if(!specs.add(jspec)) {
				System.err.println("[JMXMPSpec] WARNING: Duplicate JMXMP Spec: [" + jspec + "]");
			}
		}		
		return specs.toArray(new JMXMPSpec[specs.size()]);
	}

	/**
	 * Returns the binding interface for the JMXMP connector server
	 * @return the binding interface for the JMXMP connector server
	 */
	public String getBindInterface() {
		return bindInterface;
	}

	/**
	 * Sets the binding interface for the JMXMP connector server
	 * @param bindInterface the bind interface to set
	 */
	protected void setBindInterface(final String bindInterface) {
		if(bindInterface==null || bindInterface.trim().isEmpty()) throw new IllegalArgumentException("The passed bind interface was null or empty");
		this.bindInterface = bindInterface.trim();
	}

	/**
	 * Returns the jmx domain
	 * @return the jmx domain
	 */
	public String getJmxDomain() {
		return jmxDomain;
	}

	/**
	 * Sets the jmx domain
	 * @param jmxDomain the jmx domain to set
	 */
	protected void setJmxDomain(final String jmxDomain) {
		if(jmxDomain==null || jmxDomain.trim().isEmpty()) throw new IllegalArgumentException("The passed JMX Domain was null or empty");
		this.jmxDomain = jmxDomain.trim();
	}

	/**
	 * Returns the JMXMP listening port
	 * @return the JMXMP listening port
	 */
	public int getPort() {
		return port;
	}

	/**
	 * Sets the JMXMP listening port
	 * @param port the JMXMP listening port
	 */
	protected void setPort(final int port) {
		if(port < 0 || port > 65534) throw new IllegalArgumentException("Invalid port [" + port + "]");
		this.port = port;
	}
	
	

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("%s:%s:%s", port, bindInterface, jmxDomain);
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((bindInterface == null) ? 0 : bindInterface.hashCode());
		result = prime * result + port;
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
		JMXMPSpec other = (JMXMPSpec) obj;
		if (bindInterface == null) {
			if (other.bindInterface != null)
				return false;
		} else if (!bindInterface.equals(other.bindInterface))
			return false;
		if (port != other.port)
			return false;
		return true;
	}
	
	
}
