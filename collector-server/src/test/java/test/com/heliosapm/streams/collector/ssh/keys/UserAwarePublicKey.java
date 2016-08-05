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
package test.com.heliosapm.streams.collector.ssh.keys;

import java.security.PublicKey;

/**
 * <p>Title: UserAwarePublicKey</p>
 * <p>Description: A decoded public key that provides a reference to a user name and domain</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>test.com.heliosapm.streams.collector.ssh.keys.UserAwarePublicKey</code></p>
 */
public class UserAwarePublicKey implements PublicKey {
	/**  */
	private static final long serialVersionUID = 584844634977064095L;
	/** The wrapped public key */
	protected final PublicKey publicKey;
	/** The associated user name */
	protected final String userName;
	/** The user's domain */
	protected final String userDomain;
	
	/**
	 * Creates a new UserAwarePublicKey
	 * @param publicKey The wrapped public key
	 * @param userName The associated user name
	 * @param userDomain The user's domain
	 */
	public UserAwarePublicKey(PublicKey publicKey, String userName, String userDomain) {
		this.publicKey = publicKey;
		this.userName = userName;
		this.userDomain = userDomain;
	}

	/**
	 * {@inheritDoc}
	 * @see java.security.Key#getAlgorithm()
	 */
	@Override
	public String getAlgorithm() {
		return publicKey.getAlgorithm();
	}

	/**
	 * {@inheritDoc}
	 * @see java.security.Key#getFormat()
	 */
	@Override
	public String getFormat() {
		return publicKey.getFormat();
	}

	/**
	 * {@inheritDoc}
	 * @see java.security.Key#getEncoded()
	 */
	@Override
	public byte[] getEncoded() {
		return publicKey.getEncoded();
	}

	/**
	 * Returns the user name
	 * @return the userName
	 */
	public String getUserName() {
		return userName;
	}

	/**
	 * Returns the user domain
	 * @return the userDomain
	 */
	public String getUserDomain() {
		return userDomain;
	}
	
	/**
	 * Returns the user's fully qualified name
	 * @return the user's fully qualified name
	 */
	public String getFullName() {
		return new StringBuilder(userName).append("@").append(userDomain).toString();
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((publicKey == null) ? 0 : publicKey.hashCode());
		if(userDomain!=null && userName!=null) {
			result = prime * result
					+ ((userDomain == null) ? 0 : userDomain.hashCode());
			result = prime * result
					+ ((userName == null) ? 0 : userName.hashCode());
		}
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
		if(!(obj instanceof PublicKey)) {
			return false;
		}
		if (getClass() != obj.getClass() || userName==null || userDomain==null) {
			return publicKey.equals(obj);
		}
		UserAwarePublicKey other = (UserAwarePublicKey) obj;
		if (publicKey == null) {
			if (other.publicKey != null)
				return false;
		} else if (!publicKey.equals(other.publicKey))
			return false;
		if (userDomain == null) {
			if (other.userDomain != null)
				return false;
		} else if (!userDomain.equals(other.userDomain))
			return false;
		if (userName == null) {
			if (other.userName != null)
				return false;
		} else if (!userName.equals(other.userName))
			return false;
		return true;
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder b = new StringBuilder("PublicKey [");
		if(userDomain!=null && userName!=null) {
			b.append("\n\tUser:").append(userName).append("@").append(userDomain);
		}
		b.append("\n\tAlgorithm:").append(publicKey.getAlgorithm());
		b.append("\n\tFormat:").append(publicKey.getFormat());
		b.append("\n\tSize:").append(publicKey.getEncoded().length);
		b.append("\n]");
		return b.toString();
	}

	/**
	 * Returns the wrapped public key
	 * @return the publicKey
	 */
	public PublicKey getPublicKey() {
		return publicKey;
	}
}
