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
package test.com.heliosapm.streams.collector.ssh.auth;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.sshd.server.auth.password.PasswordAuthenticator;
import org.apache.sshd.server.session.ServerSession;

/**
 * <p>Title: PropFilePasswordAuthenticator</p>
 * <p>Description: Password authenticator that validates credentials against a resources property file</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>test.com.heliosapm.streams.collector.ssh.auth.PropFilePasswordAuthenticator</code></p>
 */
public class PropFilePasswordAuthenticator implements PasswordAuthenticator {
	/** The properties containing the username/password pairs to authenticate with */
	final Properties credentials = new Properties();
	/** The default bad password */
	final String DEF = "" + System.identityHashCode(this);
	/** The instance logger */
	protected final Logger log = Logger.getLogger(getClass());

	/**
	 * Creates a new PropFilePasswordAuthenticator
	 * @param fileName The name of the file to read the properties from
	 */
	public PropFilePasswordAuthenticator(CharSequence fileName) {
		if(fileName==null) throw new IllegalArgumentException("Passed file name was null", new Throwable());
		File f = new File(fileName.toString());
		if(!f.canRead()) {
			throw new RuntimeException("Cannot read the file [" + fileName + "]", new Throwable());
		}
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(f);
			if(f.getName().toLowerCase().endsWith(".xml")) {
				credentials.loadFromXML(fis);
			} else {
				credentials.load(fis);
			}
		} catch (Exception e) {
			throw new RuntimeException("Failed to load the file [" + fileName + "]", e);
		} finally {
			if(fis!=null) try { fis.close(); } catch (Exception e) {/* No Op */}
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.apache.sshd.server.auth.password.PasswordAuthenticator#authenticate(java.lang.String, java.lang.String, org.apache.sshd.server.session.ServerSession)
	 */
	@Override
	public boolean authenticate(final String username, final String password, final ServerSession session) {
		if(username==null || password==null) {
			log.info("Authentication failed for [" + username + "]. No username or password.");
			return false;
		}
		if(!password.equals(credentials.getProperty(username, DEF))) {
			log.info("Authentication failed for [" + username + "]. Invalid username or password.");
			return false;
		}
		log.info("Authenticated [" + username + "]");
		return true;
	}

}
