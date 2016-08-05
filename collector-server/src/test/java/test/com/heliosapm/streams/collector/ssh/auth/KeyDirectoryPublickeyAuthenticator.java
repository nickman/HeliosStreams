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
import java.security.PublicKey;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.log4j.Logger;
import org.apache.sshd.common.keyprovider.AbstractFileKeyPairProvider;
import org.apache.sshd.common.util.SecurityUtils;
import org.apache.sshd.server.session.ServerSession;

import test.com.heliosapm.streams.collector.ssh.keys.KeyDecoder;
import test.com.heliosapm.streams.collector.ssh.keys.UserAwarePublicKey;


/**
 * <p>Title: KeyDirectoryPublickeyAuthenticator</p>
 * <p>Description: Public key authenticator that validates usernames and public keys based on a directory structure.</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>org.helios.net.ssh.auth.KeyDirectoryPublickeyAuthenticator</code></p>
 */
public class KeyDirectoryPublickeyAuthenticator implements org.apache.sshd.server.auth.pubkey.PublickeyAuthenticator {
	/** The key location directory */
	protected final File keyDir;
	/** The key pair provider */
	protected final AbstractFileKeyPairProvider keyPairProvider;
	/** The loaded public keys */
	protected final Map<String, Set<PublicKey>> pks = new ConcurrentHashMap<String, Set<PublicKey>>();
	/** The instance logger */
	protected final Logger log = Logger.getLogger(getClass());
	
	
	/**
	 * Adds a public key to the directory
	 * @param key The key in string form
	 * @return the public key
	 */
	public UserAwarePublicKey addPublicKey(String key) {
		if(key==null) throw new IllegalArgumentException("The passed key was null", new Throwable());
		try {
			UserAwarePublicKey pubKey = KeyDecoder.getInstance().decodePublicKey(key);
			Set<PublicKey> userPks = pks.get(pubKey.getUserName());
			if(userPks==null) {
				userPks = new CopyOnWriteArraySet<PublicKey>();
				pks.put(pubKey.getUserName(), userPks);
			}
			userPks.add(pubKey.getPublicKey());
			log.info("Added public key [" + pubKey.getAlgorithm() + "] for user [" + pubKey.getFullName() + "]");
			return pubKey;
		} catch (Exception e) {
			throw new RuntimeException("Failed to add public key", e);
		}
	}
	
	/**
	 * Creates a new KeyDirectoryPublickeyAuthenticator
	 * @param keyDir The key location directory
	 */
	public KeyDirectoryPublickeyAuthenticator(File keyDir) {
		if(keyDir==null) throw new IllegalArgumentException("The passed directory was null", new Throwable());
		if(!keyDir.exists()) throw new IllegalArgumentException("The passed directory [" + keyDir + "] does not exist.", new Throwable());
		if(!keyDir.isDirectory()) throw new IllegalArgumentException("The passed file [" + keyDir + "] is not a directory.", new Throwable());
		this.keyDir = keyDir;	
		int loaded = 0;
		for(File f: keyDir.listFiles()) {
			if(f.getName().contains("rsa") || f.getName().contains("dsa")) {
				if(f.getName().toLowerCase().endsWith(".pub")) {
					try {						
						UserAwarePublicKey pubKey = KeyDecoder.getInstance().decodePublicKey(f);
						Set<PublicKey> userPks = pks.get(pubKey.getUserName());
						if(userPks==null) {
							userPks = new CopyOnWriteArraySet<PublicKey>();
							pks.put(pubKey.getUserName(), userPks);
						}
						userPks.add(pubKey.getPublicKey());
						//log.info("Added [" + pubKey + "]");
					} catch (Exception e) {
						log.warn("Failed to load public key in file [" + f.getAbsolutePath() + "]", e);
					}
				}
			}
		}
		for(Set<PublicKey> upks: pks.values()) {
			loaded += upks.size();
		}
		log.info("Loaded [" + loaded + "] keys for [" + pks.size() + "] users to auth provider");
		keyPairProvider = SecurityUtils.createFileKeyPairProvider();
	}

	/**
	 * Creates a new KeyDirectoryPublickeyAuthenticator
	 * @param keyDir The key location directory name
	 */
	public KeyDirectoryPublickeyAuthenticator(CharSequence keyDir) {
		this(new File(keyDir.toString()));
	}
	
	


	/**
	 * {@inheritDoc}
	 * @see org.apache.sshd.server.PublickeyAuthenticator#authenticate(java.lang.String, java.security.PublicKey, org.apache.sshd.server.session.ServerSession)
	 */
	@Override
	public boolean authenticate(String username, PublicKey key, ServerSession session) {
		if(username==null) throw new IllegalArgumentException("The passed user name was null", new Throwable());
		if(key==null) throw new IllegalArgumentException("The passed key was null", new Throwable());
		if(log.isDebugEnabled()) log.debug("Authenticating [" + username + "]");
		Set<PublicKey> userKeys = pks.get(username);
		if(userKeys==null) {
			synchronized(pks) {
				userKeys = pks.get(username);
				if(userKeys==null) {
					log.info("Authentication failed for [" + username + "]. No authorized keys found.");
					return false;
				}
			}
		}
		if(userKeys.contains(key)) {			
			log.info("Authenticated [" + username + "]");
			return true;
		}
		log.info("Authentication failed for [" + username + "]. No authorized keys matched.");
		return false;
		
	}
	
//	protected byte[] getKeyBytes(File f) {
//		ByteArrayOutputStream baos = new ByteArrayOutputStream((int)f.length());
//		FileInputStream fis = null;
//	}

}
