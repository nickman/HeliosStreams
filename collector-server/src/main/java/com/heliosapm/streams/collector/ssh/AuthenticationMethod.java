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
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.heliosapm.utils.tuples.NVP;

import ch.ethz.ssh2.InteractiveCallback;

/**
 * <p>Title: AuthenticationMethod</p>
 * <p>Description: Functional enumeration of SSH authentication methods</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.ssh.AuthenticationMethod</code></p>
 */

public enum AuthenticationMethod implements Authenticationator {
	/** Attempt to authenticate with a username and nothing else */
	NONE("none"){
		@Override
		public boolean authenticate(final SSHConnection conn) throws IOException {
			if(validate(conn)) return true;
			return conn.connection.authenticateWithNone(conn.user);
		}
	},
	/** Attempt to authenticate with a user name and password */
	PASSWORD("password"){
		@Override
		public boolean authenticate(final SSHConnection conn) throws IOException {
			if(validate(conn)) return true;
			if(conn.password==null) return false;
			return conn.connection.authenticateWithPassword(conn.user, conn.password);
		}
	},
	/** Attempt to authenticate with an interactive callback */
	INTERACTIVE("keyboard-interactive"){
		@Override
		public boolean authenticate(final SSHConnection conn) throws IOException {
			if(validate(conn)) return true;
			if(conn.password==null) return false;
			return conn.connection.authenticateWithKeyboardInteractive(conn.user, iback(conn));
		}
	},
	/** Attempt to authenticate with a public key */
	PUBLICKEY("publickey"){
		@Override
		public boolean authenticate(final SSHConnection conn) throws IOException {
			if(validate(conn)) return true;
			final char[] pk = conn.getPrivateKey();			
			if(pk==null) return false;
			return conn.connection.authenticateWithPublicKey(conn.user, pk, conn.passPhrase);
		}
	};
	
	private AuthenticationMethod(final String sshName) {
		this.sshName = sshName;
	}
	
	/** The SSH name of the authentication method */
	public final String sshName;
	
	/** Recognized SSH names for the auth methods */
	public static final Set<String> SSH_NAMES = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList("password", "publickey", "keyboard-interactive")));
	/** The AuthenticationMethods keyed by the ssh name */
	public static final Map<String, AuthenticationMethod> SSHNAME2ENUM;
	
	private static final String[] SSH_NAMES_ARR = SSH_NAMES.toArray(new String[SSH_NAMES.size()]);
	
	/**
	 * Returns the SSH names of the authentication methods
	 * @return the SSH names of the authentication methods
	 */
	public static final String[] getSSHAuthenticationMethodNames() {
		return SSH_NAMES_ARR.clone();
	}
	
	
	/**
	 * Returns a set of the AuthenticationMethods matching the supplied SSH authentication method names.
	 * @param sshAuthMethods an array of SSH authentication method names
	 * @return a [possibly empty] set of AuthenticationMethods 
	 */
	public static Set<AuthenticationMethod> getAvailableMethods(final String...sshAuthMethods) {
		final  Set<AuthenticationMethod> set = EnumSet.of(AuthenticationMethod.NONE);
		if(sshAuthMethods!=null) {
			for(String s: sshAuthMethods) {
				if(s==null) continue;
				AuthenticationMethod am = SSHNAME2ENUM.get(s.trim());
				if(am!=null) {
					set.add(am);
				}
			}
		}
		return set;
		
	}
	
	/**
	 * Authenticates the passed connection
	 * @param conn the connection on which to authenticate
	 * @return true if authentication completed, false otherwise
	 */
	public static NVP<Boolean, AuthenticationMethod> auth(final SSHConnection conn) {
		
		for(AuthenticationMethod am: getAvailableMethods(conn.getRemainingAuthMethods())) {
			try {				
//				System.err.println("Attempting [" + am + "] for [" + conn + "]");
				am.authenticate(conn);
				if(conn.connection.isAuthenticationComplete()) return new NVP<Boolean, AuthenticationMethod>(true, am);
			} catch (Exception x){
				System.err.println("Auth Fail on [" + am + "]:" + x);
				x.printStackTrace(System.err);
			}			
		}
		return new NVP<Boolean, AuthenticationMethod>(false, null);
	}
	

	static {
		final AuthenticationMethod[] values = AuthenticationMethod.values();
		final Map<String, AuthenticationMethod> tmp = new HashMap<String, AuthenticationMethod>(values.length);
		for(AuthenticationMethod am: values) {
			tmp.put(am.sshName, am);
		}
		SSHNAME2ENUM = Collections.unmodifiableMap(tmp);
	}
	
	/**
	 * Returns an initialized set of the default available AuthenticationMethods
	 * @return an initialized set of the default available AuthenticationMethods
	 */
	public static Set<AuthenticationMethod> getAvailableMethodSet() {
		return EnumSet.of(PASSWORD, PUBLICKEY);
	}
	
	/**
	 * Removes the passed AuthenticationMethods from the passed set and returns the remaining as an array of the remaining members' ssh names
	 * @param remaining The set of remaining names
	 * @param remove The methods to remove
	 * @return A string array of the remaining auth method SSH names
	 */
	public static String[] toSSHNames(final Set<AuthenticationMethod> remaining, final AuthenticationMethod...remove) {
		if(remaining==null || remaining.isEmpty()) return new String[]{};
		final Set<String> set = new HashSet<String>(remaining.size());
		remaining.removeAll(new HashSet<AuthenticationMethod>(Arrays.asList(remove)));
		if(!remaining.isEmpty()) {
			for(AuthenticationMethod am: remaining) {
				set.add(am.sshName);
			}
		}
		return set.toArray(new String[set.size()]);
	}
	
	private static boolean validate(final SSHConnection conn) {
		if(conn==null) throw new IllegalArgumentException("The passed connection was null");
		if(conn.connection.isAuthenticationComplete()) return true;
		return false;
	}
	
	private static InteractiveCallback iback(final SSHConnection conn) {
		return new InteractiveCallback() {
			@Override
			public String[] replyToChallenge(String name, String instruction,
					int numPrompts, String[] prompt, boolean[] echo)
					throws Exception {
					return new String[]{conn.password};
			}
		};
	}
	
	/**
	 * Decodes the passed comma delimited phrase into an array of AuthenticationMethods
	 * @param phrase A comma delimited string of auth method names
	 * @return an array of AuthenticationMethods
	 */
	public static AuthenticationMethod[] decode(final String phrase) {
		final EnumSet<AuthenticationMethod> set = EnumSet.noneOf(AuthenticationMethod.class);
		for(String s: phrase.split(",")) {
			if(s==null || s.trim().isEmpty()) continue;
			set.add(fromName(s));
		}
		return set.toArray(new AuthenticationMethod[set.size()]);
	}
	
	/**
	 * Returns the auth method from the passed trimmed and upper cased phrase
	 * @param phrase The sshd supplied auth method phrase
	 * @return the AuthenticationMethod
	 */
	public static AuthenticationMethod fromName(final String phrase) {
		if(phrase==null || phrase.trim().isEmpty()) throw new IllegalArgumentException("The passed phrase was null or empty");
		try {
			return AuthenticationMethod.valueOf(phrase.trim().toUpperCase());
		} catch (Exception ex) {
			throw new IllegalArgumentException("The passed phrase [" + phrase + "] could not be decoded to a valid AuthenticationMethod");
		}
	}
	
	

}
