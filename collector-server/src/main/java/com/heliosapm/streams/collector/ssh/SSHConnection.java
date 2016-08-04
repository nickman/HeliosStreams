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

import java.io.File;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.ObjectName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.jmx.JMXManagedScheduler;
import com.heliosapm.utils.url.URLHelper;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.ConnectionInfo;
import ch.ethz.ssh2.ConnectionMonitor;
import ch.ethz.ssh2.ServerHostKeyVerifier;

/**
 * <p>Title: SSHConnection</p>
 * <p>Description: Represents an SSH connection</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.ssh.SSHConnection</code></p>
 */

public class SSHConnection implements ConnectionMonitor, Runnable, ServerHostKeyVerifier {
	
	/** A cache of connections keyed by <b><code>&lt;user&gt@&lt;host&gt:&lt;sshportr&gt</code></b>. */
	private static final NonBlockingHashMap<String, SSHConnection> connections = new NonBlockingHashMap<String, SSHConnection>();
	
	
	/** Placeholder connection */
	private static final SSHConnection PLACEHOLDER = new SSHConnection();
	/** Connection key template */
	private static final String KEY_TEMPLATE = "%s@%s:%s";
	
	/** The throwable message when a normal connection close occurs */
	private static final String NORMAL_CLOSE_MESSAGE = "Closed due to user request.";
	
	/** The reconnect thread pool JMX ObjectName */
	public static final ObjectName THREAD_POOL_OBJECT_NAME = JMXHelper.objectName("");
	
	
	/** The reconnect thread pool scheduler */	
	private static final JMXManagedScheduler reconnectScheduler = new JMXManagedScheduler(THREAD_POOL_OBJECT_NAME, "SSHReconnector", 4, true);
	
	
	/** The host to connect to */
	@JsonProperty(value="host", required=true)
	protected String host = null;
	/** The ssh listener port */
	@JsonProperty(value="sshport", defaultValue="22")
	protected int sshPort = 22;
	/** The user to connect as */
	@JsonProperty(value="user", required=true)
	protected String user = null;
	/** The user password */
	@JsonProperty(value="password")
	protected String password = null;
	/** The ssh private key */
	@JsonProperty(value="privatekey")
	protected char[] privateKey = null;
	/** The ssh private key file */
	@JsonProperty(value="privatekeyfile")
	protected File privateKeyFile = null;	
	/** The ssh private key passphrase */
	@JsonProperty(value="passphrase")
	protected String passPhrase = null;

	/** The connection key */
	@JsonIgnore
	protected final String key;
	/** The connection log */
	@JsonIgnore
	protected final Logger log = LogManager.getLogger(getClass());
	
	
	/** The SSH connection */
	@JsonIgnore
	protected final Connection connection;
	/** The SSH connection's info */
	@JsonIgnore
	protected ConnectionInfo connectionInfo = null;
	/** Indicates if the connection is connected */
	@JsonIgnore
	protected final AtomicBoolean connected = new AtomicBoolean(false);
	/** Indicates if the connection is started */
	@JsonIgnore
	protected final AtomicBoolean started = new AtomicBoolean(false);
	/** The reconnect schedule handle for this connection */
	@JsonIgnore
	protected volatile ScheduledFuture<?> scheuduleHandle = null;
	
	/**
	 * Creates a new SSHConnection
	 */
	private SSHConnection() {
		key = null;
		connection = null;
	}
	
	
	/**
	 * Creates a new SSHConnection
	 * @param host The host to connect to
	 * @param sshPort The sshd listening port
	 * @param user The user to connect as
	 * @param password the optional user password
	 * @return the connection
	 */
	public static SSHConnection getConnection(final String host, final int sshPort, final String user, final String password) {
		if(host==null || host.trim().isEmpty()) throw new IllegalArgumentException("The host name was null or empty");
		if(user==null || user.trim().isEmpty()) throw new IllegalArgumentException("The user name was null or empty");
		if(sshPort < 1 || sshPort > 65535) throw new IllegalArgumentException("The ssh port number [" + sshPort + "] is invalid");
		final String key = String.format(KEY_TEMPLATE, user.trim(), host.trim(), sshPort);
		SSHConnection conn = connections.putIfAbsent(key, PLACEHOLDER);
		if(conn==null || conn==PLACEHOLDER) {
			conn = new SSHConnection(host, sshPort, user, password);
			connections.replace(key, conn);
		}
		return conn;			
	}

	
	/**
	 * Creates a new SSHConnection
	 * @param host The host to connect to
	 * @param sshPort The sshd listening port
	 * @param user The user to connect as
	 * @param privateKeyFileName The private key file name
	 * @param passPhrase The optional private key passphrase
	 * @return the connection
	 */
	public static SSHConnection getConnection(final String host, final int sshPort, final String user, final String privateKeyFileName, final String passPhrase) {
		if(host==null || host.trim().isEmpty()) throw new IllegalArgumentException("The host name was null or empty");
		if(user==null || user.trim().isEmpty()) throw new IllegalArgumentException("The user name was null or empty");
		if(sshPort < 1 || sshPort > 65535) throw new IllegalArgumentException("The ssh port number [" + sshPort + "] is invalid");
		final String key = String.format(KEY_TEMPLATE, user.trim(), host.trim(), sshPort);
		SSHConnection conn = connections.putIfAbsent(key, PLACEHOLDER);
		if(conn==null || conn==PLACEHOLDER) {
			conn = new SSHConnection(host, sshPort, user, privateKeyFileName);
			connections.replace(key, conn);
		}
		return conn;			
	}

	/**
	 * Creates a new SSHConnection
	 * @param host The host to connect to
	 * @param sshPort The sshd listening port
	 * @param user The user to connect as
	 * @param privateKey The private key characters
	 * @param passPhrase The optional private key passphrase
	 * @return the connection
	 */
	public static SSHConnection getConnection(final String host, final int sshPort, final String user, final char[] privateKey, final String passPhrase) {
		if(host==null || host.trim().isEmpty()) throw new IllegalArgumentException("The host name was null or empty");
		if(user==null || user.trim().isEmpty()) throw new IllegalArgumentException("The user name was null or empty");
		if(sshPort < 1 || sshPort > 65535) throw new IllegalArgumentException("The ssh port number [" + sshPort + "] is invalid");
		final String key = String.format(KEY_TEMPLATE, user.trim(), host.trim(), sshPort);
		SSHConnection conn = connections.putIfAbsent(key, PLACEHOLDER);
		if(conn==null || conn==PLACEHOLDER) {
			conn = new SSHConnection(host, sshPort, user, privateKey, passPhrase);
			connections.replace(key, conn);
		}
		return conn;			
	}
 
	
	/**
	 * Creates a new SSHConnection
	 * @param host The host to connect to
	 * @param sshPort The sshd listening port
	 * @param user The user to connect as
	 * @param password the optional user password
	 */
	private SSHConnection(final String host, final int sshPort, final String user, final String password) {
		this.host = host;
		this.sshPort = sshPort;
		this.user = user;
		this.password = password;
		key = toString();
		connection = new Connection(host, sshPort);
		connection.addConnectionMonitor(this);
	}

	/**
	 * Creates a new SSHConnection
	 * @param host The host to connect to
	 * @param sshPort The sshd listening port
	 * @param user The user to connect as
	 * @param privateKey The private key characters
	 * @param passPhrase The optional private key passphrase
	 */
	private SSHConnection(final String host, final int sshPort, final String user, final char[] privateKey, final String passPhrase) {
		this.host = host;
		this.sshPort = sshPort;
		this.user = user;
		this.privateKey = privateKey;
		this.passPhrase = passPhrase;
		key = toString();
		connection = new Connection(host, sshPort);
		connection.addConnectionMonitor(this);
	}

	/**
	 * Creates a new SSHConnection
	 * @param host The host to connect to
	 * @param sshPort The sshd listening port
	 * @param user The user to connect as
	 * @param privateKeyFileName The private key file name
	 * @param passPhrase The optional private key passphrase
	 */
	private SSHConnection(final String host, final int sshPort, final String user, final String privateKeyFileName, final String passPhrase) {
		this.host = host;
		this.sshPort = sshPort;
		this.user = user;
		this.privateKeyFile = new File(privateKeyFileName);
		this.passPhrase = passPhrase;
		key = toString();
		connection = new Connection(host, sshPort);
		connection.addConnectionMonitor(this);
	}
	
	/**
	 * <p>Starts a reconnect attempt</p>
	 * {@inheritDoc}
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		try {
			connectionInfo = connection.connect(this, 10000, 10000); // FIXME: make this configurable
			log.info("Connected to [{}], starting authentication", key);
			if(AuthenticationMethod.auth(this)) {
				connected.set(true);
				scheuduleHandle.cancel(false);
			} else {
				log.warn("Authentication on SSHConnection [{}] failed", key);
			}
		} catch (Exception ex) {
			if(ex instanceof InterruptedException) {
				log.info("Reconnect task for [{}] interrupted while running", key);
			}
		}
	}
	
	void start() throws Exception {
		if(started.compareAndSet(false, true)) {
			scheuduleHandle = reconnectScheduler.scheduleWithFixedDelay(this, 2, 15, TimeUnit.SECONDS);
		}
	}
	
	void stop() {
		if(started.compareAndSet(true, false)) {
			try {
				if(scheuduleHandle!=null) try { scheuduleHandle.cancel(true); } catch (Exception x) {/* No Op */}
				scheuduleHandle = null;
				try { connection.close(); } catch (Exception x) {/* No Op */}
			} finally {
				connections.remove(key);
			}
		}		
	}
	
	char[] getPrivateKey() {
		if(privateKey!=null) return privateKey;
		if(privateKeyFile!=null) {
			if(privateKeyFile.canRead()) {
				return URLHelper.getCharsFromURL(URLHelper.toURL(privateKeyFile));
			}
		}
		return null;
	}
	
	/**
	 * {@inheritDoc}
	 * @see ch.ethz.ssh2.ConnectionMonitor#connectionLost(java.lang.Throwable)
	 */
	@Override
	public void connectionLost(final Throwable reason) {
		if(connected.compareAndSet(true, false)) {
			if(reason==null || !started.get() || NORMAL_CLOSE_MESSAGE.equals(reason.getMessage())) {
				// Normal close
				log.info("Connection [{}] was closed", key);
			} else {
				// Not so normal
				log.error("Connection [{}] was lost. Starting reconnect task.", key, reason);
			}
		}
	}
	
	/**
	 * FIXME: this should actually do something
	 * {@inheritDoc}
	 * @see ch.ethz.ssh2.ServerHostKeyVerifier#verifyServerHostKey(java.lang.String, int, java.lang.String, byte[])
	 */
	@Override
	public boolean verifyServerHostKey(final String hostname, final int port, final String serverHostKeyAlgorithm, final byte[] serverHostKey) throws Exception {
		log.info("Verifying Host Ket from [{}:{}], Algo: [{}]", hostname, port, serverHostKeyAlgorithm);
		return true;
	}
	
	


	/**
	 * Returns the host to connect to
	 * @return the host
	 */
	public String getHost() {
		return host;
	}

	/**
	 * Returns the sshd listening port
	 * @return the sshPort
	 */
	public int getSshPort() {
		return sshPort;
	}

	/**
	 * Returns the the user to connect as
	 * @return the user
	 */
	public String getUser() {
		return user;
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format(KEY_TEMPLATE, user.trim(), host.trim(), sshPort);
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((host == null) ? 0 : host.hashCode());
		result = prime * result + sshPort;
		result = prime * result + ((user == null) ? 0 : user.hashCode());
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
		SSHConnection other = (SSHConnection) obj;
		if (host == null) {
			if (other.host != null)
				return false;
		} else if (!host.equals(other.host))
			return false;
		if (sshPort != other.sshPort)
			return false;
		if (user == null) {
			if (other.user != null)
				return false;
		} else if (!user.equals(other.user))
			return false;
		return true;
	}




	
	
}
