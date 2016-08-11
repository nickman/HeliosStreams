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
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.ObjectName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.heliosapm.streams.json.JSONOps;
import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.jmx.JMXManagedScheduler;
import com.heliosapm.utils.jmx.SharedNotificationExecutor;
import com.heliosapm.utils.tuples.NVP;
import com.heliosapm.utils.url.URLHelper;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.ConnectionInfo;
import ch.ethz.ssh2.ConnectionMonitor;
import ch.ethz.ssh2.LocalPortForwarder;
import ch.ethz.ssh2.ServerHostKeyVerifier;

/**
 * <p>Title: SSHConnection</p>
 * <p>Description: Represents an SSH connection</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.ssh.SSHConnection</code></p>
 */
@JsonDeserialize(using=SSHConnection.SSHConnectionDeserializer.class)
@JsonSerialize(using=SSHConnection.SSHConnectionSerializer.class)
public class SSHConnection implements ConnectionMonitor, Runnable, ServerHostKeyVerifier {
	
	/** A cache of connections keyed by <b><code>&lt;user&gt@&lt;host&gt:&lt;sshportr&gt</code></b>. */
	private static final Cache<String, SSHConnection> connections = CacheBuilder.newBuilder()
			.concurrencyLevel(Runtime.getRuntime().availableProcessors())
			.initialCapacity(256)
			.weakValues()
			.removalListener(new RemovalListener<String, SSHConnection>() {
				@Override
				public void onRemoval(RemovalNotification<String, SSHConnection> notification) {
					try { notification.getValue().stop(false); } catch (Exception x) {/* No Op */}
					
				}
			
			})
			.build();
	
	
	/** to string template */
	public static final String TOSTRING_TEMPLATE = "%s@%s:%s, cto:%s, kto:%s, tun:%s";
	/** Connection key template */
	public static final String KEY_TEMPLATE = "%s@%s:%s";
	
	/** The throwable message when a normal connection close occurs */
	private static final String NORMAL_CLOSE_MESSAGE = "Closed due to user request.";
	
	/** The reconnect thread pool JMX ObjectName */
	public static final ObjectName THREAD_POOL_OBJECT_NAME = JMXHelper.objectName("com.heliosapm.ssh:service=ReconnectScheduler");
	
	/** The default connect timeout in ms. */
	public static final int DEFAULT_CONNECT_TIMEOUT = 10000;
	/** The default kex timeout in ms. */
	public static final int DEFAULT_KEX_TIMEOUT = 10000;
	
	
	/** The reconnect thread pool scheduler */	
	private static final JMXManagedScheduler reconnectScheduler = new JMXManagedScheduler(THREAD_POOL_OBJECT_NAME, "SSHReconnector", 4, true);
	
	
	/** The host to connect to */
	protected String host = null;
	/** The ssh listener port */
	protected int sshPort = 22;
	/** The user to connect as */
	protected String user = null;
	/** The user password */
	protected String password = null;
	/** The ssh private key */
	protected char[] privateKey = null;
	/** The ssh private key file */
	protected File privateKeyFile = null;	
	/** The ssh private key passphrase */
	protected String passPhrase = null;
	/** The ssh connect timeout in ms. */
	protected int connectTimeout = 10000;
	/** The ssh kex (key verification) timeout in ms. */
	protected int kexTimeout = 10000;
	/** The port tunneling requests attached to this connection */
	protected final NonBlockingHashMap<String, LocalPortForwardRequest> tunnels = new NonBlockingHashMap<String, LocalPortForwardRequest>(); 
	

	/** The connection key */
	protected String key = null;
	/** The connection log */
	protected final Logger log = LogManager.getLogger(getClass());
	
	/** The authentication method used to authenticate */
	protected AtomicReference<AuthenticationMethod> authMethod  = new AtomicReference<AuthenticationMethod>(null); 
	
	
	
	/** The SSH connection */
	protected Connection connection = null;
	/** The SSH connection's info */
	protected ConnectionInfo connectionInfo = null;
	/** Indicates if the connection is connected */
	protected final AtomicBoolean connected = new AtomicBoolean(false);	
	/** Indicates if the connection is started */
	protected final AtomicBoolean started = new AtomicBoolean(false);
	/** The reconnect schedule handle for this connection */
	protected volatile ScheduledFuture<?> scheduleHandle = null;
	/** A set of connection listeners */
	protected final Set<SSHConnectionListener> connectionListeners = new CopyOnWriteArraySet<SSHConnectionListener>();
	/** The notification executor */
	protected final SharedNotificationExecutor notifExecutor = SharedNotificationExecutor.getInstance();
	
	/**
	 * Creates a new SSHConnection
	 */
	private SSHConnection() {
		key = null;
		connection = null;
	}
	
	/**
	 * Creates a new SSHConnection. For JSON deser only.
	 * @param key the key
	 * @param connection the connection
	 */
	private SSHConnection(final String key, final Connection connection) {
		this.key = key;
		this.connection = connection;
		connection.addConnectionMonitor(this);
	}

	
	public static void main(String[] args) {
		try {
			log("SSHConnection Test");
			final int jmxPort = JMXHelper.fireUpJMXMPServer(36636).getAddress().getPort();
			log("JMXMP Port:" + jmxPort);
			
			//SSHConnection conn = SSHConnection.getConnection("localhost", 45803, "fred", "flintstone");
			SSHConnection conn = SSHConnection.getConnection("localhost", 45803, "fred", URLHelper.getCharsFromURL("./src/test/resources/ssh/auth/keys/fred_rsa"), "the moon is a balloon");
			conn.connection.connect(conn);
			log("Connected");
			log("ConnAuths Available:" + Arrays.toString(conn.connection.getRemainingAuthMethods(conn.user)));
			log("Authenticated:" + AuthenticationMethod.auth(conn));
			LocalPortForwarder lpf = conn.connection.createLocalPortForwarder(28374, "127.0.0.1", jmxPort);
			log("LocalPortForwarder Started:" + lpf);
			StdInCommandHandler.getInstance().run();
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
		}
		
	}
	
	public static void log(Object msg) {
		System.out.println(msg);
	}
	
	/**
	 * Acquires the existing connection for the passed host, port and user, returning null if one does not exist
	 * @param host The host to connect to
	 * @param sshPort The sshd listening port
	 * @param user The user to connect as
	 * @return the connection or null if one did not exist
	 */
	public static SSHConnection getConnection(final String host, final int sshPort, final String user) {
		if(host==null || host.trim().isEmpty()) throw new IllegalArgumentException("The host name was null or empty");
		if(user==null || user.trim().isEmpty()) throw new IllegalArgumentException("The user name was null or empty");
		if(sshPort < 1 || sshPort > 65535) throw new IllegalArgumentException("The ssh port number [" + sshPort + "] is invalid");
		final String key = String.format(KEY_TEMPLATE, user.trim(), host.trim(), sshPort);
		return connections.getIfPresent(key);
	}
	
	/**
	 * Acquires the existing connection for the host, port and user encoded in the passed key in the format <b><code>&lt;user&gt@&lt;host&gt:&lt;sshportr&gt</code></b>. 
	 * @param key The connection key
	 * @return the connection or null if one did not exist
	 */
	public static SSHConnection getConnection(final String key) {
		if(key==null || key.trim().isEmpty()) throw new IllegalArgumentException("The key was null or empty");
		return connections.getIfPresent(key.trim());
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
		try {
			return connections.get(key, new Callable<SSHConnection>(){
				@Override
				public SSHConnection call() throws Exception {				
					return new SSHConnection(host, sshPort, user, password);
				}
			});
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
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
		try {
			return connections.get(key, new Callable<SSHConnection>(){
				@Override
				public SSHConnection call() throws Exception {				
					return new SSHConnection(host, sshPort, user, privateKeyFileName);
				}
			});
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
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
		try {
			return connections.get(key, new Callable<SSHConnection>(){
				@Override
				public SSHConnection call() throws Exception {				
					return new SSHConnection(host, sshPort, user, privateKey, passPhrase);
				}
			});
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
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
		key = String.format(KEY_TEMPLATE, user, host, sshPort);
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
		key = String.format(KEY_TEMPLATE, user, host, sshPort);
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
		key = String.format(KEY_TEMPLATE, user, host, sshPort);
		connection = new Connection(host, sshPort);
		connection.addConnectionMonitor(this);
	}
	
	
	/**
	 * Creates a port forward to the specified connect host and connect port through this connection
	 * @param localPort The local binding port or zero for an ephemeral port
	 * @param connectHost The host to connect to
	 * @param connectPort The port to connect to
	 * @return the created LocalPortForwarder
	 */
	LocalPortForwarder createPortForward(final int localPort, final String connectHost, final int connectPort) {
		if(!connected.get()) throw new IllegalStateException("Failed to create port forward [" + localPort + "-->" + connectHost + ":" + connectPort + "] as connection is closed");
		if(connectHost==null || connectHost.trim().isEmpty()) throw new IllegalArgumentException("The connect host name was null or empty");
		if(localPort < 0 || sshPort > 65535) throw new IllegalArgumentException("The local port number [" + localPort + "] is invalid");
		if(connectPort < 1 || connectPort > 65535) throw new IllegalArgumentException("The connect port number [" + connectPort + "] is invalid");
		try {
			return connection.createLocalPortForwarder(localPort, connectHost.trim(), connectPort);
		} catch (IOException iex) {
			throw new RuntimeException("Failed to create port forward to [" + connectHost + ":" + connectPort + "]", iex);
		}
	}
	
	/**
	 * Creates a port forward to the same host this connection is to and the specified connect port through this connection
	 * @param localPort The local binding port or zero for an ephemeral port
	 * @param connectPort The port to connect to
	 * @return the created LocalPortForwarder
	 */
	LocalPortForwarder createPortForward(final int localPort, final int connectPort) {
		return createPortForward(localPort, host, connectPort);
	}
	
	/**
	 * Creates a port forward from the passed request
	 * @param req a port forward definition
	 * @return the port forward
	 */
	LocalPortForwarder createPortForward(final LocalPortForwardRequest req) {
		return createPortForward(req.getLocalPort(), req.getRemoteHost(), req.getRemotePort());
	}

	
	/**
	 * Registers a connection listener on this connection
	 * @param listener the listener to register
	 */
	public void addConnectionListener(final SSHConnectionListener listener) {
		if(listener!=null) {
			connectionListeners.add(listener);
		}
	}
	
	/**
	 * Unregisters a connection listener from this connection
	 * @param listener the listener to unregister
	 */
	public void removeConnectionListener(final SSHConnectionListener listener) {
		if(listener!=null) {
			connectionListeners.remove(listener);
		}
	}
	
	
	/**
	 * Indicates if this connection is connected
	 * @return true if this connection is connected, false otherwise
	 */
	public boolean isConnected() {
		return connected.get();
	}
	
	/**
	 * Indicates if this connection is authenticated
	 * @return true if this connection is authenticated, false otherwise
	 */
	public boolean isAuthenticated() {
		return connected.get() && connection.isAuthenticationComplete();
	}
	
	/**
	 * Issues a connect with no auth if the connection is not connected 
	 */
	public void connect() {
		if(connection==null) {
			connection = new Connection(host, sshPort);
			connection.addConnectionMonitor(this);
			key = String.format(KEY_TEMPLATE, user, host, sshPort);
		}
		if(connected.compareAndSet(false, true)) {
			try {
				connectionInfo = connection.connect(this, connectTimeout, kexTimeout);				
			} catch (Exception ex) {
				try { connection.close(); } catch (Exception x) {/* No Op */}
				connected.set(false);
				throw new RuntimeException("Failed to connect [" + this + "]", ex);
			} 
		}
	}
	
	
	
	/**
	 * Attempts to authenticate this connection
	 */
	public void authenticate() {
		connect();			
		log.info("Connected to [{}], starting authentication", key);
		final NVP<Boolean, AuthenticationMethod> authResult = AuthenticationMethod.auth(this); 
		if(authResult.getKey()) {
			authMethod.set(authResult.getValue());
			if(scheduleHandle!=null) {
				scheduleHandle.cancel(false);
				scheduleHandle = null;
			}
			fireConnectionConnected();
		} else {
			authMethod.set(null);
			log.warn("Authentication on SSHConnection [{}] failed", key);
		}		
	}
	
	/**
	 * <p>Starts a reconnect attempt</p>
	 * {@inheritDoc}
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		try {
			authenticate();
		} catch (Exception ex) {
			if(ex instanceof InterruptedException) {
				log.info("Reconnect task for [{}] interrupted while running", key);
			}
		}
	}
	
	void start() throws Exception {
		if(started.compareAndSet(false, true)) {
			scheduleHandle = reconnectScheduler.scheduleWithFixedDelay(this, 2, 15, TimeUnit.SECONDS);
			fireConnectionStarted();
		}
	}
	
	void stop() {
		stop(true);
	}
	
	void stop(final boolean fire) {
		if(started.compareAndSet(true, false)) {
			try {
				if(scheduleHandle!=null) try { scheduleHandle.cancel(true); } catch (Exception x) {/* No Op */}
				scheduleHandle = null;
				try { connection.close(); } catch (Exception x) {/* No Op */}
				fireConnectionStopped();
			} finally {
				if(fire)connections.invalidate(key);
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
		authMethod.set(null);
		if(connected.compareAndSet(true, false)) {
			fireConnectionDisconnected();
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
		log.info("Verifying Host Key from [{}:{}], Algo: [{}]", hostname, port, serverHostKeyAlgorithm);
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
	 * Notifies all registered listeners that this connection started
	 */
	private void fireConnectionStarted() {
		if(!connectionListeners.isEmpty()) {
			for(final SSHConnectionListener listener: connectionListeners) {
				notifExecutor.execute(new Runnable(){
					@Override
					public void run() {
						listener.onStarted(key);
					}
				});
			}
		}
	}
	
	/**
	 * Notifies all registered listeners that this connection stopped
	 */
	private void fireConnectionStopped() {
		if(!connectionListeners.isEmpty()) {
			for(final SSHConnectionListener listener: connectionListeners) {
				notifExecutor.execute(new Runnable(){
					@Override
					public void run() {
						listener.onStopped(key);
					}
				});
			}
		}
	}
	
	/**
	 * Notifies all registered listeners that this connection [re-]connected
	 */
	private void fireConnectionConnected() {
		if(!connectionListeners.isEmpty()) {
			for(final SSHConnectionListener listener: connectionListeners) {
				notifExecutor.execute(new Runnable(){
					@Override
					public void run() {
						listener.onConnected(key);
					}
				});
			}
		}
	}
	
	/**
	 * Notifies all registered listeners that this connection disconnected
	 */
	private void fireConnectionDisconnected() {
		if(!connectionListeners.isEmpty()) {
			for(final SSHConnectionListener listener: connectionListeners) {
				notifExecutor.execute(new Runnable(){
					@Override
					public void run() {
						listener.onDisconnected(key);
					}
				});
			}
		}
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format(TOSTRING_TEMPLATE, user.trim(), host.trim(), sshPort, connectTimeout, kexTimeout, tunnels.size());
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

	/**
	 * Returns the connection timeout in ms.
	 * @return the connection timeout in ms.
	 */
	public int getConnectTimeout() {
		return connectTimeout;
	}

	/**
	 * Sets the connection timeout in ms.
	 * @param connectTimeout the connect timeout to set
	 */
	public void setConnectTimeout(final int connectTimeout) {
		if(connectTimeout < 0) throw new IllegalArgumentException("The connection timeout [" + connectTimeout + "] is invalid");
		this.connectTimeout = connectTimeout;
	}

	/**
	 * Returns the kex timeout in ms.
	 * @return the kex timeout in ms.
	 */
	public int getKexTimeout() {
		return kexTimeout;
	}

	/**
	 * Sets the kex timeout in ms.
	 * @param kexTimeout the kex timeout to set
	 */
	public void setKexTimeout(int kexTimeout) {
		if(kexTimeout < 0) throw new IllegalArgumentException("The kex timeout [" + connectTimeout + "] is invalid");
		this.kexTimeout = kexTimeout;
	}
	
	/**
	 * Returns the available authentication methods
	 * @return an array of the available authentication methods
	 * @see ch.ethz.ssh2.Connection#getRemainingAuthMethods(java.lang.String)
	 */
	public String[] getRemainingAuthMethods() {
		if(!connected.get()) throw new IllegalStateException("Not connected");
		try {
			return connection.getRemainingAuthMethods(user);
		} catch (IOException iex) {
			throw new RuntimeException("Failed to get RemainingAuthMethods for [" + this + "]", iex);
		}
	}
	


	/**
	 * Returns the authentication method used to authenticate or null if not authenticated
	 * @return the authentication method used to authenticate or null if not authenticated
	 */
	public AuthenticationMethod getAuthenticationMethod() {
		return authMethod.get();
	}
	
	/**
	 * Returns a map of this connection's LocalPortForwardRequests keyed by the request key 
	 * @return a map of this connection's LocalPortForwardRequests
	 */
	public Map<String, LocalPortForwardRequest> getTunnels() {
		return new HashMap<String, LocalPortForwardRequest>(tunnels);
	}
	
	/** Sharable JSON deserializer for SSHConnections */
	public static final JsonDeserializer<SSHConnection> DESER = new SSHConnectionDeserializer();
	/** Sharable JSON serializer for SSHConnections */
	public static final JsonSerializer<SSHConnection> SER = new SSHConnectionSerializer();
	
	static {
		JSONOps.registerSerialization(SSHConnection.class, DESER, SER);
	}
	
	

	/**
	 * <p>Title: SSHConnectionSerializer</p>
	 * <p>Description: JSON serializer for SSHConnections</p> 
	 * <p>Company: Helios Development Group LLC</p>
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>com.heliosapm.streams.collector.ssh.SSHConnection.SSHConnectionSerializer</code></p>
	 */
	public static class SSHConnectionSerializer extends JsonSerializer<SSHConnection> {
		/**
		 * {@inheritDoc}
		 * @see com.fasterxml.jackson.databind.JsonSerializer#serialize(java.lang.Object, com.fasterxml.jackson.core.JsonGenerator, com.fasterxml.jackson.databind.SerializerProvider)
		 */
		@Override
		public void serialize(final SSHConnection value, final JsonGenerator gen, final SerializerProvider serializers) throws IOException, JsonProcessingException {
			gen.writeStartObject();
			gen.writeStringField("host", value.host);
			gen.writeNumberField("sshport", value.sshPort);
			gen.writeStringField("user", value.user);
			if(value.password!=null) {
				gen.writeStringField("password", value.password);
			}
			if(value.privateKey!=null) {				
				gen.writeStringField("pkey", new String(value.privateKey));
			}
			if(value.privateKeyFile!=null) {				
				gen.writeStringField("pkeyfile", value.privateKeyFile.getAbsolutePath());
			}
			if(value.passPhrase!=null) {				
				gen.writeStringField("pphrase", value.passPhrase);
			}
			gen.writeNumberField("connectTimeout", value.connectTimeout);
			gen.writeNumberField("kexTimeout", value.kexTimeout);
			if(!value.tunnels.isEmpty()) {
				gen.writeArrayFieldStart("tunnels");
				for(LocalPortForwardRequest lr: value.tunnels.values()) {
					gen.writeObject(lr);
				}
				gen.writeEndArray();
			}
			gen.writeEndObject();			
		}
	}
	
	/**
	 * <p>Title: SSHConnectionDeserializer</p>
	 * <p>Description: JSON deserializer for SSHConnections</p> 
	 * <p>Company: Helios Development Group LLC</p>
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>com.heliosapm.streams.collector.ssh.SSHConnection.SSHConnectionDeserializer</code></p>
	 */
	public static class SSHConnectionDeserializer extends JsonDeserializer<SSHConnection> {
		/**
		 * {@inheritDoc}
		 * @see com.fasterxml.jackson.databind.JsonDeserializer#deserialize(com.fasterxml.jackson.core.JsonParser, com.fasterxml.jackson.databind.DeserializationContext)
		 */
		@Override
		public SSHConnection deserialize(final JsonParser p, final DeserializationContext ctxt) throws IOException, JsonProcessingException {
			final JsonNode node = p.getCodec().readTree(p);
			final String host = node.get("host").textValue();
			final String user = node.get("user").textValue();
			final int sshPort = node.has("sshport") ? node.get("sshport").intValue() : 22;
			
			final String key = String.format(KEY_TEMPLATE, user, host, sshPort);
			final Connection connection = new Connection(host, sshPort);
			
			final SSHConnection sshConn = new SSHConnection(key, connection);
			sshConn.host = host;
			sshConn.user = user;
			sshConn.sshPort = sshPort;
			sshConn.password = node.has("password") ? node.get("password").textValue() : null;
			sshConn.privateKey = node.has("pkey") ? node.get("pkey").textValue().toCharArray() : null;
			sshConn.passPhrase = node.has("passphrase") ? node.get("passphrase").textValue() : null;
			sshConn.privateKeyFile = node.has("pkeyfile") ? new File(node.get("pkeyfile").textValue()) : null;
			sshConn.connectTimeout = node.has("connectTimeout") ? node.get("connectTimeout").intValue() : DEFAULT_CONNECT_TIMEOUT;
			sshConn.kexTimeout = node.has("kexTimeout") ? node.get("kexTimeout").intValue() : DEFAULT_KEX_TIMEOUT;
			
			if(node.has("tunnels")) {
				final ArrayNode an = (ArrayNode)node.get("tunnels");
				if(an.size() > 0) {
					for(JsonNode n: an) {
						LocalPortForwardRequest req = JSONOps.parseToObject(n, LocalPortForwardRequest.class);
						sshConn.tunnels.put(req.getKey(), req);
					}
				}
			}
			connections.put(key, sshConn);
			return sshConn;
		}
	}

	
	
}
