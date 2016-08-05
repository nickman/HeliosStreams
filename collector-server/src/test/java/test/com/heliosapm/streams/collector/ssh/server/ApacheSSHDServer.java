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
package test.com.heliosapm.streams.collector.ssh.server;

import java.io.File;
import java.security.Provider;
import java.security.PublicKey;
import java.security.Security;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.sshd.common.NamedFactory;
import org.apache.sshd.common.session.Session;
import org.apache.sshd.common.util.SecurityUtils;
import org.apache.sshd.common.util.net.SshdSocketAddress;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.auth.UserAuth;
import org.apache.sshd.server.auth.password.PasswordAuthenticator;
import org.apache.sshd.server.auth.password.UserAuthPasswordFactory;
import org.apache.sshd.server.auth.pubkey.PublickeyAuthenticator;
import org.apache.sshd.server.auth.pubkey.UserAuthPublicKeyFactory;
import org.apache.sshd.server.forward.ForwardingFilter;
import org.apache.sshd.server.keyprovider.AbstractGeneratorHostKeyProvider;
import org.apache.sshd.server.session.ServerSession;
import org.apache.sshd.server.shell.ProcessShellFactory;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import com.heliosapm.utils.io.StdInCommandHandler;

import test.com.heliosapm.streams.collector.ssh.auth.KeyDirectoryPublickeyAuthenticator;
import test.com.heliosapm.streams.collector.ssh.auth.PropFilePasswordAuthenticator;
import test.com.heliosapm.streams.collector.ssh.keys.UserAwarePublicKey;


/**
 * <p>Title: ApacheSSHDServer</p>
 * <p>Description: An sshd server for testing</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>test.com.heliosapm.streams.collector.ssh.server.ApacheSSHDServer</code></p>
 */

public class ApacheSSHDServer {
	/** Static class logger */
	static final Logger LOG = LogManager.getLogger(ApacheSSHDServer.class);
	/** The server instance */
	static final AtomicReference<SshServer> server = new AtomicReference<SshServer>(null);
	
	
	/**
	 * Launch the sshd server
	 * @param args None
	 */
	public static void main(String...args) {
		
		
		//Logger.getLogger(ChannelSession.class).setLevel(Level.WARN);
//		LogManager.getLogger("org.apache").setLevel(Level.WARN);
		//Logger.getLogger(ServerSession.class).setLevel(Level.WARN);
		//Logger.getLogger(SecurityUtils.class).setLevel(Level.WARN);

		SshServer sshd = server.get();
		if(sshd==null) {
			synchronized(server) {
				sshd = server.get();
				if(sshd!=null) {
					LOG.info("Server already running on port [" + sshd.getPort() + "]");
					return;
				}
				sshd = SshServer.setUpDefaultServer();
				server.set(sshd);
			}
		}		

		LOG.info("Starting SSHd Server");
		
		int port = -1;
		try {
			port = Integer.parseInt(args[0]);
		} catch (Exception e) {
			port = 0;
		}
		sshd.setPort(port);
		sshd.setHost("0.0.0.0");
		//LOG.info("Listening Port [" + port + "]");
		Provider provider = new BouncyCastleProvider();
		Security.addProvider(provider);
		List<NamedFactory<UserAuth>> userAuthFactories = new ArrayList<NamedFactory<UserAuth>>();
		
		userAuthFactories.add(new UserAuthPasswordFactory());		
		userAuthFactories.add(new UserAuthPublicKeyFactory());
		//sshd.setUserAuthFactories(userAuthFactories);
		final File hostKeySerFile = new File(new File(System.getProperty("java.io.tmpdir")),"hostkey.ser");
		hostKeySerFile.deleteOnExit();
		//final SimpleGeneratorHostKeyProvider hostKeyProvider = new SimpleGeneratorHostKeyProvider(hostKeySerFile);
		final AbstractGeneratorHostKeyProvider hostKeyProvider = SecurityUtils.createGeneratorHostKeyProvider(hostKeySerFile.toPath());
		hostKeyProvider.setAlgorithm("RSA");
		sshd.setKeyPairProvider(hostKeyProvider);
		sshd.setPasswordAuthenticator(NO_AUTH);
		sshd.setPublickeyAuthenticator(NO_KEY_AUTH);
        sshd.setTcpipForwardingFilter(new ForwardingFilter() {
            /**
			 * {@inheritDoc}
			 * @see org.apache.sshd.server.forward.ForwardingFilter#canForwardAgent(org.apache.sshd.common.session.Session)
			 */
			@Override
			public boolean canForwardAgent(final Session session) {
				return true;
			}

			/**
			 * {@inheritDoc}
			 * @see org.apache.sshd.server.forward.ForwardingFilter#canForwardX11(org.apache.sshd.common.session.Session)
			 */
			@Override
			public boolean canForwardX11(final Session session) {
				return true;
			}

			/**
			 * {@inheritDoc}
			 * @see org.apache.sshd.server.forward.ForwardingFilter#canListen(org.apache.sshd.common.util.net.SshdSocketAddress, org.apache.sshd.common.session.Session)
			 */
			@Override
			public boolean canListen(final SshdSocketAddress address, final Session session) {				
				return true;
			}

			/**
			 * {@inheritDoc}
			 * @see org.apache.sshd.server.forward.ForwardingFilter#canConnect(org.apache.sshd.server.forward.ForwardingFilter.Type, org.apache.sshd.common.util.net.SshdSocketAddress, org.apache.sshd.common.session.Session)
			 */
			@Override
			public boolean canConnect(final Type type, final SshdSocketAddress address, final Session session) {
				return true;
			}
        });
        
//		sshd.setPasswordAuthenticator(new PropFilePasswordAuthenticator("./src/test/resources/auth/password/credentials.properties"));
//		sshd.setPublickeyAuthenticator(new KeyDirectoryPublickeyAuthenticator("./src/test/resources/auth/keys"));
		
		
		
		if (System.getProperty("os.name").toLowerCase().contains("windows")) {
			boolean useBash = false;
			if(System.getenv().containsKey("Path")) {
				for(String pathEntry: System.getenv().get("Path").split(";")) {
					File bashFile = new File(pathEntry + File.separator + "bash.exe");
					if(bashFile.exists() && bashFile.canExecute()) {
						useBash = true;
						break;
					}
				}
			}
			if(useBash) {
				LOG.info("shell is bash");
				sshd.setShellFactory(new ProcessShellFactory(new String[] { "bash.exe", "-i", "-l"})); //EnumSet.of(ProcessShellFactory.TtyOptions.ONlCr)
			} else {
				LOG.info("shell is cmd");
				sshd.setShellFactory(new ProcessShellFactory(new String[] { "cmd.exe"})); // EnumSet.of(ProcessShellFactory.TtyOptions.Echo, ProcessShellFactory.TtyOptions.ICrNl, ProcessShellFactory.TtyOptions.ONlCr)
			}
			
		} else {
			sshd.setShellFactory(new ProcessShellFactory(new String[] { "/bin/sh", "-i", "-l" })); //EnumSet.of(ProcessShellFactory.TtyOptions.ONlCr)
		}
		
		try {
			sshd.start();
			LOG.info("Server started on port [" + sshd.getPort() + "]");
			StdInCommandHandler.getInstance().run();
		} catch (Exception e) {
			e.printStackTrace(System.err);
		}
	}
	
	/**
	 * Returns the port of the running server
	 * @return the port of the running server
	 */
	public static int getPort() {
		SshServer sshd = server.get();
		if(sshd==null) throw new IllegalStateException("The SSHd server is not running", new Throwable());
		return sshd.getPort();
	}
	
	
	/** Passthrough key authenticator. Always authenticates. */
	static final PublickeyAuthenticator NO_KEY_AUTH = new PublickeyAuthenticator() {
		@Override
		public boolean authenticate(String username, PublicKey key, ServerSession session) {
			return true;
		}		
	};	
	/** Passthrough password authenticator. Always authenticates. */
	static final PasswordAuthenticator NO_AUTH = new PasswordAuthenticator() {
		@Override
		public boolean authenticate(String username, String password, ServerSession session) {
			return true;
		}		
	};
	/** Property file driven password authenticator */
	static final PropFilePasswordAuthenticator PW_AUTH = new PropFilePasswordAuthenticator("./src/test/resources/ssh/auth/password/credentials.properties");
	/** Public key file driven authenticator */
	static final KeyDirectoryPublickeyAuthenticator KEY_AUTH = new KeyDirectoryPublickeyAuthenticator("./src/test/resources/ssh/auth/keys");
	
	
	/**
	 * Adds a public key to the directory
	 * @param key The key in string form
	 * @return the public key
	 */
	public static UserAwarePublicKey addPublicKey(String key) {
		return KEY_AUTH.addPublicKey(key);
	}
	
	
	/**
	 * Removes all authenticators and activates the NO_AUTH 
	 */
	public static void resetAuthenticators() {
		SshServer sshd = server.get();
		if(sshd==null) throw new IllegalStateException("The SSHd server is not running", new Throwable());
		sshd.setPasswordAuthenticator(NO_AUTH);
		sshd.setPublickeyAuthenticator(NO_KEY_AUTH);
	}
	
	/**
	 * Enables or disables the property file driven password authenticator
	 * @param active If true, enables the property file driven password authenticator, otherwise disables password based authentication
	 */
	public static void activatePasswordAuthenticator(boolean active) {
		SshServer sshd = server.get();
		if(sshd==null) throw new IllegalStateException("The SSHd server is not running", new Throwable());
		if(active) {
			sshd.setPasswordAuthenticator(PW_AUTH);
		} else {
			sshd.setPasswordAuthenticator(NO_AUTH);
		}
	}
	
	/**
	 * Enables or disables the key based authenticator
	 * @param active If true, enables the key authenticator, otherwise disables key based authentication
	 */
	public static void activateKeyAuthenticator(boolean active) {
		SshServer sshd = server.get();
		if(sshd==null) throw new IllegalStateException("The SSHd server is not running", new Throwable());
		if(active) {
			sshd.setPublickeyAuthenticator(KEY_AUTH);
		} else {
			sshd.setPublickeyAuthenticator(NO_KEY_AUTH);
		}
	}
	
	
	
	/**
	 * Stops the SSHd server immediately
	 */
	public static void stop() {
		stop(true);
	}
	
	
	/**
	 * Stops the SSHd server
	 * @param immediately If true, stops the server immediately, otherwise waits for pending requests.
	 */
	public static void stop(boolean immediately) {
		SshServer sshd = server.get();
		if(sshd==null) return;
		try {
			sshd.stop(immediately);
			server.set(null);
		} catch (Exception e) {
			throw new RuntimeException("Failed to stop SSHd server", e);
		}		
	}
	
	/**
	 * Indicates if the server is started
	 * @return true if the server is started, false otherwise
	 */
	public static boolean isStarted() {
		return server.get()!=null;
	}
	
	/**
	 * Recycles the server
	 */
	public static void restart() {
		try { stop(); } catch (Exception e) {/* No Op */}
		main();
	}
	
	
	/**
	 * Returns the algo list for the passed provider
	 * @param p the provder
	 * @return the algo list
	 */
	public static String getAlgoList(Provider p) {
		StringBuilder b = new StringBuilder();
		Set<String> types = new HashSet<String>();
		for(Provider.Service svc: p.getServices()) {
			types.add(svc.getType());
			if("KeyGenerator".equals(svc.getType())) 
			b.append("\n\t").append("[").append(svc.getType()).append("] ").append(svc.getAlgorithm());
		}
		System.out.println("Types:\n" + types);
//		for(Map.Entry<Object, Object> entry: p.entrySet()) {
//			String key = (String)entry.getKey();
//			String value = (String)entry.getValue();
//			if(key.startsWith("Cipher."))
//			b.append("\n\t[").append(key).append("]:").append(value);			
//			if(key!=null && key.startsWith("Cipher.") && !key.contains(" ")) {
//				key = key.split("\\.")[1];				
//				b.append("\n\t[").append(key).append("]:").append(value);
//			}
//		}
		return b.toString();
	}

}

