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
package test.com.heliosapm.streams.collector.ssh.conn;

import java.io.File;
import java.net.URL;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.heliosapm.streams.collector.ssh.SSHConnection;
import com.heliosapm.streams.collector.ssh.SSHTunnelManager;
import com.heliosapm.utils.lang.StringHelper;
import com.heliosapm.utils.reflect.PrivateAccessor;
import com.heliosapm.utils.url.URLHelper;

import test.com.heliosapm.streams.collector.BaseTest;
import test.com.heliosapm.streams.collector.ssh.server.ApacheSSHDServer;

/**
 * <p>Title: SSHConnectionTest</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>test.com.heliosapm.streams.collector.ssh.conn.SSHConnectionTest</code></p>
 */

public class SSHConnectionTest extends BaseTest {
	/** The resource path of the test json */
	public static final String TEST_JSON = "ssh/json/connections.json";
	/** The system property to set or get the test SSHD server's listening port */
	public static final String SSHD_PORT_PROP = "heliosapm.sshd.port";
	/** A reference to the tunnel manager */
	protected static final SSHTunnelManager tunnelManager = SSHTunnelManager.getInstance();
	
	/**
	 * Tests loading an array of SSHConnections from JSON
	 * @throws Exception thrown on any error
	 */
	@SuppressWarnings("static-method")
	@Test
	public void testBasicConnUnmarshall() throws Exception {
		try {
			System.setProperty(SSHD_PORT_PROP, "22");
			final URL url = SSHConnectionTest.class.getClassLoader().getResource(TEST_JSON);
			log("Test JSON URL:" + url);
			final SSHConnection[] connections = SSHTunnelManager.parseConnections(url);
			log(Arrays.deepToString(connections));
			final ArrayNode nodes = (ArrayNode)OBJECT_MAPPER.readTree(
				StringHelper.resolveTokens(
					URLHelper.getTextFromURL(url)
				)
			).get("connections");
			Assert.assertEquals("Number of conns != number of json nodes", nodes.size(), connections.length);
			for(int i = 0; i < connections.length; i++) {
				validateConnection(connections[i], nodes.get(i));
			}
		} finally {
			System.clearProperty(SSHD_PORT_PROP);
		}
	}
	
	/**
	 * Tests loading an array of SSHConnections from JSON and establishing a basic connection
	 * @throws Exception thrown on any error
	 */
	@SuppressWarnings("static-method")
	@Test
	public void testBasicConnects() throws Exception {
		final ApacheSSHDServer sshdServer = ApacheSSHDServer.getInstance(); 
		try {			
			final URL url = SSHConnectionTest.class.getClassLoader().getResource(TEST_JSON);
			final SSHConnection[] connections = SSHTunnelManager.parseConnections(url);
			log(Arrays.deepToString(connections));
			for(int i = 0; i < connections.length; i++) {
				connections[i].connect();
				Assert.assertTrue("Not connected: [" + connections[i] + "]", connections[i].isConnected());
			}
		} finally {
			sshdServer.stop(true);
			System.clearProperty(SSHD_PORT_PROP);
		}
	}
	
	/**
	 * Tests loading an array of SSHConnections from JSON and establishing a basic connection
	 * @throws Exception thrown on any error
	 */
	@SuppressWarnings("static-method")
	@Test
	public void testBasicConnectAndAuthenticate() throws Exception {
		final ApacheSSHDServer sshdServer = ApacheSSHDServer.getInstance();
		sshdServer.activateKeyAuthenticator(true);
		sshdServer.activatePasswordAuthenticator(true);
		try {			
			final URL url = SSHConnectionTest.class.getClassLoader().getResource(TEST_JSON);
			final SSHConnection[] connections = SSHTunnelManager.parseConnections(url);
			log(Arrays.deepToString(connections));
			for(int i = 0; i < connections.length; i++) {
				connections[i].authenticate();
				Assert.assertTrue("Not authenticated: [" + connections[i] + "]", connections[i].isAuthenticated());
			}
		} finally {
			sshdServer.stop(true);
			System.clearProperty(SSHD_PORT_PROP);
		}
	}
	
	/**
	 * Validates that the passed connection has the same values as the passed json node
	 * @param conn The connection to test
	 * @param jsonNode The json to test against
	 * @throws Exception thrown on any error
	 */
	protected static void validateConnection(final SSHConnection conn, final JsonNode jsonNode) throws Exception {
		try {
			Assert.assertNotNull("Connection was null", conn);
			Assert.assertNotNull("Node was null", jsonNode);
			Assert.assertEquals("Mismatched user on connection vs. json on [" + conn + "]", jsonNode.get("user").textValue(), conn.getUser());
			Assert.assertEquals("Mismatched host on connection vs. json on [" + conn + "]", jsonNode.get("host").textValue(), conn.getHost());
			Assert.assertEquals("Mismatched ssh port on connection vs. json on [" + conn + "]", jsonNode.get("sshport").asInt(), conn.getSshPort());
			if(jsonNode.has("password")) Assert.assertEquals("Mismatched password on connection vs. json on [" + conn + "]", jsonNode.get("password").textValue(), PrivateAccessor.getFieldValue(conn, "password"));
			if(jsonNode.has("pkey")) Assert.assertArrayEquals("Mismatched pkey on connection vs. json on [" + conn + "]", jsonNode.get("pkey").textValue().toCharArray(), (char[])PrivateAccessor.getFieldValue(conn, "privateKey"));
			if(jsonNode.has("pphrase")) Assert.assertEquals("Mismatched passphrase on connection vs. json on [" + conn + "]", jsonNode.get("pphrase").textValue(), PrivateAccessor.getFieldValue(conn, "passPhrase"));
			if(jsonNode.has("pkeyfile")) Assert.assertEquals("Mismatched passphrase on connection vs. json on [" + conn + "]", new File(jsonNode.get("pkeyfile").textValue()), (PrivateAccessor.getFieldValue(conn, "privateKeyFile")));
			if(jsonNode.has("connectTimeout")) {
				Assert.assertEquals("Mismatched connectTimeout on connection vs. json on [" + conn + "]", jsonNode.get("connectTimeout").longValue(), ((Integer)(PrivateAccessor.getFieldValue(conn, "connectTimeout"))).intValue());
			} else {
				Assert.assertEquals("Mismatched connectTimeout on connection vs. default on [" + conn + "]", SSHConnection.DEFAULT_CONNECT_TIMEOUT, ((Integer)(PrivateAccessor.getFieldValue(conn, "connectTimeout"))).intValue());
			}
			if(jsonNode.has("kexTimeout")) {
				Assert.assertEquals("Mismatched connectTimeout on connection vs. json on [" + conn + "]", jsonNode.get("kexTimeout").longValue(), ((Integer)(PrivateAccessor.getFieldValue(conn, "kexTimeout"))).intValue());
			} else {
				Assert.assertEquals("Mismatched kexTimeout on connection vs. default on [" + conn + "]", SSHConnection.DEFAULT_KEX_TIMEOUT, ((Integer)(PrivateAccessor.getFieldValue(conn, "kexTimeout"))).intValue());
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			throw ex;
		}
	}
	
//	/** The host to connect to */
//	@JsonProperty(value="host", required=true)
//	protected String host = null;
//	/** The ssh listener port */
//	@JsonProperty(value="sshport", defaultValue="22")
//	protected int sshPort = 22;
//	/** The user to connect as */
//	@JsonProperty(value="user", required=true)
//	protected String user = null;
//	/** The user password */
//	@JsonProperty(value="password")
//	protected String password = null;
//	/** The ssh private key */
//	@JsonProperty(value="pkey")
//	protected char[] privateKey = null;
//	/** The ssh private key file */
//	@JsonProperty(value="pkeyfile")
//	protected File privateKeyFile = null;	
//	/** The ssh private key passphrase */
//	@JsonProperty(value="pphrase")
//	protected String passPhrase = null;
	
}
