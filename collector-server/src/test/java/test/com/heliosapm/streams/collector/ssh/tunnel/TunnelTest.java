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
package test.com.heliosapm.streams.collector.ssh.tunnel;

import java.net.URL;
import java.util.Arrays;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.jmxmp.JMXMPConnectorServer;

import org.junit.Assert;
import org.junit.Test;

import com.heliosapm.streams.collector.ssh.SSHConnection;
import com.heliosapm.streams.collector.ssh.SSHTunnelManager;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.reflect.PrivateAccessor;

import ch.ethz.ssh2.LocalPortForwarder;
import test.com.heliosapm.streams.collector.BaseTest;
import test.com.heliosapm.streams.collector.ssh.conn.SSHConnectionTest;
import test.com.heliosapm.streams.collector.ssh.server.ApacheSSHDServer;

/**
 * <p>Title: TunnelTest</p>
 * <p>Description: Basic tunnel marshalling tests</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>test.com.heliosapm.streams.collector.ssh.tunnel.TunnelTest</code></p>
 */

public class TunnelTest extends BaseTest {
	
	/** The resource path of the test json */
	public static final String TEST_JSON = "ssh/json/connections.json";
	/** The system property to set or get the test SSHD server's listening port */
	public static final String SSHD_PORT_PROP = "heliosapm.sshd.port";
	/** A reference to the tunnel manager */
	protected static final SSHTunnelManager tunnelManager = SSHTunnelManager.getInstance();
	

	/**
	 * Tests loading an array of SSHConnections from JSON and establishing a basic connection
	 * @throws Exception thrown on any error
	 */
	@SuppressWarnings("static-method")
	@Test
	public void testJMXMPTunnel() throws Exception {
		final ApacheSSHDServer sshdServer = ApacheSSHDServer.getInstance();
		final JMXMPConnectorServer jmxmp = JMXHelper.fireUpJMXMPServer(0);
		final int remotePort = jmxmp.getAddress().getPort();
		final String agentId = JMXHelper.getAgentId();
		log("Actual Agent ID: [" + agentId + "]");
		try {			
			final URL url = SSHConnectionTest.class.getClassLoader().getResource(TEST_JSON);
			final SSHConnection[] connections = SSHTunnelManager.parseConnections(url);
			log(Arrays.deepToString(connections));
			for(int i = 0; i < connections.length; i++) {
				connections[i].authenticate();
				LocalPortForwarder lpf = (LocalPortForwarder)PrivateAccessor.invoke(connections[i], "createPortForward", new Object[]{0, "localhost", remotePort}, int.class, String.class, int.class);
				final int localPort = lpf.getLocalPort();
				final JMXConnector jmxConnector = JMXHelper.getJMXConnection("service:jmx:jmxmp://localhost:" + localPort, true, null);
				final MBeanServerConnection server = jmxConnector.getMBeanServerConnection();
				final String readAgentId = JMXHelper.getAgentId(server);
				log("Agent ID: for [" + connections[i] + "] : [" + agentId + "]");
				Assert.assertEquals("Mismatch on expected agent ids", agentId, readAgentId);
				jmxConnector.close();
			}
		} finally {
			jmxmp.stop();
			sshdServer.stop(true);
			System.clearProperty(SSHD_PORT_PROP);
		}
	}

	

	
}



// LocalPortForwarder createPortForward(final int localPort, final String connectHost, final int connectPort)
