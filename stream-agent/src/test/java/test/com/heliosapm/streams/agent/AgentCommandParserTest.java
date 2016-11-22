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
package test.com.heliosapm.streams.agent;

import org.junit.Assert;
import org.junit.Test;
import org.kohsuke.args4j.CmdLineException;

import com.heliosapm.streams.agent.cl.AgentCommandParser;
import com.heliosapm.streams.agent.cl.JMXMPSpec;
import com.heliosapm.streams.agent.endpoint.Endpoint;

/**
 * <p>Title: AgentCommandParserTest</p>
 * <p>Description: Agent command string parsing tests</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>test.com.heliosapm.streams.agent.AgentCommandParserTest</code></p>
 */

public class AgentCommandParserTest extends BaseTest {
	
	/**
	 * Tests a full command with one spec and one endpoint
	 * @throws Exception on any error
	 */
	@Test
	public void tesFullCommandOneSpecOneEndpoint() throws Exception {
		final String spec = "1892:0.0.0.0:DefaultDomain";
		final String ep = "foobar-23s/foobarproc";
		final String zookeep = "server5:2181,server6:2181";
		final String host = "host-foo.bar.dc1";
		final String app = "FooBar";
		final JMXMPSpec[] specs = JMXMPSpec.parse(spec);
		final Endpoint[] endpoints = Endpoint.fromStrings(ep);
		final String cmd = String.format("--specs %s --zk %s --app %s --host %s --ep %s", spec, zookeep, app, host, ep);
		test(cmd, zookeep, app, host, specs, endpoints);
	}
	
	/**
	 * Tests a full command with one spec and two endpoints
	 * @throws Exception on any error
	 */
	@Test
	public void tesFullCommandOneSpecTwoEndpoints() throws Exception {
		final String spec = "1892:0.0.0.0:DefaultDomain";
		final String ep1 = "foobar-23s/foobarproc";
		final String ep2 = "snafu-1m";
		final String zookeep = "server5:2181,server6:2181";
		final String host = "host-foo.bar.dc1";
		final String app = "FooBar";
		final JMXMPSpec[] specs = JMXMPSpec.parse(spec);
		final Endpoint[] endpoints = Endpoint.fromStrings(ep1, ep2);
		final String cmd = String.format("--specs %s --zk %s --app %s --host %s --ep %s --ep %s", spec, zookeep, app, host, ep1, ep2);
		test(cmd, zookeep, app, host, specs, endpoints);
	}
	
	/**
	 * Tests a full command with one spec and one endpoint, no zookeep, which will throw an exception
	 * @throws Exception on any error
	 */
	@Test
	public void tesFullCommandOneSpecOneEndpointNoZookeep() throws Exception {
		final String spec = "1892:0.0.0.0:DefaultDomain";
		final String ep = "foobar-23s/foobarproc";
		final String host = "host-foo.bar.dc1";
		final String app = "FooBar";
		final JMXMPSpec[] specs = JMXMPSpec.parse(spec);
		final Endpoint[] endpoints = Endpoint.fromStrings(ep);
		final String cmd = String.format("--specs %s --app %s --host %s --ep %s", spec, app, host, ep);
		try {
			test(cmd, null, app, host, specs, endpoints);
			Assert.fail("Expected RuntimeException(CmdLineException)");
		} catch (Exception ex) {
			Assert.assertTrue("Not a RuntimeException", (ex instanceof RuntimeException));
			final Throwable inner = ex.getCause();
			Assert.assertNotNull("Inner was null", inner);
			Assert.assertTrue("Not a CmdLineException", (inner instanceof CmdLineException));
		}
	}
	
	/**
	 * Tests a full command with two specs and two endpoints
	 * @throws Exception on any error
	 */
	@Test
	public void tesFullCommandTwoSpecsTwoEndpoints() throws Exception {
		final String spec = "1892:0.0.0.0:DefaultDomain,1893:127.0.0.1:jboss";
		final String ep1 = "foobar-23s/foobarproc";
		final String ep2 = "snafu-1m";
		final String zookeep = "server5:2181,server6:2181";
		final String host = "host-foo.bar.dc1";
		final String app = "FooBar";
		final JMXMPSpec[] specs = JMXMPSpec.parse(spec);
		final Endpoint[] endpoints = Endpoint.fromStrings(ep1, ep2);
		final String cmd = String.format("--specs %s --zk %s --app %s --host %s --ep %s --ep %s", spec, zookeep, app, host, ep1, ep2);
		test(cmd, zookeep, app, host, specs, endpoints);
	}
	
	/**
	 * Tests a command with one spec only
	 * @throws Exception on any error
	 */
	@Test
	public void tesCommandOneSpec() throws Exception {
		final String spec = "1892:0.0.0.0:DefaultDomain";
		final JMXMPSpec[] specs = JMXMPSpec.parse(spec);
		final String cmd = String.format("--specs %s", spec);
		test(cmd, null, null, null, specs, null);		
	}
	
	/**
	 * Creates an AgentCommandParser from the passed command and validates that the content within matches the passed expected values
	 * @param cmd The agent command
	 * @param zookeep The expected zookeeper connect string
	 * @param app The expected app name
	 * @param host The expected host name
	 * @param specs The expected JMXMPSpecs
	 * @param endpoints The expected endpoints
	 */
	public static void test(final String cmd, final String zookeep, final String app, final String host, final JMXMPSpec[] specs, final Endpoint[] endpoints) {
		final AgentCommandParser parser = new AgentCommandParser(cmd).parse();
		if(zookeep==null) {
			Assert.assertNull("ZKString not null", parser.getZKConnectString());
		} else {
			Assert.assertEquals("ZKString mismatch", zookeep, parser.getZKConnectString());
		}
		if(app==null) {
			Assert.assertNull("App not null", parser.getApp());
		} else {
			Assert.assertEquals("App mismatch", app, parser.getApp());
		}
		if(host==null) {
			Assert.assertNull("Host not null", parser.getHost());
		} else {
			Assert.assertEquals("Host mismatch", host, parser.getHost());
		}
		if(specs==null) {
			Assert.assertEquals("JMXMP Specs Mismatch", 0, parser.getJMXMPSpecs().length);
		} else { 
			Assert.assertArrayEquals("JMXMP Specs Mismatch", specs, parser.getJMXMPSpecs());
		}
		if(endpoints==null) {
			Assert.assertEquals("Endpoints Mismatch", 0, parser.getEndpoints().length);
		} else {
			Assert.assertArrayEquals("Endpoints Mismatch", endpoints, parser.getEndpoints());
		}
	}
	
}
