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

import com.heliosapm.streams.agent.cl.CommandLineParser;
import com.heliosapm.streams.agent.cl.JMXMPSpec;
import com.heliosapm.streams.agent.endpoint.Endpoint;
import com.heliosapm.streams.agent.publisher.AgentCommand;

/**
 * <p>Title: CommandLineParserTest</p>
 * <p>Description: Tests the command line parser</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>test.com.heliosapm.streams.agent.CommandLineParserTest</code></p>
 */

public class CommandLineParserTest extends BaseTest {


	/**
	 * Tests a full install command with one spec and one endpoint
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
		final AgentCommand agentCommand = AgentCommand.INSTALL;
		final long pid = 2034L;
		final String[] args = {agentCommand.name(), "--pid", "" + pid, "--zk", zookeep, "--app", app, "--host", host, "--specs", spec, "--ep", ep};
		test(agentCommand, pid, zookeep, app, host, specs, endpoints, args);
	}
	
	
	/**
	 * Creates an AgentCommandParser from the passed command and validates that the content within matches the passed expected values
	 * @param agentCommand The expected agent command decode
	 * @param pid The decoded pid
	 * @param zookeep The expected zookeeper connect string
	 * @param app The expected app name
	 * @param host The expected host name
	 * @param specs The expected JMXMPSpecs
	 * @param endpoints The expected endpoints
	 * @param cmd The agent command
	 */
	public static void test(final AgentCommand agentCommand, final Long pid, final String zookeep, final String app, final String host, final JMXMPSpec[] specs, final Endpoint[] endpoints, final String...cmd) {
		final CommandLineParser parser = new CommandLineParser(cmd).parse();
		Assert.assertSame("Agent Command Miismatch", agentCommand, parser.getCommand());
		if(pid==null) {
			Assert.assertNull("PID not null", parser.getPid());
		} else {
			Assert.assertEquals("PID mismatch", pid, parser.getPid());
		}		
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
