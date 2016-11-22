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

import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.heliosapm.streams.agent.endpoint.AdvertisedEndpoint;
import com.heliosapm.streams.agent.endpoint.Endpoint;
import com.heliosapm.streams.agent.services.EndpointPublisher;
import com.heliosapm.utils.concurrency.CompletionFuture;
import com.heliosapm.utils.enums.TimeUnitSymbol;
import com.heliosapm.utils.io.StdInCommandHandler;


/**
 * <p>Title: EndpointPublicationTest</p>
 * <p>Description: Tests endpoint publications against a test-time created zookeeper server</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>test.com.heliosapm.endpoint.EndpointPublicationTest</code></p>
 */

public class EndpointPublicationTest extends BaseTest {
	final ThreadLocal<Integer> currentZooKeeperPort = new ThreadLocal<Integer>(); 
	final ThreadLocal<EndpointPublisher> currentPublisher = new ThreadLocal<EndpointPublisher>();
	
	/** The bytes for a JSON Sample */
	private static final byte[] AP = "{\"endpoints\":[{\"name\":\"aaa-15s/drop\"},{\"name\":\"bbb-30m\"},{\"name\":\"ccc/wiz\"},{\"name\":\"ddd\"}],\"jmx\":\"service:jmx:jmxmp://foo:1924\",\"app\":\"TestApp\",\"host\":\"TestHost\",\"port\":1924}".getBytes(UTF8);
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		log("Zookeep Test");
		final int port = startZooKeeper();
		log("Zookeeper started on port: " + port);
		StdInCommandHandler.getInstance().registerCommand("stop", new Runnable(){
			public void run() {
				log("Stopping ZooKeep....");
				stopZooKeeper(port);
				log("ZooKeep Stopped.");
				System.exit(0);
			}
		}).run();
	}
	
	@Before
	public void zooKeepUp() {
		zooKeepUp(null);
	}

	
	public void zooKeepUp(final Integer zooPort) {
		final int port = zooPort==null ? startZooKeeper() : startZooKeeper(zooPort);
		final String connectString = "localhost:" + port;
		System.setProperty(EndpointPublisher.ZK_CONNECT_CONF, connectString);	
		log("PORT:" + port);
		currentZooKeeperPort.set(port);
		final EndpointPublisher ep = EndpointPublisher.getInstance();
		currentPublisher.set(ep.waitForConnect());
	}
	
	@After
	public void zooKeepDown() {
		EndpointPublisher ep = currentPublisher.get();
		currentPublisher.remove();
		if(ep!=null) {
			try { ep.close(); } catch (Exception x) {/* No Op */}
		}
		final Integer port = currentZooKeeperPort.get();
		currentZooKeeperPort.remove();
		if(port!=null) {
			stopZooKeeper(port);
		}
	}
	
	@Test
	public void testZooKeepUpDown() {
		zooKeepDown();
		for(int i = 0; i < 10; i++) {
			zooKeepUp();
			zooKeepDown();
		}
	}
	
	@Test
	public void testParseEndpointFromString() {
		final AdvertisedEndpoint ap = new AdvertisedEndpoint("service:jmx:jmxmp://foo:1924", "TestApp", "TestHost", "aaa-15s/drop", "bbb-30m", "ccc/wiz", "ddd");
		testEndpointParsing(ap);
	}
	
	
	//@Test(timeout=5000)
	@Test
	public void testPublishAndRetrieve() throws Exception {		
		final AdvertisedEndpoint ap = new AdvertisedEndpoint("service:jmx:jmxmp://foo:1924", "TestApp", "TestHost", "aaa-15s/drop", "bbb-30m", "ccc/wiz", "ddd"); 
		final EndpointPublisher ep = currentPublisher.get();
		final CompletionFuture cf = ep.register(ap);
		if(!cf.get(2, TimeUnit.SECONDS)) throw new Exception("Timed out waiting for registration");
		final ZooKeeper client = ep.getZooClient().getZooKeeper();
		final String path = ap.getZkPath(ep.getServiceType()); 
		final Stat stat = client.exists(path, false);
		Assert.assertNotNull("Stat for [" + path + "] was null", stat);
		final byte[] data = client.getData(path, false, stat);
		final AdvertisedEndpoint ap2 = AdvertisedEndpoint.fromBytes(data);
		Assert.assertEquals("The AdvertisedEndpoints are not equal", ap, ap2);
		testEndpointParsing(ap);
	}
	
	@Test
	public void testWaitForZooPublishAndRetrieve() throws Exception {
		final int port = currentZooKeeperPort.get();
		zooKeepDown();
		final String connectString = "localhost:" + port;
		System.setProperty(EndpointPublisher.ZK_CONNECT_CONF, connectString);		
		final AdvertisedEndpoint ap = new AdvertisedEndpoint("service:jmx:jmxmp://foo:1924", "TestApp", "foo", "aaa-15s/drop", "bbb-30m", "ccc/wiz", "ddd");
		final EndpointPublisher ep = EndpointPublisher.getInstance();
		final CompletionFuture cf = ep.register(ap);
		scheduler.schedule(new Runnable(){
			public void run() {
				zooKeepUp(port);
				log("Zookeeper Restarted.");
			}
		}, 1, TimeUnit.SECONDS);
		if(!cf.get(2, TimeUnit.SECONDS)) throw new Exception("Timed out waiting for registration");
		final ZooKeeper client = ep.getZooClient().getZooKeeper();
		final String path = ap.getZkPath(ep.getServiceType()); 
		final Stat stat = client.exists(path, false);
		Assert.assertNotNull("Stat for [" + path + "] was null", stat);
		final byte[] data = client.getData(path, false, stat);
		final AdvertisedEndpoint ap2 = AdvertisedEndpoint.fromBytes(data);
		Assert.assertEquals("The AdvertisedEndpoints are not equal", ap, ap2);
	}
	
	
	
//	{
//		  "endpoints": [
//		    {
//		      "name": "aaa-15s/drop"
//		    },
//		    {
//		      "name": "bbb-30m"
//		    },
//		    {
//		      "name": "ccc/wiz"
//		    },
//		    {
//		      "name": "ddd"
//		    }
//		  ],
//		  "jmx": "service:jmx:jmxmp://foo:1924",
//		  "app": "TestApp",
//		  "host": "TestHost",
//		  "port": 1924
//		}
	
	
	public void testEndpointParsing(final AdvertisedEndpoint ap) {
		Assert.assertEquals("JMX URL Mismatch", "service:jmx:jmxmp://foo:1924", ap.getJmxUrl());
		Assert.assertEquals("App Mismatch", "TestApp", ap.getApp());
		Assert.assertEquals("Host Mismatch", "TestHost", ap.getHost());
		Assert.assertEquals("Post Mismatch", 1924, ap.getPort());
		final Endpoint[] endpoints = ap.getEndpoints();
		Assert.assertEquals("Endpoint Count Mismatch", 4, endpoints.length);
		for(int i = 0; i < 4; i++) {
			switch(i) {
			case 0:
				Assert.assertEquals("Endpoint " + i + " Name Mismatch", "aaa", endpoints[i].getName());
				Assert.assertEquals("Endpoint " + i + " Period Mismatch", 15L, endpoints[i].getPeriod());
				Assert.assertEquals("Endpoint " + i + " Period Unit Mismatch", TimeUnitSymbol.SECONDS, endpoints[i].getUnit());
				Assert.assertEquals("Endpoint " + i + " Processor Mismatch", "drop", endpoints[i].getProcessor());
				break;
			case 1:
				Assert.assertEquals("Endpoint " + i + " Name Mismatch", "bbb", endpoints[i].getName());
				Assert.assertEquals("Endpoint " + i + " Period Mismatch", 30L, endpoints[i].getPeriod());
				Assert.assertEquals("Endpoint " + i + " Period Unit Mismatch", TimeUnitSymbol.MINUTES, endpoints[i].getUnit());
				Assert.assertNull("Endpoint " + i + " Processor Mismatch", endpoints[i].getProcessor());
				break;
			case 2:
				Assert.assertEquals("Endpoint " + i + " Name Mismatch", "ccc", endpoints[i].getName());
				Assert.assertEquals("Endpoint " + i + " Period Mismatch", -1L, endpoints[i].getPeriod());
				Assert.assertNull("Endpoint " + i + " Period Unit Mismatch", endpoints[i].getUnit());
				Assert.assertEquals("Endpoint " + i + " Processor Mismatch", "wiz", endpoints[i].getProcessor());
				break;
			case 3:
				Assert.assertEquals("Endpoint " + i + " Name Mismatch", "ddd", endpoints[i].getName());
				Assert.assertEquals("Endpoint " + i + " Period Mismatch", -1L, endpoints[i].getPeriod());
				Assert.assertNull("Endpoint " + i + " Period Unit Mismatch", endpoints[i].getUnit());
				Assert.assertNull("Endpoint " + i + " Processor Mismatch", endpoints[i].getProcessor());
				break;
			default:
				Assert.fail();
			}
		}
	}

}
