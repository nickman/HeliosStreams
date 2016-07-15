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
package com.heliosapm.streams.opentsdb;

import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.heliosapm.aop.retransformer.Retransformer;
import com.heliosapm.streams.buffers.BufferManager;
import com.heliosapm.streams.metrics.StreamedMetricValue;
import com.heliosapm.streams.opentsdb.mocks.TSDBTestTemplate;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.url.URLHelper;

import io.netty.buffer.ByteBuf;
import net.opentsdb.core.TSDB;
import net.opentsdb.tsd.RpcManager;

/**
 * <p>Title: KafkaRPCTest</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.opentsdb.KafkaRPCTest</code></p>
 */

public class KafkaRPCTest extends BaseTest {
	private static TSDB tsdb = null;
	private static Producer<String, ByteBuf> producer = null;
	class FakeRpcManager {
		@SuppressWarnings("unused")
		private void initializeBuiltinRpcs(final String mode, 
		        final ImmutableMap.Builder<String, Object> telnet,
		        final ImmutableMap.Builder<String, Object> http) {
			/* No Op */
		}
		
	}
	
	
	/**
	 * Creates the TSDB instance and plugin jar
	 */
	@BeforeClass
	public static void init() {		
		System.setProperty("tsdb.id.host", "helioleopard");
		createPluginJar(KafkaRPC.class);
		Retransformer.getInstance().transform(TSDB.class, TSDBTestTemplate.class);		
		Retransformer.getInstance().transform(RpcManager.class, FakeRpcManager.class);		
		tsdb = newTSDB("coretest");
		RpcManager.instance(tsdb);
		startProducer();
	}
	
	@AfterClass
	public static void close() {
		stopProducer();
	}
	
	static void startProducer() {
		final Properties p = URLHelper.readProperties(KafkaRPCTest.class.getClassLoader().getResource("configs/brokers/default.properties"));
		p.setProperty("value.serializer", com.heliosapm.streams.buffers.ByteBufSerde.ByteBufSerializer.class.getName());
		producer = new KafkaProducer<String, ByteBuf>(p);
	}
	
	static void stopProducer() {
		if(producer!=null) try { producer.close(); } catch (Exception x) {/* No Op */}
	}
	
	protected void send(final Set<StreamedMetricValue> metrics) {
		final ByteBuf buff = BufferManager.getInstance().buffer(metrics.size() * 128);
		
		try {
			 
			for(StreamedMetricValue smv: metrics) {
				smv.intoByteBuf(buff);
			}
			log("Sent Buff Size:" + buff.readableBytes());
			final int vsize = producer.send(new ProducerRecord<String, ByteBuf>("tsdb.metrics.binary", buff)).get().serializedValueSize();
			log("Sent Value Size:" + vsize);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		} finally {			
			try { buff.release(); } catch (Exception x) {/* No Op */}
		}
	}
	
	public static final AtomicReference<CountDownLatch> latch = new AtomicReference<CountDownLatch>(null);
	
	@Test
	public void go() {		
		final int metricCount = 2000;
		for(int x = 0; x < 500; x++) {
			final CountDownLatch waitLatch = new CountDownLatch(metricCount);
			latch.set(waitLatch);
			final Set<StreamedMetricValue> originals = new LinkedHashSet<StreamedMetricValue>(metricCount);
			for(int i = 0; i < metricCount; i++) {
				StreamedMetricValue smv = new StreamedMetricValue(System.currentTimeMillis(), nextPosDouble(), getRandomFragment(), randomTags(3));
				originals.add(smv);			
			}
			send(originals);
			
			final long now = System.currentTimeMillis();
			try {
				if(!waitLatch.await(JMXHelper.isDebugAgentLoaded() ? 5000 : 500, TimeUnit.SECONDS)) {
					Assert.fail("Timed out while waiting");
				}
			} catch (Exception ex) {
				Assert.fail("Interrupted while waiting:" + ex);
			}
	//		Assert.assertEquals("Metrics sent != Metrics received", metricCount, TSDBTestTemplate.points.longValue());
		}
		
	}
}
