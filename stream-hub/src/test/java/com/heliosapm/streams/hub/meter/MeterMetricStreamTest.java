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
package com.heliosapm.streams.hub.meter;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.heliosapm.streams.hub.base.BaseHubStreamTest;
import com.heliosapm.streams.metrics.StreamedMetricValue;

/**
 * <p>Title: MeterMetricStreamTest</p>
 * <p>Description: Test for metering streamed metrics</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.hub.meter.MeterMetricStreamTest</code></p>
 */

public class MeterMetricStreamTest extends BaseHubStreamTest {
	
	/**
	 * Starts a kafka test server with the meter config
	 */
	@BeforeClass
	public static void startKafkaServer() {
		startKafkaServer("meter");
	}
	
	@Before
	public void stopAppCtx() {
		stopAppContext();
	}
	
	/**
	 * Tests sending a receiving a test text metric to the meter topic
	 * @throws Exception on any error 
	 */
	@Test
	public void testSendMeterMetric() throws Exception {
		Producer<String, String> producer = newProducer("meter", String.class, String.class);
		producer.partitionsFor("tsdb.metrics.meter");
		Consumer<String, String> consumer = newConsumer("meter", String.class, String.class);
		final StreamedMetricValue smv1 = randomDoubleStreamedMetric(null);
		final CountDownLatch latch = new CountDownLatch(1);
		final Throwable[] t = new Throwable[1];
		final Callback callback = new Callback() {
			@Override
			public void onCompletion(final RecordMetadata metadata, final Exception exception) {
				t[0] = exception;
				latch.countDown();				
			}
		};
		producer.send(new ProducerRecord<String, String>("tsdb.metrics.meter", smv1.getMetricName(), smv1.toString()), callback);
		if(!latch.await(1, TimeUnit.SECONDS)) {
			throw new RuntimeException("Timeout out waiting on producer callback");
		}
		Assert.assertNull("Callback exception was not null", t[0]);
		startAppContext("meter");
		
//		consumer.subscribe(Arrays.asList("tsdb.metric.meter"));
//		
//		final ConsumerRecords<String, String> records = consumer.poll(5000);
//		final ConsumerRecord<String, String> record  = records.iterator().next();
//		
//		Assert.assertEquals("Record Key Mismatch", smv1.getMetricName(), record.key());
//		Assert.assertEquals("Record Value Mismatch", smv1.toString(), record.value());
//		final StreamedMetricValue smv2 = StreamedMetric.fromString(record.value()).forValue(); 
//		assertEquals(smv1, smv2);
		
		
	}
	
	
	
}
