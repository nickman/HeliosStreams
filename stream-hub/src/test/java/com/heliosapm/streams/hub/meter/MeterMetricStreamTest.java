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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.heliosapm.streams.hub.base.BaseHubStreamTest;
import com.heliosapm.streams.kafka.KafkaAdminClient;
import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.StreamedMetricValue;



/**
 * <p>Title: MeterMetricStreamTest</p>
 * <p>Description: Test for metering streamed metrics</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.hub.meter.MeterMetricStreamTest</code></p>
 */

public class MeterMetricStreamTest extends BaseHubStreamTest {
	
	/** The name of the meter topic */
	public static final String METER_TOPIC = "tsdb.metrics.meter";
	/** The name of the default consumer group */
	public static final String METER_CONSUMER_GROUP = "meterTestGroup";
	
	
	
	
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
		try {
			Producer<String, String> producer = newProducer("meter", String.class, String.class);		
			List<PartitionInfo> producerPartitionInfo = producer.partitionsFor(METER_TOPIC);
			Assert.assertFalse("No producer partitions assigned", producerPartitionInfo.isEmpty());
			PartitionInfo pi = producerPartitionInfo.get(0);
			Assert.assertEquals("Producer not expected topic", METER_TOPIC, pi.topic());
			Assert.assertNotNull("Producer leader was null", pi.leader());
			
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
			producer.send(new ProducerRecord<String, String>(METER_TOPIC, smv1.getMetricName(), smv1.toString()), callback);
			producer.flush();
			if(!latch.await(1, TimeUnit.SECONDS)) {
				throw new RuntimeException("Timeout out waiting on producer callback");
			}
			Assert.assertNull("Callback exception was not null", t[0]);
			
			KafkaAdminClient adminClient = newAdminClient(zooKeepPort);
			Consumer<String, String> consumer = newConsumer("meter", String.class, String.class);
			final CountDownLatch consumerLatch = new CountDownLatch(3); 
			consumer.subscribe(Arrays.asList(METER_TOPIC), new ConsumerRebalanceListener(){
				@Override
				public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
					consumerLatch.countDown();				
				}
	
				@Override
				public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
					/* No Op */
				}
			});
			boolean acquired = false;
			for(int i = 0; i < 3; i++) {
				if(!consumerLatch.await(5, TimeUnit.SECONDS)) {
					consumerLatch.countDown();
				} else {
					acquired = true;
					break;
				}
			}
			Assert.assertTrue("No consumer partitions assigned", acquired);
			List<PartitionInfo> consumerPartitionInfo = consumer.partitionsFor(METER_TOPIC);
			Set<TopicPartition> consumerTopicPartitionInfo = consumer.assignment();
	//		if(consumerTopicPartitionInfo.isEmpty()) {
	//			consumerTopicPartitionInfo = new HashSet<TopicPartition>();
	//			consumerTopicPartitionInfo.add(new TopicPartition(METER_TOPIC, 0));
	//			consumer.assign(consumerTopicPartitionInfo);
	//		}
	//		consumerTopicPartitionInfo = consumer.assignment();
			Assert.assertFalse("No consumer partitions assigned", consumerPartitionInfo.isEmpty());
			pi = consumerPartitionInfo.get(0);
			Assert.assertEquals("Consumer not expected topic", METER_TOPIC, pi.topic());
			Assert.assertNotNull("Consumer Leader was null", pi.leader());
			//ConsumerSummary cons = adminClient.getConsumers(METER_CONSUMER_GROUP).iterator().next();
			
			
			Assert.assertTrue("Consumer group not active", adminClient.consumerGroupActive(METER_CONSUMER_GROUP));
			
	
	//		startAppContext("meter");
			Assert.assertTrue("Consumer group not active", adminClient.consumerGroupActive(METER_CONSUMER_GROUP));
			ConsumerRecord<String, String> record = null;
			for(int i = 0; i < 3; i++) {
				final ConsumerRecords<String, String> records = consumer.poll(5000);
				if(records.count() > 0) {
					record = records.iterator().next();
					break;
				}
			}
			Assert.assertNotNull("No record available", record);
			
			Assert.assertEquals("Record Key Mismatch", smv1.getMetricName(), record.key());
			Assert.assertEquals("Record Value Mismatch", smv1.toString(), record.value());
			final StreamedMetricValue smv2 = StreamedMetric.fromString(record.value()).forValue(); 
			assertEquals(smv1, smv2);
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			throw ex;
		}
		
	}
	
	
	
}
