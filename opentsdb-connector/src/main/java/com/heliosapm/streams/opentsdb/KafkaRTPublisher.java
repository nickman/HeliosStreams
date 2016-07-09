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

import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.heliosapm.streams.opentsdb.ringbuffer.RingBufferService;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.Annotation;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.tsd.RTPublisher;

/**
 * <p>Title: KafkaRTPublisher</p>
 * <p>Description: RTPublisher to publish events to a kafka topic</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.opentsdb.KafkaRTPublisher</code></p>
 */

public class KafkaRTPublisher extends RTPublisher {
	/** Instance logger */
	protected final Logger log = LoggerFactory.getLogger(getClass());	
	/** The publication dispatch */
	protected RingBufferService ringBufferService = null;

	/**
	 * Creates a new KafkaRTPublisher
	 */
	public KafkaRTPublisher() {
		log.info("Created KafkaRTPublisher");
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#initialize(net.opentsdb.core.TSDB)
	 */
	@Override
	public void initialize(final TSDB tsdb) {
		log.info(">>>>> Initializing KafkaRTPublisher...");
		final Map<String, String> config = tsdb.getConfig().getMap();
		final Properties p = new Properties();
		p.putAll(config);
		ringBufferService = RingBufferService.getInstance(p);
		log.info("<<<<< KafkaRTPublisher initialized.");
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#shutdown()
	 */
	@Override
	public Deferred<Object> shutdown() {
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#version()
	 */
	@Override
	public String version() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#collectStats(net.opentsdb.stats.StatsCollector)
	 */
	@Override
	public void collectStats(StatsCollector collector) {
		// TODO Auto-generated method stub

	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#publishDataPoint(java.lang.String, long, long, java.util.Map, byte[])
	 */
	@Override
	public Deferred<Object> publishDataPoint(String metric, long timestamp, long value, Map<String, String> tags,
			byte[] tsuid) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#publishDataPoint(java.lang.String, long, double, java.util.Map, byte[])
	 */
	@Override
	public Deferred<Object> publishDataPoint(String metric, long timestamp, double value, Map<String, String> tags,
			byte[] tsuid) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RTPublisher#publishAnnotation(net.opentsdb.meta.Annotation)
	 */
	@Override
	public Deferred<Object> publishAnnotation(Annotation annotation) {
		// TODO Auto-generated method stub
		return null;
	}

}
