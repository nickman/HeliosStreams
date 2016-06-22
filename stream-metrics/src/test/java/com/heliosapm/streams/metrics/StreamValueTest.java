/**
 * Helios, OpenSource Monitoring
 * Brought to you by the Helios Development Group
 *
 * Copyright 2016, Helios Development Group and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org. 
 *
 */
package com.heliosapm.streams.metrics;

import org.junit.Test;

import com.heliosapm.utils.jmx.JMXHelper;

/**
 * <p>Title: StreamValueTest</p>
 * <p>Description: Unit tests for creating and de/serializing {@link StreamedMetric} instances</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.StreamValueTest</code></p>
 */

public class StreamValueTest extends BaseTest {
	
	static {
		JMXHelper.fireUpJMXMPServer(1928);
	}

	/**
	 * Tests creating a StreamedMetric, serializing it,then deserializing it
	 */
	@Test
	public void testStreamedMetricSerDe() {
		final StreamedMetric sm = new StreamedMetric("sys.cpu.total", StreamedMetric.tagsFromArray("foo=bar", "sna=foo"));
		log("SM1: [%s]:  KEY: [%s]", sm, sm.metricKey());
		final byte[] ser = sm.toByteArray();
		log("SM1 serialized to [%s] bytes, Estimated: [%s]", ser.length, sm.byteSize);
		final StreamedMetric sm2 = StreamedMetric.read(ser);
		log("SM2: [%s]   KEY: [%s]", sm2, sm2.metricKey());
		assertEquals(sm, sm2);
	}
	
	/**
	 * Tests creating a StreamedMetric with a value type, serializing it,then deserializing it
	 */
	@Test
	public void testStreamedMetricWithVTSerDe() {
		final StreamedMetric sm = new StreamedMetric("sys.cpu.total", StreamedMetric.tagsFromArray("foo=bar", "sna=foo")).setValueType(ValueType.A);
		log("SM1: [%s]:  KEY: [%s]", sm, sm.metricKey());
		final byte[] ser = sm.toByteArray();
		log("SM1 serialized to [%s] bytes, Estimated: [%s]", ser.length, sm.byteSize);
		final StreamedMetric sm2 = StreamedMetric.read(ser);
		log("SM2: [%s]   KEY: [%s]", sm2, sm2.metricKey());
		assertEquals(sm, sm2);
	}
	
	
	/**
	 * Tests creating a StreamedMetricValue, serializing it,then deserializing it
	 */
	@Test
	public void testStreamedMetricValueSerDe() {
		final StreamedMetricValue sm = new StreamedMetricValue(nextPosDouble(), "sys.cpu.total", StreamedMetric.tagsFromArray("foo=bar", "sna=foo"));
		log("SMV1: [%s]:  KEY: [%s]", sm, sm.metricKey());
		final byte[] ser = sm.toByteArray();
		log("SMV1 serialized to [%s] bytes, Estimated: [%s]", ser.length, sm.byteSize);
		final StreamedMetricValue sm2 = (StreamedMetricValue)StreamedMetric.read(ser);
		log("SMV2: [%s]   KEY: [%s]", sm2, sm2.metricKey());
		assertEquals(sm, sm2);
	}
	
	/**
	 * Tests creating a StreamedMetricValue with a value type, serializing it,then deserializing it
	 */
	@Test
	public void testStreamedMetricValueWithVTSerDe() {
		final StreamedMetricValue sm = new StreamedMetricValue(nextPosDouble(), "sys.cpu.total", StreamedMetric.tagsFromArray("foo=bar", "sna=foo")).setValueType(ValueType.P);
		log("SMV1: [%s]:  KEY: [%s]", sm, sm.metricKey());
		final byte[] ser = sm.toByteArray();
		log("SMV1 serialized to [%s] bytes, Estimated: [%s]", ser.length, sm.byteSize);
		final StreamedMetricValue sm2 = (StreamedMetricValue)StreamedMetric.read(ser);
		log("SMV2: [%s]   KEY: [%s]", sm2, sm2.metricKey());
		assertEquals(sm, sm2);
	}

	
	
	
	/**
	 * Tests creating a StreamedMetric from a string
	 */
	@Test
	public void testStreamedMetricFromString() {
		final long now = System.currentTimeMillis();
		final String metricName = "sys.cpu.total";
		final String host = "webserver05";
		final String app = "login-sso";
		final String[] tags = {"dc=us-west1", "colo=false", "host=" + host, "app=" + app};
		final double value = nextPosDouble();
		
		final String dvl = directedValueless("A", now,  metricName, host, app, tags);
		final String uvl = undirectedValueless(now,  metricName, host, app, tags);
		final String dv = directedValue("P", now,  value, metricName, host, app, tags);
		log(dv);
		
		final String uv = undirectedValue(now,  value, metricName, host, app, tags);
		
		assertEquals(new StreamedMetric(now, metricName, StreamedMetric.tagsFromArray(tags)).setValueType(ValueType.A), StreamedMetric.fromString(dvl));
		assertEquals(new StreamedMetric(now, metricName, StreamedMetric.tagsFromArray(tags)), StreamedMetric.fromString(uvl));

		assertEquals(new StreamedMetricValue(now, value, metricName, StreamedMetric.tagsFromArray(tags)).setValueType(ValueType.P), StreamedMetric.fromString(dv));
		assertEquals(new StreamedMetricValue(now, value, metricName, StreamedMetric.tagsFromArray(tags)), StreamedMetric.fromString(uv));
		
	}
	
	

}
