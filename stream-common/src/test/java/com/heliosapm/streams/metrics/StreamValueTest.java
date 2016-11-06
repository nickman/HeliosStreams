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

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.heliosapm.utils.buffer.BufferManager;
import com.heliosapm.streams.json.JSONOps;
import com.heliosapm.utils.jmx.JMXHelper;

import io.netty.buffer.ByteBuf;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

/**
 * <p>Title: StreamValueTest</p>
 * <p>Description: Unit tests for creating and de/serializing {@link StreamedMetric} instances</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.StreamValueTest</code></p>
 */

public class StreamValueTest extends BaseTest {
	
	static {
		JMXHelper.fireUpJMXMPServer(1290);
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
		final StreamedMetric sm = new StreamedMetric("sys.cpu.total", StreamedMetric.tagsFromArray("foo=bar", "sna=foo")).setValueType(ValueType.ACCUMULATOR);
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
		final StreamedMetricValue sm = new StreamedMetricValue(nextPosDouble(), "sys.cpu.total", StreamedMetric.tagsFromArray("foo=bar", "sna=foo")).setValueType(ValueType.PERIODAGG);
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
		
		assertEquals(new StreamedMetric(now, metricName, StreamedMetric.tagsFromArray(tags)).setValueType(ValueType.ACCUMULATOR), StreamedMetric.fromString(dvl));
		assertEquals(new StreamedMetric(now, metricName, StreamedMetric.tagsFromArray(tags)), StreamedMetric.fromString(uvl));

		assertEquals(new StreamedMetricValue(now, value, metricName, StreamedMetric.tagsFromArray(tags)).setValueType(ValueType.PERIODAGG), StreamedMetric.fromString(dv));
		assertEquals(new StreamedMetricValue(now, value, metricName, StreamedMetric.tagsFromArray(tags)), StreamedMetric.fromString(uv));
		
	}
	
	@Test
	public void testFromString() {
		final String template = "123456789, 3, x.y.z, app-09, hoo-haa";
		final StreamedMetric sm = StreamedMetric.fromString(template);
		log(JSONOps.serializeToString(sm));
	}

	
	@Test
	public void testStreamedMetricMarshallable() {
		final long now = System.currentTimeMillis();
		final String metricName = "sys.cpu.total";
		final String host = "webserver05";
		final String app = "login-sso";
		final String[] tags = {"dc=us-west1", "colo=false", "host=" + host, "app=" + app};
		final double value = nextPosDouble();
		log("Starting BytesMarshallable Test");
		Bytes bytes = Bytes.elasticByteBuffer(128);
		StreamedMetric sm1 = new StreamedMetric(now, metricName, StreamedMetric.tagsFromArray(tags)).setValueType(ValueType.ACCUMULATOR);
		sm1.writeMarshallable(bytes);
		log("Bytes For Read:" + bytes.length());
		StreamedMetric sm2 = StreamedMetric.fromBytes(bytes);
		assertEquals(sm1, sm2);
		bytes.release();
		
		bytes = Bytes.elasticByteBuffer(128);
		
		StreamedMetricValue smv1 = new StreamedMetricValue(now, value, metricName, StreamedMetric.tagsFromArray(tags)).setValueType(ValueType.STRAIGHTTHROUGH);
		smv1.writeMarshallable(bytes);
		StreamedMetricValue smv2 = StreamedMetric.fromBytes(bytes).forValue(0);
		assertEquals(smv1, smv2);
		bytes.release();
		Assert.assertTrue("StreamedMetric not WriteBytesMarshallable", (sm1 instanceof WriteBytesMarshallable));
		Assert.assertTrue("StreamedMetricValue not WriteBytesMarshallable", (smv1 instanceof WriteBytesMarshallable));
		Assert.assertTrue("StreamedMetric not ReadBytesMarshallable", (sm1 instanceof ReadBytesMarshallable));
		Assert.assertTrue("StreamedMetricValue not ReadBytesMarshallable", (smv1 instanceof ReadBytesMarshallable));
		
	}
	
	@Test
	public void testSMVIterMultiNoRelease() {
		testStreamedMetricValueIterator(false, false);
	}
	
	@Test
	public void testSMVIterSingleNoRelease() {
		testStreamedMetricValueIterator(true, false);
	}
	
	@Test
	public void testSMVIterMultiRelease() {
		testStreamedMetricValueIterator(false, true);
	}

	@Test
	public void testSMVIterSingleRelease() {
		testStreamedMetricValueIterator(true, true);
	}
	
	protected void testStreamedMetricValueIterator(final boolean single, final boolean release) {
		ByteBuf inBuffer = null;
		try {
			final int metricCount = 10000;
			inBuffer = BufferManager.getInstance().buffer(metricCount * 128);
			final Set<StreamedMetricValue> originals = new LinkedHashSet<StreamedMetricValue>(metricCount);
			for(int i = 0; i < metricCount; i++) {
				StreamedMetricValue smv = new StreamedMetricValue(System.currentTimeMillis(), nextPosDouble(), getRandomFragment(), randomTags(3));
				originals.add(smv);
				smv.intoByteBuf(inBuffer);
			}
			Assert.assertEquals("Invalid number of samples", metricCount, originals.size());
			final Iterator<StreamedMetricValue> originalsIter = originals.iterator();
			final Iterator<StreamedMetricValue> iter = StreamedMetricValue.streamedMetricValues(single, inBuffer, release).iterator();
			int loops = 0;
			while(originalsIter.hasNext()) {
				final StreamedMetricValue smv1 = originalsIter.next();
				Assert.assertTrue("Buffered iterator had no next metric", iter.hasNext());
				final StreamedMetricValue smv2 = iter.next();
				assertEquals(smv1, smv2);
//				log(smv1);
				loops++;
			}
			Assert.assertFalse("Buffer iter should have no more metrics", iter.hasNext());
			Assert.assertEquals("Invalid number of loops", metricCount, loops);
			if(release) {
				Assert.assertEquals("Invalid refCount on released buffer", 0, inBuffer.refCnt());
			} else {
				Assert.assertEquals("Invalid refCount on released buffer", 1, inBuffer.refCnt());
			}
		} finally {
			if(!release) inBuffer.release();
		}
	}	
	
	@Test
	public void getMinByteSize() {
		final StreamedMetricValue smv = new StreamedMetricValue(System.currentTimeMillis(), nextPosDouble(), "a", Collections.singletonMap("b",  "c"));
		final int len = smv.toByteArray().length;
		log("Size:" + len);
		Assert.assertEquals("Minimum Size For Metric Is Broken", StreamedMetricValue.MIN_READABLE_BYTES, len);
	}

}
