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
package com.heliosapm.streams.chronicle;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import javax.xml.bind.DatatypeConverter;

import com.codahale.metrics.Timer;
import com.heliosapm.streams.tracing.TagKeySorter;
import com.heliosapm.utils.lang.StringHelper;
import com.lmax.disruptor.EventFactory;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;


/**
 * <p>Title: TSDBMetricMeta</p>
 * <p>Description: Represents a unique metric definition, not an instance</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.chronicle.TSDBMetricMeta</code></p>
 */

public class TSDBMetricMeta implements BytesMarshallable, Marshallable {
	/** The factory to create new instances of TSDBMetricMeta */
	public static final EventFactory<TSDBMetricMeta> FACTORY = new TSDBMetricMetaEventFactory();
	
	/** The metric name */
	protected String metricName = null;
	/** The metric uid */
	protected String metricUid = null;
	
	/** The metric tsuid */
	protected byte[] tsuid = null;	
	/** The metric tags */
	protected final TreeMap<String, String> tags = new TreeMap<String, String>(TagKeySorter.INSTANCE);
	/** The tag key UIDs */
	protected final TreeMap<String, String>  tagKeyUids = new TreeMap<String, String>(TagKeySorter.INSTANCE);
	/** The tag value UIDs */
	protected final TreeMap<String, String>  tagValueUids = new TreeMap<String, String>(TagKeySorter.INSTANCE);
	
	/** The end to end timer start time in ms. */
	protected long endToEndStartTime = -1L;
	
	

	
	
	public static void log(final Object msg) {
		System.out.println(msg);
	}
	
	
	
	/**
	 * Creates a new TSDBMetricMeta
	 */
	private TSDBMetricMeta() {

	}
	
	/**
	 * Creates a new TSDBMetricMeta
	 */
	private TSDBMetricMeta(final TSDBMetricMeta meta) {
		this.metricName = meta.metricName;
		this.tsuid = meta.tsuid;
		this.tags.putAll(meta.tags);
		this.metricUid = meta.metricUid;
		this.getTagKeyUids().putAll(meta.tagKeyUids);
		this.getTagValueUids().putAll(meta.tagValueUids);
		this.endToEndStartTime = meta.endToEndStartTime;
	}
	
	/**
	 * <p>Creates a deep, full and independent clone of this meta.</p>
	 * {@inheritDoc}
	 * @see java.lang.Object#clone()
	 */
	public TSDBMetricMeta clone() {
		return new TSDBMetricMeta(this);
	}
	
	
	private static class TSDBMetricMetaEventFactory implements EventFactory<TSDBMetricMeta> {
		@Override
		public TSDBMetricMeta newInstance() {			
			return new TSDBMetricMeta();
		}
	}
	
	/**
	 * Resets this instance, preparing it for the next load
	 * @return this instance
	 */
	public TSDBMetricMeta reset() {
		metricName = null;
		tsuid = null;
		tags.clear();
		tagKeyUids.clear();
		tagValueUids.clear();
		endToEndStartTime = -1L;
		return this;
	}
	
	/**
	 * Records the end to end start time
	 * @return this instance
	 */
	public TSDBMetricMeta startTimer() {
		endToEndStartTime = System.currentTimeMillis();
		return this;
	}
	
	/**
	 * Records the end to end elapsed into the passed timer
	 * @param timer The timer to record the elapsed time with
	 * @return this instance
	 */
	public TSDBMetricMeta recordTimer(final Timer timer) {
		if(timer!=null && endToEndStartTime!=-1L) {
			timer.update(System.currentTimeMillis() - endToEndStartTime, TimeUnit.MILLISECONDS);
		}
		return this;		
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		final StringBuilder b = new StringBuilder("TSDBMetricMeta: [");
		b.append("\n\tTSUID:").append(tsuid==null ? "<null>" : DatatypeConverter.printHexBinary(tsuid));
		b.append("\n\tMetric Name:").append(metricName).append("/").append(metricUid==null ? "<null>" : metricUid);
		b.append("\n\tTags: [");
		for(Map.Entry<String, String> tag: tags.entrySet()) {
			final String key = tag.getKey();
			final String keyUid = tagKeyUids.get(key);
			final String value = tag.getValue();
			final String valueUid = tagValueUids.get(value);
			b.append("\n\tKey:").append(tag.getKey()).append("/").append(keyUid==null ? "<null>" : keyUid).append(", Value:")
				.append(value).append("/").append(valueUid==null ? "<null>" : valueUid);
		}
		b.append("\n\t]");
		return b.append("\n]").toString();
	}
	
	/**
	 * Loads this meta with a callback from the rt-publisher
	 * @param metricName The metric name
	 * @param tags The metric tags
	 * @param tsuid The metric tsuid
	 * @return this instance
	 */
	public TSDBMetricMeta load(final String metricName, final Map<String, String> tags, final byte[] tsuid) {
		this.metricName = metricName;
		this.tsuid = tsuid;
		this.tags.putAll(tags);
		return this;
	}
	
	/**
	 * Loads this from another TSDBMetricMeta
	 * @param otherMeta the TSDBMetricMeta to load from
	 * @return this instance
	 */
	public TSDBMetricMeta load(final TSDBMetricMeta otherMeta) {
		this.metricName = otherMeta.metricName;
		this.tsuid = otherMeta.tsuid;
		this.tags.putAll(otherMeta.tags);
		this.endToEndStartTime = otherMeta.endToEndStartTime;
		return this;
	}
	
	
	/**
	 * Resolves the UIDs for this metric
	 * @param metricUid The metric name UID
	 * @param tagKeyUids The tag key UIDs keyed by the tag key value
	 * @param tagValueUids The tag value UIDs keyed by the tag value value
	 * @return this instance
	 */
	public TSDBMetricMeta resolved(final String metricUid, final Map<String, String> tagKeyUids, final Map<String, String> tagValueUids) {
		this.metricUid = metricUid;
		this.getTagKeyUids().putAll(tagKeyUids);
		this.getTagValueUids().putAll(tagValueUids);
		return this;
	}
	
	/**
	 * Resolves the UIDs for this metric from another metric
	 * @param otherMeta The metric to resolve from
	 * @return this instance
	 */
	public TSDBMetricMeta resolved(final TSDBMetricMeta otherMeta) {
		this.metricUid = otherMeta.metricUid;
		this.getTagKeyUids().putAll(otherMeta.tagKeyUids);
		this.getTagValueUids().putAll(otherMeta.tagValueUids);
		return this;
		
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.openhft.chronicle.bytes.BytesMarshallable#writeMarshallable(net.openhft.chronicle.bytes.BytesOut)
	 */
	@Override
	public void writeMarshallable(final BytesOut bytes) {		
		final int t = tags.size();
		final int k = tagKeyUids.size();
		final int v = tagKeyUids.size();
		if(t != k || t != v) throw new IllegalStateException(new StringBuilder("Mismatch in tag, tagKeyUid and tagValueUid map sizes, t:")
				.append(t).append(", k:").append(k).append(", v:").append(v).toString());
		bytes.writeByte(MessageType.METRICMETA.byteOrdinal);
		bytes.writeUtf8(metricName);
		bytes.writeByte((byte)tags.size());
		for(Map.Entry<String, String> tag: tags.entrySet()) {
			bytes.writeUtf8(tag.getKey());
			bytes.writeUtf8(tag.getValue());
		}
		bytes.writeShort((short)tsuid.length);
		bytes.write(tsuid);

		bytes.writeUtf8(metricUid);
		
		// tag key uids: HashMap<String, byte[]> tagKeyUids
		for(Map.Entry<String, String> uid: tagKeyUids.entrySet()) {
			bytes.writeUtf8(uid.getKey());
			bytes.writeUtf8(uid.getValue());
		}
		// tag value uids: HashMap<String, byte[]> tagValueUids 
		for(Map.Entry<String, String> uid: tagValueUids.entrySet()) {
			bytes.writeUtf8(uid.getKey());
			bytes.writeUtf8(uid.getValue());
		}
		// end to end start time in ms.
		bytes.writeLong(endToEndStartTime);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.openhft.chronicle.wire.Marshallable#writeMarshallable(net.openhft.chronicle.wire.WireOut)
	 */
	@Override
	public void writeMarshallable(final WireOut wire) {
		wire
		.write("mn").text(metricName)
		.write("tags").marshallable(tags, String.class, String.class, true)
		.write("tsuid").text(StringHelper.bytesToHex(tsuid))
		.write("muid").text(metricUid)
		.write("tkuid").marshallable(tagKeyUids, String.class, String.class, true)
		.write("tvuid").marshallable(tagValueUids, String.class, String.class, true)
		.write("e2e").int64(endToEndStartTime);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.openhft.chronicle.wire.Marshallable#readMarshallable(net.openhft.chronicle.wire.WireIn)
	 */
	@Override
	public void readMarshallable(final WireIn wire) throws IORuntimeException {
		metricName = wire.read("mn").text();
		tags.putAll(wire.read("tags").marshallableAsMap(String.class, String.class));
		tsuid = StringHelper.hexToBytes(wire.read("tsuid").text());
		metricUid = wire.read("muid").text();
		tagKeyUids.putAll(wire.read("tkuid").marshallableAsMap(String.class, String.class));
		tagValueUids.putAll(wire.read("tvuid").marshallableAsMap(String.class, String.class));
		endToEndStartTime = wire.read("e2e").int64();
	}
	
	
	
	
	
	/**
	 * {@inheritDoc}
	 * @see net.openhft.chronicle.bytes.BytesMarshallable#readMarshallable(net.openhft.chronicle.bytes.BytesIn)
	 */
	@Override
	public void readMarshallable(final BytesIn bytes) throws IORuntimeException {
		final byte mt = bytes.readByte();
		if(mt!=MessageType.METRICMETA.byteOrdinal) throw new IllegalStateException("Header byte was not for MessageType.METRICMETA.byteOrdinal:" + mt);
		metricName = bytes.readUtf8();
		final int tagCount = bytes.readByte();
		for(int i = 0; i < tagCount; i++) {
			tags.put(bytes.readUtf8(), bytes.readUtf8());
		}
		tsuid = readShortSizedBytes(bytes);
		metricUid = bytes.readUtf8();
		// tag key uids: HashMap<String, byte[]> tagKeyUids		
		for(int i = 0; i < tagCount; i++) {
			tagKeyUids.put(bytes.readUtf8(), bytes.readUtf8());
		}
		// tag value uids: HashMap<String, byte[]> tagValueUids
		for(int i = 0; i < tagCount; i++) {
			tagValueUids.put(bytes.readUtf8(), bytes.readUtf8());
		}		
		endToEndStartTime = bytes.readLong();
	}
	
	
	private static byte[] readByteSizedBytes(final BytesIn<?> bytes) {
		final byte[] b = new byte[bytes.readByte()];
		bytes.read(b);
		return b;
	}
	
	private static byte[] readShortSizedBytes(final BytesIn<?> bytes) {
		final byte[] b = new byte[bytes.readShort()];
		bytes.read(b);
		return b;
	}

	/**
	 * Returns the loaded metric name
	 * @return the loaded metric name
	 */
	public String getMetricName() {
		return metricName;
	}

	/**
	 * Returns the loaded TSUID
	 * @return the loaded TSUID
	 */
	public byte[] getTsuid() {
		return tsuid;
	}

	/**
	 * Returns the loaded tags
	 * @return the loaded tags
	 */
	public TreeMap<String, String> getTags() {
		return tags;
	}

	/**
	 * Returns the resolved tag key UID map
	 * @return the resolved tag key UID map
	 */
	public Map<String, String> getTagKeyUids() {
		return tagKeyUids;
	}

	/**
	 * Returns the resolved tag value UID map
	 * @return the resolved tag value UID map
	 */
	public Map<String, String> getTagValueUids() {
		return tagValueUids;
	}
	
	/**
	 * Returns the resolved metric uid
	 * @return the resolved metric uid
	 */
	public String getMetricUid() {
		return metricUid;
	}
	
	

	

}
