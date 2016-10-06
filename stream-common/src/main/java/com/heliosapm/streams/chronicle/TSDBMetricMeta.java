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

import java.io.File;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import javax.xml.bind.DatatypeConverter;

import com.codahale.metrics.Timer;
import com.heliosapm.streams.tracing.TagKeySorter;
import com.lmax.disruptor.EventFactory;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.wire.JSONWire;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import scala.util.parsing.json.JSONObject;


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
	protected byte[] metricUid = null;
	
	/** The metric tsuid */
	protected byte[] tsuid = null;	
	/** The metric tags */
	protected final TreeMap<String, String> tags = new TreeMap<String, String>(TagKeySorter.INSTANCE);
	/** The tag key UIDs */
	protected final MarshallableUIDMap tagKeyUids = new MarshallableUIDMap(8);
	/** The tag value UIDs */
	protected final MarshallableUIDMap tagValueUids = new MarshallableUIDMap(8);
	
	/** The end to end timer start time in ms. */
	protected long endToEndStartTime = -1L;
	
	/** A random value generator */
	protected static final Random RANDOM = new Random(System.currentTimeMillis());
	public static byte[] randomBytes(int size) {
		final byte[] bytes = new byte[size];
		RANDOM.nextBytes(bytes);
		return bytes;
	}
	/**
	 * Generates an array of random strings created from splitting a randomly generated UUID.
	 * @return an array of random strings
	 */
	public static String[] getRandomFragments() {
		return UUID.randomUUID().toString().split("-");
	}
	/**
	 * Generates a random string made up from a UUID.
	 * @return a random string
	 */
	public static String getRandomFragment() {
		return UUID.randomUUID().toString();
	}
	/**
	 * Returns a random positive int within the bound
	 * @param bound the bound on the random number to be returned. Must be positive. 
	 * @return a random positive int
	 */
	public static int nextPosInt(int bound) {
		return Math.abs(RANDOM.nextInt(bound));
	}
	
	

	
	public static void main(String[] args) {
		log("TSDBMetricMeta Test");
		final boolean TEXT = false;
		File tmp = null;
		MappedBytes bytes = null;
		
		try {
			tmp = File.createTempFile("tsdbmetricmeta", ".bytes");
			bytes = MappedBytes.mappedBytes(tmp, 2048);
			final JSONWire jw = new JSONWire(bytes);
			final long startPos = bytes.writePosition();
			log("Bytes Pos:" + startPos);
			final TSDBMetricMeta m = TSDBMetricMeta.FACTORY.newInstance();
			final String mn = getRandomFragment();
			final HashMap<String, String> tags = new HashMap<String, String>(4);
			
			final HashMap<String, byte[]> tagKeys = new HashMap<String, byte[]>(4);
			final HashMap<String, byte[]> tagValues = new HashMap<String, byte[]>(4);
			for(int y = 0; y < 4; y++) {
				String[] keys = getRandomFragments();
				String[] values = getRandomFragments();
				tags.put(keys[0], values[0]);
				tagKeys.put(keys[0], randomBytes(6));
				tagValues.put(values[0], randomBytes(6));
			}
			byte[] tsuid = randomBytes(nextPosInt(60) + 4);
			m.load(mn, tags, tsuid);
			m.metricUidCallback.call(randomBytes(6));
			m.tagKeyUids.putAll(tagKeys);
			m.tagValueUids.putAll(tagValues);
			m.startTimer();
			if(TEXT) {				
				m.writeMarshallable(jw);				
			} else {
				m.writeMarshallable(bytes);
			}
			
			final long endPos = bytes.writePosition();
			if(TEXT) {
				log("JSON:\n==============================================================\n" + jw.toString());
			}
			log("M1:\n==============================================================\n" + m);
			if(TEXT) {
				m.reset().readMarshallable(jw);
			} else {
				m.reset().readMarshallable(bytes);
			}
			log("M2:\n==============================================================\n" + m);
			log("Size:" + (endPos - startPos));
//			log("Content:\n" + jw.toString());
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
		} finally {
			if(bytes!=null) try { bytes.close(); } catch (Exception x) {/* No Op */}
			if(tmp!=null) tmp.delete();
		}
	}
	
	public static void log(final Object msg) {
		System.out.println(msg);
	}
	
	
	public final Callback<Deferred<Void>, byte[]> metricUidCallback = new Callback<Deferred<Void>, byte[]>() {
		/**
		 * Callback with the metric uid
		 * {@inheritDoc}
		 * @see com.stumbleupon.async.Callback#call(java.lang.Object)
		 */
		@Override
		public Deferred<Void> call(final byte[] uid) throws Exception {
			metricUid = uid;
			return null;
		}
	};
	
	public final Callback<Void, ArrayList<byte[]>> tagUidsCallback = new Callback<Void, ArrayList<byte[]>>() {
		/**
		 * Callback with tags uids in tagk1, tagv1, .. tagkn, tagvn
		 * {@inheritDoc}
		 * @see com.stumbleupon.async.Callback#call(java.lang.Object)
		 */
		@Override
		public Void call(final ArrayList<byte[]> tagUids) throws Exception {
			
			return null;
		}
	};
	
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
		b.append("\n\tMetric Name:").append(metricName).append("/").append(metricUid==null ? "<null>" : DatatypeConverter.printHexBinary(metricUid));
		b.append("\n\tTags: [");
		for(Map.Entry<String, String> tag: tags.entrySet()) {
			final String key = tag.getKey();
			final byte[] keyUid = tagKeyUids.get(key);
			final String value = tag.getValue();
			final byte[] valueUid = tagValueUids.get(value);
			b.append("\n\tKey:").append(tag.getKey()).append("/").append(keyUid==null ? "<null>" : DatatypeConverter.printHexBinary(keyUid)).append(", Value:")
				.append(value).append("/").append(valueUid==null ? "<null>" : DatatypeConverter.printHexBinary(valueUid));
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
		return this;
	}
	
	
	/**
	 * Resolves the UIDs for this metric
	 * @param metricUid The metric name UID
	 * @param tagKeyUids The tag key UIDs keyed by the tag key value
	 * @param tagValueUids The tag value UIDs keyed by the tag value value
	 * @return this instance
	 */
	public TSDBMetricMeta resolved(final byte[] metricUid, final Map<String, byte[]> tagKeyUids, final Map<String, byte[]> tagValueUids) {
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

		bytes.writeByte((byte)metricUid.length);
		bytes.write(metricUid);
		
		// tag key uids: HashMap<String, byte[]> tagKeyUids
		for(Map.Entry<String, byte[]> uid: tagKeyUids.entrySet()) {
			bytes.writeUtf8(uid.getKey());
			bytes.writeByte((byte)uid.getValue().length);
			bytes.write(uid.getValue());
		}
		// tag value uids: HashMap<String, byte[]> tagValueUids 
		for(Map.Entry<String, byte[]> uid: tagValueUids.entrySet()) {
			bytes.writeUtf8(uid.getKey());
			bytes.writeByte((byte)uid.getValue().length);
			bytes.write(uid.getValue());
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
		.write("tsuid").text(bytesToHex(tsuid))
		.write("muid").text(bytesToHex(metricUid))
		.write("tkuid")
			.object(tagKeyUids)
//			.marshallable(tagKeyUids, String.class, byte[].class, true)
		.write("tvuid")
			.object(tagValueUids)
//			.marshallable(tagValueUids, String.class, byte[].class, true)
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
		tsuid = hexToBytes(wire.read("tsuid").text());
		metricUid = hexToBytes(wire.read("muid").text());
		wire.read("tkuid").marshallable((Marshallable)tagKeyUids);
		wire.read("tvuid").marshallable((Marshallable)tagValueUids);
//		tagKeyUids.putAll((Map<String, byte[]>)wire.read("tkuid").object());
//		tagValueUids.putAll((Map<String, byte[]>)wire.read("tvuid").object());				
		endToEndStartTime = wire.read("e2e").int64();
	}
	
	/**
	 * <p>Title: MarshallableUIDMap</p>
	 * <p>Description: A marshallable map of strings to byte arrays</p> 
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>com.heliosapm.streams.chronicle.TSDBMetricMeta.MarshallableUIDMap</code></p>
	 */
	static class MarshallableUIDMap extends HashMap<String, byte[]> implements Marshallable {
		/**  */
		private static final long serialVersionUID = 3250838014971886072L;

		/**
		 * Creates a new MarshallableUIDMap
		 * @param initialCapacity The initial capacity of the map
		 */
		public MarshallableUIDMap(final int initialCapacity) {
			super(initialCapacity);
		}

		/**
		 * {@inheritDoc}
		 * @see net.openhft.chronicle.wire.Marshallable#writeMarshallable(net.openhft.chronicle.wire.WireOut)
		 */
		@Override
		public void writeMarshallable(final WireOut wire) {
			if(!isEmpty()) {
				final ConcurrentHashMap<String, String> tmp = new ConcurrentHashMap<String, String>(size());
				entrySet().parallelStream().forEach(e -> tmp.put(e.getKey(), bytesToHex(e.getValue())));
				wire.writeAllAsMap(String.class, String.class, tmp);
			}
		}
		
		/**
		 * {@inheritDoc}
		 * @see net.openhft.chronicle.wire.Marshallable#readMarshallable(net.openhft.chronicle.wire.WireIn)
		 */
		@Override
		public void readMarshallable(final WireIn wire) throws IORuntimeException {
			final ConcurrentHashMap<String, String> tmp = new ConcurrentHashMap<String, String>(8);
			wire.readAllAsMap(String.class, String.class, tmp);
			if(!tmp.isEmpty()) {				
				tmp.entrySet().parallelStream().forEach(e -> put(e.getKey(), hexToBytes(e.getValue())));
			}
		}
	}
	
	
	private static String bytesToHex(final byte[] bytes) {
		return bytes==null ? "" : DatatypeConverter.printHexBinary(bytes);
	}
	
	private static byte[] hexToBytes(final String s) {
		if (s == null || s.isEmpty()) {
			return null;
		}
		String id = s;
		if(id.length() % 2 > 0) {
			id = "0" + id;
		}	      
		return DatatypeConverter.parseHexBinary(id);		
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
		metricUid = readByteSizedBytes(bytes);
		// tag key uids: HashMap<String, byte[]> tagKeyUids		
		for(int i = 0; i < tagCount; i++) {
			tagKeyUids.put(bytes.readUtf8(), readByteSizedBytes(bytes));
		}
		// tag value uids: HashMap<String, byte[]> tagValueUids
		for(int i = 0; i < tagCount; i++) {
			tagValueUids.put(bytes.readUtf8(), readByteSizedBytes(bytes));
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
	public HashMap<String, byte[]> getTagKeyUids() {
		return tagKeyUids;
	}

	/**
	 * Returns the resolved tag value UID map
	 * @return the resolved tag value UID map
	 */
	public HashMap<String, byte[]> getTagValueUids() {
		return tagValueUids;
	}
	
	/**
	 * Returns the resolved metric uid
	 * @return the resolved metric uid
	 */
	public byte[] getMetricUid() {
		return metricUid;
	}
	
	

	

}
