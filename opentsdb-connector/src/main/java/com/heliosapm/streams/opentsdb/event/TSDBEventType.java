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
package com.heliosapm.streams.opentsdb.event;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.regex.Pattern;


/**
 * <p>Title: TSDBEventType</p>
 * <p>Description: Enumerates the event types that can be passed to a plugin from an OpenTSDB instance.</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.tsdbex.plugins.event.TSDBEventType</code></p>
 */
public enum TSDBEventType {
//	/** Plugin Initialization */
//	PLUGIN_INIT,
//	/** Plugin Shutdown */
//	PLUGIN_STOP,	
	/** A long value data point */
	DPOINT_LONG("publish-long", 10, PluginType.PUBLISH),
	/** A double value sdata point */
	DPOINT_DOUBLE("publish-double", 9, PluginType.PUBLISH),
	/** A published annotation */
	ANNOTATION_PUBLISH("publish-annotation", 8, PluginType.PUBLISH),	
	/** An annotation index */
	ANNOTATION_INDEX("index-annotation", 7, PluginType.SEARCH),
	/** An annotation deletion */
	ANNOTATION_DELETE("delete-annotation", 6, PluginType.SEARCH),
	/** A TSMeta index */
	TSMETA_INDEX("index-tsmeta", 4, PluginType.SEARCH),
	/** A TSMeta deletion */
	TSMETA_DELETE("delete-tsmeta", 1, PluginType.SEARCH),
	/** A UIDMeta index */
	UIDMETA_INDEX("index-uidmeta", 3, PluginType.SEARCH),
	/** A UIDMeta deletion */
	UIDMETA_DELETE("delete-uidmeta", 2, PluginType.SEARCH),
	/** A search query event */
	SEARCH("search", 5, PluginType.SEARCH),
	/** A search query response event */
	SEARCH_RESPONSE("search-response", 11, PluginType.SEARCH),
	/** A ping event to determine if there is a listener active on the dispatcher */
	PING("ping", 0, PluginType.PING);
	
	private TSDBEventType(String shortName, final int order, PluginType...pluginTypes) {
		this.pluginTypes = pluginTypes;
		this.order = order;
		this.shortName = shortName; 
	}
	
	/** An all zeroes bit mask template */
	public static final String BITS = "0000000000000000000000000000000000000000000000000000000000000000";
	/** Comma splitter pattern */
	public static final Pattern COMMA_SPLITTER = Pattern.compile(",");
		

	
	/** The plugin types this event is targetted at */
	public final PluginType[] pluginTypes;
	/** The short name of the event */
	public final String shortName;
	/** The bitmask for this event type */
	public final int mask = Integer.parseInt("1" + BITS.substring(0, ordinal()), 2);
	/** The default ordering */
	public final int order;
	
	/**
	 * Prints the event masks
	 * @param args None
	 */
	public static void main(String[] args) {
		for(TSDBEventType t: TSDBEventType.values()) {
			System.out.println(t.name() + "[mask:" + t.mask + ", shortName:" + t.shortName + ", pluginTypes:" + Arrays.toString(t.pluginTypes) + "]" );
			for(TSDBEventType te: TSDBEventType.values()) {
				System.out.println("\t" + t.name() + " enabled for " + te.name() + ": " + t.isEnabled(te.mask));				
			}
			System.out.println("\t" + t.name() + " enabled for ALL: " + t.isEnabled(getMask(TSDBEventType.values())));
		}
	}
	
	private static final TSDBEventType[] SEARCH_EVENTS = {ANNOTATION_INDEX, ANNOTATION_DELETE, SEARCH, TSMETA_DELETE, TSMETA_INDEX, UIDMETA_DELETE, UIDMETA_INDEX};
	private static final TSDBEventType[] PUBLISH_EVENTS = {ANNOTATION_PUBLISH, DPOINT_DOUBLE, DPOINT_LONG};
	
	private static final TSDBEventType[] ALL_EVENTS_P = {ANNOTATION_DELETE, SEARCH, TSMETA_DELETE, TSMETA_INDEX, UIDMETA_DELETE, UIDMETA_INDEX, ANNOTATION_PUBLISH, DPOINT_DOUBLE, DPOINT_LONG};
	private static final TSDBEventType[] ALL_EVENTS_S = {ANNOTATION_INDEX, ANNOTATION_DELETE, SEARCH, TSMETA_DELETE, TSMETA_INDEX, UIDMETA_DELETE, UIDMETA_INDEX, DPOINT_DOUBLE, DPOINT_LONG};
	
	/** Empty TSDBEventType array const */
	public static final TSDBEventType[] NONE = {};
	
	/**
	 * Decodes the passed text to an array of TSDBEventTypes
	 * @param text The text to parse
	 * @param ignoreErrors true to ignore any undecodeable segments, false otherwise
	 * @return an array of TSDBEventTypes
	 */
	public static TSDBEventType[] decode(final String text, final boolean ignoreErrors) {
		if(text==null || text.trim().isEmpty()) return NONE;
		final String[] frags = COMMA_SPLITTER.split(text.trim().toUpperCase());
		EnumSet<TSDBEventType> decoded = EnumSet.noneOf(TSDBEventType.class);
		for(String s: frags) {
			if(s==null || s.trim().isEmpty()) continue;
			try {
				final String v = s.trim().toUpperCase();
				if("S".equals(v)) {
					Collections.addAll(decoded, SEARCH_EVENTS);
					continue;
				}
				if("P".equals(v)) {
					Collections.addAll(decoded, PUBLISH_EVENTS);
					continue;
				}
				
				TSDBEventType et = valueOf(v);
				decoded.add(et);
			} catch (Exception ex) {
				if(!ignoreErrors) throw new IllegalArgumentException("Invalid TSDBEventType defined in text [" + text.trim() + "]");
			}
		}
		return decoded.toArray(new TSDBEventType[decoded.size()]);
	}
	
	/**
	 * Decodes the passed text to an array of TSDBEventTypes, throwing an exception if any segment fails to decode
	 * @param text The text to parse
	 * @return an array of TSDBEventTypes
	 */
	public static TSDBEventType[] decode(final String text) {
		return decode(text, false);
	}
	
	/**
	 * Returns all the event types for the search plugin events
	 * @return and array of event types
	 */
	public static TSDBEventType[] getSearchEvents() {
		return SEARCH_EVENTS;
	}
	
	/**
	 * Returns all the event types for the rt publisher plugin events
	 * @return and array of event types
	 */
	public static TSDBEventType[] getPublisherEvents() {
		return PUBLISH_EVENTS;
	}
	
	/**
	 * Returns all the event types favoring the RT Publisher {@link #ANNOTATION_PUBLISH} event 
	 * over {@link #ANNOTATION_INDEX} for annotations, since having both would be redundant in this case. 
	 * @return and array of event types
	 */
	public static TSDBEventType[] getAllPEvents() {
		return ALL_EVENTS_P;
	}
	
	/**
	 * Returns all the event types favoring the Search {@link #ANNOTATION_INDEX} event 
	 * over {@link #ANNOTATION_PUBLISH} for annotations, since having both would be redundant in this case. 
	 * @return and array of event types
	 */
	public static TSDBEventType[] getAllSEvents() {
		return ALL_EVENTS_S;
	}
	
	/**
	 * Indicates if this event type is targetted at search plugins
	 * @return true if this event type is targetted at search plugins, false otherwise
	 */
	public boolean isForSearch() {
		return Arrays.binarySearch(pluginTypes, PluginType.SEARCH) >= 0;
	}
	
	/**
	 * Indicates if this event type is targetted at dispatcher plugins
	 * @return true if this event type is targetted at dispatcher plugins, false otherwise
	 */
	public boolean isForPublisher() {
		return Arrays.binarySearch(pluginTypes, PluginType.PUBLISH) >= 0;
		
	}
	
	/**
	 * Indicates if this event type is targetted at RPC plugins
	 * @return true if this event type is targetted at RPC plugins, false otherwise
	 */
	public boolean isForRPC() {
		return Arrays.binarySearch(pluginTypes, PluginType.RPC) >= 0;		
	}
	
	
	/**
	 * Generates a selective bitmask for the passed types
	 * @param types The types to create a bitmask for
	 * @return the selective mask
	 */
	public static final int getMask(final TSDBEventType...types) {
		if(types==null || types.length==0) return 0;
		int _mask = 0;
		for(TSDBEventType t: types) {
			if(t==null) continue;
			_mask = _mask | t.mask;
		}
		return _mask;
	}
	
	/**
	 * Indicates if the passed mask is enabled for this event type
	 * @param mask The mask to test
	 * @return true if enabled, false otherwise
	 */
	public final boolean isEnabled(final int mask) {
		return mask == (mask | this.mask);
	}
	
	private static final TSDBEventType[] values = values();
	private static final int MAX_IND = values.length-1;
	
	/**
	 * Decodes the passed ordinal to the corresponding event
	 * @param ord The byte ordinal
	 * @return The corresponding event
	 */
	public static TSDBEventType ordinal(byte ord) {
		if(ord < 0 || ord > MAX_IND) throw new IllegalArgumentException("Invalid TSDBEventType Ordinal [" + ord + "]");
		return values[ord];
	}
	
	/**
	 * Decodes the passed ordinal to the corresponding event
	 * @param ord The byte ordinal
	 * @return The corresponding event
	 */
	public static TSDBEventType ordinal(Number ord) {
		if(ord==null) throw new IllegalArgumentException("The passed TSDBEventType Ordinal was null");
		return ordinal(ord.byteValue());
	}
	
	/**
	 * Decodes the passed event name the corresponding event
	 * @param value The string value to decode which is trimmed and upper-cased
	 * @return The corresponding event
	 */
	public static TSDBEventType ordinal(CharSequence value) {
		if(value==null || value.toString().trim().isEmpty()) throw new IllegalArgumentException("The passed TSDBEventType name null or empty");
		try {
			return valueOf(value.toString().trim().toUpperCase());
		} catch (Exception ex) {
			throw new IllegalArgumentException("The passed TSDBEventType name [" + value + "] was invalid");
		}
	}

	
}
