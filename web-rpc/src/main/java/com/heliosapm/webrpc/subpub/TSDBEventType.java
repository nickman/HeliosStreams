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
package com.heliosapm.webrpc.subpub;



/**
 * <p>Title: TSDBEventType</p>
 * <p>Description: Enumeration of event types to subscribe to</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.webrpc.subpub.TSDBEventType</code></p>
 */

public enum TSDBEventType {
	/** A long value data point */
	DPOINT_LONG("publish-long"),
	/** A double value sdata point */
	DPOINT_DOUBLE("publish-double"),
	/** An annotation index */
	ANNOTATION_INDEX("index-annotation"),
	/** An annotation deletion */
	ANNOTATION_DELETE("delete-annotation"),
	/** A TSMeta index */
	TSMETA_INDEX("index-tsmeta"),
	/** A TSMeta deletion */
	TSMETA_DELETE("delete-tsmeta"),
	/** A UIDMeta index */
	UIDMETA_INDEX("index-uidmeta"),
	/** A UIDMeta deletion */
	UIDMETA_DELETE("delete-uidmeta"),
	/** A search query event */
	SEARCH("search");
	
	/** An all zeroes bit mask template */
	public static final String BITS = "0000000000000000000000000000000000000000000000000000000000000000";

	
	private TSDBEventType(final String shortName) {
		this.shortName = shortName;
	}
	
	/** The event short name */
	public final String shortName;
	/** The bitmask for this event type */
	public final int mask = Integer.parseInt("1" + BITS.substring(0, ordinal()), 2);
	
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
	

	

}
