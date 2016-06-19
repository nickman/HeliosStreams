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
package com.heliosapm.streams.metrics;

import gnu.trove.map.hash.TIntObjectHashMap;

/**
 * <p>Title: ValueType</p>
 * <p>Description: Enumerates the possible streamed metric value types</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.ValueType</code></p>
 */

public enum ValueType {
	/** Tracking an all time total of a specific event type */
	A("Accumulator", true, "Tracking an all time total of a specific event type"),
	/** Tracking the per seconds/minutes/hours/day rate of a specific event type */
	M("Meter", true, "Tracking the per seconds/minutes/hours/day rate of a specific event type"),
	/** The value of a given metric instance is the delta of the current and the prior value */
	D("Delta", false, "The value of a given metric instance is the delta of the current and the prior value"),
	/** Values are aggregated to count, min, max and average per defined period (e.g. 15 sedonds) */
	P("PeriodAggregation", false, "Values are aggregated to count, min, max and average per defined period (e.g. 15 sedonds)"),
	/** The metric is not aggregated and forwarded directly to endpoint */
	S("StraightThrough", false, "The metric is not aggregated and forwarded directly to endpoint"),
	/** The value type indicating the metric is directed (has no value type) */
	X("Directed", true, "The value type indicating the metric is directed (has no value type)");
	
	private ValueType(final String name, final boolean valueless, final String description) {
		this.name = name;
		this.valueless = valueless;
		this.description = description;
		charcode[0] = name().charAt(0);
		charcode[1] = name().toLowerCase().charAt(0);
	}
	
	private static final TIntObjectHashMap<ValueType> decodeByInt = new TIntObjectHashMap<ValueType>(values().length*2, 0f, -1);
	
	static {
		for(ValueType v: values()) {
			if(v==X) continue;
			decodeByInt.put(v.charcode[0], v);
			decodeByInt.put(v.charcode[1], v);
		}
	}
	
	/** The value type name */
	public final String name;
	/** Indicates if the value type is valueless */
	public final boolean valueless;
	/** A description of the value type */
	public final String description;
	/** The char representations for this value type */
	private final int[] charcode = new int[2];
	
	/**
	 * Decodes the passed character to the corresponding ValueType
	 * @param c The character to decode
	 * @return the decoded ValueType or {@link #X} if the char does not map to a ValueType
	 */
	public static ValueType decode(final char c) {
		final ValueType v = decodeByInt.get(c);
		return v==null ? X : v;
	}
	
	/**
	 * Determines if the passed character is a ValueType symbol
	 * @param c The char to test
	 * @return true if a ValueType symbol, false otherwise
	 */
	public static boolean isValueType(final char c) {
		return decodeByInt.get(c) != null;
	}
	
}
