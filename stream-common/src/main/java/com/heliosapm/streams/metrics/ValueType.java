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
	ACCUMULATOR('A', "Accumulator", "tsdb.metrics.accumulator", true, "Tracking an all time total of a specific event type"),
	/** Tracking the per seconds/minutes/hours/day rate of a specific event type */
	METER('M', "Meter", "tsdb.metrics.meter", true, "Tracking the per seconds/minutes/hours/day rate of a specific event type"),
	/** The value of a given metric instance is the delta of the current and the prior value */
	DELTA('D', "Delta", "tsdb.metrics.delta", false, "The value of a given metric instance is the delta of the current and the prior value"),
	/** Values are aggregated to count, min, max and average per defined period (e.g. 15 seconds) */
	PERIODAGG('P', "PeriodAggregation", "tsdb.metrics.pagg", false, "Values are aggregated to count, min, max and average per defined period (e.g. 15 sedonds)"),
	/** Values are aggregated to count, min, max and average per defined period (e.g. 15 seconds), but aggregations retain their values in the next period if there is no activity */
	PERIODAGGSTICKY('T', "StickyPeriodAggregation", "tsdb.metrics.spagg", false, "Values are aggregated to count, min, max and average per defined period (e.g. 15 seconds), but aggregations retain their values in the next period if there is no activity"),
	/** The metric is not aggregated and forwarded directly to endpoint */
	STRAIGHTTHROUGH('S', "StraightThrough", "tsdb.metrics.st", false, "The metric is not aggregated and forwarded directly to endpoint"),
	/** The value type indicating the metric is directed by its value type or a default */
	DIRECTED('X', "Directed", "tsdb.metrics.directed", true, "The value type indicating the metric is directed by its value type or a default");
	
	// U for undirected ?
	
	private static final ValueType[] values = values();
	private static final int MAX_INDEX = values.length -1;
	
	private ValueType(final char decode, final String name, final String topicName, final boolean valueless, final String description) {
		this.name = name;
		this.valueless = valueless;
		this.description = description;
		charCode = decode;
		charcode[0] = decode;
		charcode[1] = charcode[0] + 32; 
		this.topicName = topicName;
	}
	
	/**
	 * Returns the ValueType with the passed ordinal
	 * @param ordinal the ordinal to get a ValueType for
	 * @return the ValueType
	 */
	public static ValueType ordinal(final int ordinal) {
		if(ordinal<0 || ordinal > MAX_INDEX) throw new IllegalArgumentException("Invalid ordinal for ValueType [" + ordinal + "]");
		return values[ordinal];
	}
	
	private static final TIntObjectHashMap<ValueType> decodeByInt = new TIntObjectHashMap<ValueType>(values().length*2, 0f, -1);
	
	static {
		for(ValueType v: values()) {
			if(v==DIRECTED) continue;
			decodeByInt.put(v.charcode[0], v);
			decodeByInt.put(v.charcode[1], v);
		}
	}
	
	/** The value type name */
	public final String name;
	/** The char code for this ValueType */
	public final char charCode;
	/** Indicates if the value type is valueless */
	public final boolean valueless;
	/** A description of the value type */
	public final String description;
	/** The char representations for this value type */
	private final int[] charcode = new int[2];
	
	/** The default name of the topic this type of values is sent to */
	public final String topicName;
	
	public static void main(String[] args) {
		for(ValueType v: values()) {
			System.out.println(v.name() + ", cc[0]:" + ((char)v.charcode[0] ) + ", cc[1]:" + ((char)v.charcode[1] ));
		}
	}
	
	/**
	 * Decodes the passed character to the corresponding ValueType
	 * @param c The character to decode
	 * @return the decoded ValueType or {@link #DIRECTED} if the char does not map to a ValueType
	 */
	public static ValueType decode(final char c) {
		final ValueType v = decodeByInt.get(c);
		return v==null ? DIRECTED : v;
	}
	
	/**
	 * Decodes the first character in the passed string to the corresponding ValueType
	 * @param value The string to decode
	 * @return the decoded ValueType or {@link #DIRECTED} if the char does not map to a ValueType
	 */
	public static ValueType decode(final String value) {
		if(value==null || value.trim().isEmpty()) throw new IllegalArgumentException("The passed value was null");
		final ValueType v = decodeByInt.get(value.trim().charAt(0));
		return v;//==null ? DIRECTED : v;
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
