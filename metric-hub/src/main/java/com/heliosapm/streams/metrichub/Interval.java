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
package com.heliosapm.streams.metrichub;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>Title: Interval</p>
 * <p>Description: Enumerates the relative intervals for specifying query start and end times</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrichub.Interval</code></p>
 */

public enum Interval {
	/** Interval for seconds */
	S(1),
	/** Interval for minutes */
	M(60),
	/** Interval for hours */
	H(M.value * 60),
	/** Interval for days */
	D(H.value * 24),
	/** Interval for weeks */
	W(D.value * 7),
	/** Interval for years */
	Y(D.value * 365);
	
	private Interval(final int value) {
		this.value = value;
	}
	
	/** The interval relative value in seconds */
	public final int value;
	
	public static final Pattern INTERVAL_PATTERN;
	
	private static final Interval[] values = values();
	
	static {
		final StringBuilder b = new StringBuilder("(\\d+)([");
		for(int i = 0; i < values.length; i++) {
			b.append(values[i].name().toLowerCase()).append("|");
		}
		INTERVAL_PATTERN = Pattern.compile(b.deleteCharAt(b.length()-1).append("])\\-ago$").toString(), Pattern.CASE_INSENSITIVE);
	}
	
	public static void main(String[] args) {
		System.out.println(INTERVAL_PATTERN);
	}
	
	/**
	 * Decodes the passed stringy to an interval
	 * @param value The stringy to decode
	 * @return the decoded interval
	 */
	public static Interval forSymbol(final CharSequence value)  {
		if(value==null) throw new IllegalArgumentException("The passed value was null");
		final String v = value.toString().trim().toUpperCase();
		if(v.isEmpty()) throw new IllegalArgumentException("The passed value was empty");
		try {
			return Interval.valueOf(v);
		} catch (Exception ex) {
			throw new IllegalArgumentException("The passed value [" + value + "] is not a valid interval");
		}
	}
	
	/**
	 * Decodes the passed interval expression (e.g. <b><code>4h-ago</code></b>) to a ms timestamp in the past relative to now
	 * @param value The expression to decode
	 * @return the ms timestamp in the past relative to now
	 */
	public static long toMsTime(final CharSequence value) {
		if(value==null) throw new IllegalArgumentException("The passed value was null");
		final String v = value.toString().trim().toUpperCase();
		if(v.isEmpty()) throw new IllegalArgumentException("The passed value was empty");
		final long now = System.currentTimeMillis();
		final Matcher m = INTERVAL_PATTERN.matcher(v);
		if(!m.matches()) throw new IllegalArgumentException("The passed value [" + value + "] could not be decoded to time");
		final Interval inter = forSymbol(m.group(2));
		return now - (Long.parseLong(m.group(1)) * inter.value);		
	}
	
//case 's': break;                               // seconds
//case 'm': interval *= 60; break;               // minutes
//case 'h': interval *= 3600; break;             // hours
//case 'd': interval *= 3600 * 24; break;        // days
//case 'w': interval *= 3600 * 24 * 7; break;    // weeks
//case 'y': interval *= 3600 * 24 * 365; break;  // years
	
	
}
