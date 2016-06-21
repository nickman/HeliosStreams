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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * <p>Title: Utils</p>
 * <p>Description: Statis utility methods</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.Utils</code></p>
 */

public class Utils {
	
	/** The text line timestamp extractor */
	public static final TimestampExtractor TEXT_TS_EXTRACTOR = new TextLineTimestampExtractor();

	//  [<value-type>,]<timestamp>, [<value>,] <metric-name>, <host>, <app> [,<tagkey1>=<tagvalue1>,<tagkeyn>=<tagvaluen>]
	
	
	/**
	 * Determines the {@link ValueType} of the metric text line in the passed string builder.
	 * If the metric is undirected, the the value type character will be trimmed out of the string builder. 
	 * @param textLine The metric text line to determine the value type for
	 * @return the value type
	 */
	public static ValueType valueType(final StringBuilder textLine) {
		final ValueType v = ValueType.decode(textLine.charAt(0));
		if(v!=ValueType.X) {
			textLine.deleteCharAt(0);
		}
		return v;		 
	}
	
	/**
	 * Converts the passed string to a ms timestamp.
	 * If the parsed long has less than 13 digits, it is assumed to be in seconds.
	 * Otherwise assumed to be in milliseconds.
	 * @param value The string value to parse
	 * @return a ms timestamp
	 */
	public static long toMsTime(final String value) {
		final long v = Long.parseLong(value.trim());
		return digits(v) < 13 ? TimeUnit.SECONDS.toMillis(v) : v; 
	}
	
	/**
	 * Determines the number of digits in the passed long
	 * @param v The long to test
	 * @return the number of digits
	 */
	public static int digits(final long v) {
		if(v==0) return 1;
		return (int)(Math.log10(v)+1);
	}
	
	/**
	 * Optimized version of {@code String#split} that doesn't use regexps.
	 * This function works in O(5n) where n is the length of the string to
	 * split.
	 * @param s The string to split.
	 * @param c The separator to use to split the string.
	 * @param trimBlanks true to not return any whitespace only array items
	 * @return A non-null, non-empty array.
	 * <p>Copied from <a href="http://opentsdb.net">OpenTSDB</a>.
	 */
	public static String[] splitString(final String s, final char c, final boolean trimBlanks) {
		final char[] chars = s.toCharArray();
		int num_substrings = 1;
		final int last = chars.length-1;
		for(int i = 0; i <= last; i++) {
			char x = chars[i];
			if (x == c) {
				num_substrings++;
			}
		}
		final String[] result = new String[num_substrings];
		final int len = chars.length;
		int start = 0;  // starting index in chars of the current substring.
		int pos = 0;    // current index in chars.
		int i = 0;      // number of the current substring.
		for (; pos < len; pos++) {
			if (chars[pos] == c) {
				result[i++] = new String(chars, start, pos - start);
				start = pos + 1;
			}
		}
		result[i] = new String(chars, start, pos - start);
		if(trimBlanks) {
			int blanks = 0;
			final List<String> strs = new ArrayList<String>(result.length);
			for(int x = 0; x < result.length; x++) {
				if(result[x].trim().isEmpty()) {
					blanks++;
				} else {
					strs.add(result[x]);
				}
			}
			if(blanks==0) return result;
			return strs.toArray(new String[result.length - blanks]);
		}
		return result;
	}
	
	/**
	 * <p>Title: TextLineTimestampExtractor</p>
	 * <p>Description: The text stream metrics timestamp extractor</p> 
	 * <p>Company: Helios Development Group LLC</p>
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>com.heliosapm.streams.metrics.Utils.TextLineTimestampExtractor</code></p>
	 */
	public static class TextLineTimestampExtractor implements TimestampExtractor {
		@Override
		public long extract(final ConsumerRecord<Object, Object> record) {
			try {
				final String s = record.value().toString().trim();
				final int index = s.indexOf(',')+1;
				if(index==-1) return System.currentTimeMillis();			
				if(ValueType.isValueType(s.charAt(0))) {
					final int nextIndex = s.indexOf(',', index);
					return toMsTime(s.substring(index, nextIndex));
				}
				return toMsTime(s.substring(0, index-1));
			} catch (Exception ex) {
				return System.currentTimeMillis();
			}
			
		}
	}
	
	/** An empty string array const */
	public static final String[] EMPTY_STR_ARR = {};
	
	/**
	 * Extracts a string array from the named property in the passed properties
	 * @param p The properties to read from
	 * @param key The property key to read
	 * @param defaultValue The default value to return if the property is not defined or empty
	 * @return a String array
	 */
	public static String[] getArrayProperty(final Properties p, final String key, final String...defaultValue) {
		if(p==null || p.isEmpty()) throw new IllegalArgumentException("The passed properties was null or empty");
		if(key==null || key.trim().isEmpty()) throw new IllegalArgumentException("The passed key was null or empty");
		final String rawValue = p.getProperty(key);
		if(rawValue==null || rawValue.trim().isEmpty()) return defaultValue;
		final String[] arr = splitString(rawValue, ',', true);
		if(arr.length==0) return defaultValue;
		return arr;
	}
	
	/**
	 * Indicates if the passed pre-trimmed numeric string is a double
	 * @param value the string to test
	 * @return true if a double (or a float), false otherwise
	 */
	public static boolean isDouble(final String value) {
		return value.indexOf('.')!=-1;
	}
	
	/**
	 * Attempts to parse the passed pre-trimmed string to a numeric
	 * @param value the string to parse
	 * @return a number or null if it was not numeric
	 */
	public static Number numeric(final String value) {
		try {
			return Double.parseDouble(value);			
		} catch (Exception ex) {
			return null;
		}
	}
	
	
	
//	public static byte[] renderSubmitMetric(final StringBuilder textLine) {
//		
//	}
	
	
	
	private Utils() {}

}
