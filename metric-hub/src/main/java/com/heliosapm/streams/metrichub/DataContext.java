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

import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

/**
 * <p>Title: DataContext</p>
 * <p>Description: Accepts a {@MetricsMetaAPI expression and a number of data query parameters to build an OpenTSDB <b>/api/query</b>
 * JSON request, and executes it against the provided server.</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrichub.DataQuery</code></p>
 */

public class DataContext {
	/** The start time in ms */
	protected long startTimeMs = -1L;
	/** The start time as an expression */
	protected String startTime = null;
	/** The end time in ms */
	protected long endTimeMs = -1L;
	/** The end time as an expression */
	protected String endTime = null;
	/** The aggregator to use */
	protected Aggregator aggregator = null;

	
	/** The downsampling expression */
	protected String downSampling = null;

	
	/** Whether or not to output data point timestamps in milliseconds (true) or seconds (false). */
	protected boolean msResolution = false;
	/** Whether or not to include annotations (true) or not (false). */
	protected boolean includeAnnotations = false;
	/** Whether or not to include global annotations (true) or not (false). */
	protected boolean includeGlobalAnnotations = false;
	/** Whether or not to TSUIDs in the response (true) or not (false). */
	protected boolean includeTsuids = false;
	/** Whether or not to show a performance summary in the response (true) or not (false). */
	protected boolean showSummary = false;
	/** Whether or not to show a the submitted query in the response (true) or not (false). */
	protected boolean showQuery = false;
	
	
	/** Indicates if the call to OpenTSDB is for a deletion. <b>BE CAREFUL !</b> */
	protected boolean deletion = false;
	
	
	/** The standard TS format */
	public static final String DEFAULT_TS_FORMAT = "yyyy/MM/dd-HH:mm:ss";  // eg. 2016/10/27-00:00:00
	
	/** The simple date formatter for the standard format. Be sure to use via synchronized method */
	private static final SimpleDateFormat DEFAULT_TS_SDF = new SimpleDateFormat(DEFAULT_TS_FORMAT);
	
	
	/**
	 * Creates a new DataContext
	 * @param startTime The start time in seconds or millis
	 * @param aggregator The aggregator to apply
	 */
	public DataContext(final long startTime, final Aggregator aggregator) {
		if(aggregator==null) throw new IllegalArgumentException("The passed aggregator was null");
		this.aggregator = aggregator;
		this.startTimeMs = toMsTime(startTime);
		
	}
	
	/**
	 * Creates a new DataContext
	 * @param startTime The start time in the standard format ({@link #DEFAULT_TS_FORMAT} or an "ago" expression, e.g. <b>4h-ago</b>
	 * @param aggregator The aggregator to apply
	 */
	public DataContext(final String startTime, final Aggregator aggregator) {
		if(startTime==null || startTime.trim().isEmpty()) throw new IllegalArgumentException("The passed startTime was null or empty");
		if(aggregator==null) throw new IllegalArgumentException("The passed aggregator was null");		
		this.aggregator = aggregator;
		if(startTime.toLowerCase().contains("-ago")) {
			this.startTime = startTime.trim();
			startTimeMs = -1L;
		} else {		
			startTimeMs = standardFormatToMs(startTime);
		}
	}
	
	/**
	 * Creates a new DataContext
	 * @param startTime The start time in a non-standard format 
	 * @param format The simple date format of the supplied start time
	 * @param aggregator The aggregator to apply
	 */
	public DataContext(final String startTime, final String format, final Aggregator aggregator) {
		if(aggregator==null) throw new IllegalArgumentException("The passed aggregator was null");
		if(startTime==null || startTime.trim().isEmpty()) throw new IllegalArgumentException("The passed startTime was null or empty");
		if(format==null || format.trim().isEmpty()) throw new IllegalArgumentException("The passed time format was null or empty");
		this.aggregator = aggregator;
		final SimpleDateFormat sdf = new SimpleDateFormat(format.trim());
		try {
			startTimeMs = sdf.parse(startTime.trim()).getTime();
		} catch (Exception ex) {
			throw new IllegalArgumentException("Failed to convert time [" + startTime + "] to supplied format [" + format + "]", ex);
		}
	}
	
	
	
	
	
	/**
	 * Formats a string in the default timestamp format to a long utc
	 * @param time The time string to format
	 * @return the supplied time in long utc
	 */
	public static long standardFormatToMs(final String time) {
		if(time==null || time.trim().isEmpty()) throw new IllegalArgumentException("The passed time was null or empty");
		try {
			synchronized(DEFAULT_TS_SDF) {
				return DEFAULT_TS_SDF.parse(time.trim()).getTime();
			}
		} catch (Exception ex) {
			throw new IllegalArgumentException("Failed to convert time [" + time + "] to standard format [" + DEFAULT_TS_FORMAT + "]", ex);
		}
	}
	
	/**
	 * Converts the passed string to a ms timestamp.
	 * If the parsed long has less than 13 digits, it is assumed to be in seconds.
	 * Otherwise assumed to be in milliseconds.
	 * @param value The string value to parse
	 * @return a ms timestamp
	 */
	public static long toMsTime(final String value) {
		final long v = (long)Double.parseDouble(value.trim());
		return digits(v) < 13 ? TimeUnit.SECONDS.toMillis(v) : v; 
	}
	
	/**
	 * Examines the passed timestamp and if determined to be in seconds, converts to millis
	 * @param v The time to test and convert
	 * @return the passed time in ms.
	 */
	public static long toMsTime(final long v) {
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

}
