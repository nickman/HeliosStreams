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
 * <p>Description: Accepts a {@link MetricsMetaAPI} expression and a number of data query parameters to build an OpenTSDB <b>/api/query</b>
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
	
	/** The rate options */
	protected RateOptions rateOptions = null;

	
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
	
	
	static class RateOptions {
		/** Whether or not the underlying data is a monotonically increasing counter that may roll over */
		boolean monotonic = false;
		/** A positive number representing the maximum value for the counter. */
		long counterMax = Long.MAX_VALUE;
		/** An optional value that, when exceeded, will cause the aggregator to return a 0 instead of the calculated rate. */
		long resetValue = 0L;
		
		/**
		 * Renders the options for query building. e.g. <b><code>:rate{counter,,1000}</code></b>
		 * {@inheritDoc}
		 * @see java.lang.Object#toString()
		 */
		public String toString() {			
			final StringBuilder b = new StringBuilder(":rate{");
			if(monotonic) b.append("counter,").append(counterMax).append(",");
			return b.append(resetValue).append("}").toString();
		}
	}
	
	
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
	 * Enables rate reporting and sets the reset value
	 * @param resetValue The value at which the aggregator returns zero instead of the calculated rate
	 * @return this data context
	 */
	public DataContext rateResetValue(final long resetValue) {
		if(rateOptions==null) rateOptions = new RateOptions();
		rateOptions.resetValue = resetValue;
		return this;
	}
	
	
	/**
	 * Enables rate reporting and sets the rate options maximum value for the counter and enables monotonic
	 * @param counterMax A positive number representing the maximum value for the counter
	 * @return this data context
	 */
	public DataContext rateCounterMax(final long counterMax) {
		if(counterMax < 1) throw new IllegalArgumentException("Countermax must be positive");
		if(rateOptions==null) rateOptions = new RateOptions();
		rateOptions.counterMax = counterMax;
		rateOptions.monotonic = true;
		return this;
	}
	
	/**
	 * Disables rate reporting
	 * @return this data context
	 */
	public DataContext unrate() {
		rateOptions = null;
		return this;
	}

	/**
	 * Enables rate reporting and sets the rate options monotonic counter to true
	 * @return this data context
	 */
	public DataContext rateMonotonic() {
		if(rateOptions==null) rateOptions = new RateOptions();
		rateOptions.monotonic = true;
		return this;
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

	/**
	 * Returns the start time as a ms timestamp, or -1 if an expression is being used 
	 * @return the startTimeMs  the start time as a ms timestamp, or -1 if an expression is being used
	 */
	public long getStartTimeMs() {
		return startTimeMs;
	}

	/**
	 * Sets the start time as a ms or s timestamp
	 * @param startTime the start time
	 * @return this data context
	 */
	public DataContext start(final long startTime) {
		this.startTimeMs = toMsTime(startTimeMs);
		this.startTime = null;
		return this;
	}

	/**
	 * Returns the start time expression or null if one is not set
	 * @return the startTime expression or null if one is not set
	 */
	public String getStartTime() {
		return startTime;
	}

	/**
	 * Sets the start time expression
	 * @param startTime the startTime to set
	 * @return this data context
	 */
	public DataContext startTime(final String startTime) {
		Interval.toMsTime(startTime); // to validate
		this.startTime = startTime;
		startTimeMs = -1L;
		return this;
	}

	/**
	 * Returns the end time expression or null if one is not set
	 * @return the end time expression or null if one is not set
	 */
	public String getEndTime() {
		return endTime;
	}

	/**
	 * Sets the end time expression
	 * @param endTime the endTime to set
	 * @return this data context
	 */
	public DataContext endTime(final String endTime) {
		Interval.toMsTime(endTime); // to validate
		this.endTime = endTime;
		endTimeMs = -1L;
		return this;
	}

	/**
	 * Returns the aggregator
	 * @return the aggregator
	 */
	public Aggregator getAggregator() {
		return aggregator;
	}

	/**
	 * Sets the aggregator
	 * @param aggregator the aggregator to set
	 * @return this data context
	 */
	public DataContext aggregator(final Aggregator aggregator) {
		if(aggregator==null) throw new IllegalArgumentException("The passed aggregator was null");
		this.aggregator = aggregator;
		return this;
	}

	/**
	 * Returns the downsampling expression or null if one is not set
	 * @return the downSampling expression
	 */
	public String getDownSampling() {
		return downSampling;
	}

	/**
	 * Sets the downsampling expression
	 * @param downSampling the downSampling to set
	 * @return this data context
	 */
	public DataContext downSampling(final String downSampling) {
		if(downSampling==null || downSampling.trim().isEmpty()) throw new IllegalArgumentException("The passed downsampling expression was null or empty");
		this.downSampling = downSampling;
		return this;
	}
	
	/**
	 * Disables downsampling
	 * @return this data context
	 */
	public DataContext noDownSample() {
		this.downSampling = null;
		return this;
	}
	

	/**
	 * Returns the
	 * @return the msResolution
	 */
	public boolean isMsResolution() {
		return msResolution;
	}

	/**
	 * Sets the
	 * @param msResolution the msResolution to set
	 */
	public void setMsResolution(boolean msResolution) {
		this.msResolution = msResolution;
	}

	/**
	 * Returns the
	 * @return the includeAnnotations
	 */
	public boolean isIncludeAnnotations() {
		return includeAnnotations;
	}

	/**
	 * Sets the
	 * @param includeAnnotations the includeAnnotations to set
	 */
	public void setIncludeAnnotations(boolean includeAnnotations) {
		this.includeAnnotations = includeAnnotations;
	}

	/**
	 * Returns the
	 * @return the includeGlobalAnnotations
	 */
	public boolean isIncludeGlobalAnnotations() {
		return includeGlobalAnnotations;
	}

	/**
	 * Sets the
	 * @param includeGlobalAnnotations the includeGlobalAnnotations to set
	 */
	public void setIncludeGlobalAnnotations(boolean includeGlobalAnnotations) {
		this.includeGlobalAnnotations = includeGlobalAnnotations;
	}

	/**
	 * Returns the
	 * @return the includeTsuids
	 */
	public boolean isIncludeTsuids() {
		return includeTsuids;
	}

	/**
	 * Sets the
	 * @param includeTsuids the includeTsuids to set
	 */
	public void setIncludeTsuids(boolean includeTsuids) {
		this.includeTsuids = includeTsuids;
	}

	/**
	 * Returns the
	 * @return the showSummary
	 */
	public boolean isShowSummary() {
		return showSummary;
	}

	/**
	 * Sets the
	 * @param showSummary the showSummary to set
	 */
	public void setShowSummary(boolean showSummary) {
		this.showSummary = showSummary;
	}

	/**
	 * Returns the
	 * @return the showQuery
	 */
	public boolean isShowQuery() {
		return showQuery;
	}

	/**
	 * Sets the
	 * @param showQuery the showQuery to set
	 */
	public void setShowQuery(boolean showQuery) {
		this.showQuery = showQuery;
	}

	/**
	 * Returns the
	 * @return the deletion
	 */
	public boolean isDeletion() {
		return deletion;
	}

	/**
	 * Sets the
	 * @param deletion the deletion to set
	 */
	public void setDeletion(boolean deletion) {
		this.deletion = deletion;
	}	

}
