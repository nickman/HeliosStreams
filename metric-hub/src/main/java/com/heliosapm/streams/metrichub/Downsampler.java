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

import java.util.regex.Pattern;

/**
 * <p>Title: Downsampler</p>
 * <p>Description: The OpenTSDB supported downsamplers as of 2.3. For Quantile details see https://en.wikipedia.org/wiki/Quantile.</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrichub.Downsampler</code></p>
 */

public enum Downsampler {
	/** Averages the data points with Linear Interpolation */
	AVG,
	/** The number of raw data points in the set with Zero if missing */
	COUNT,
	/** Calculates the standard deviation with Linear Interpolation */
	DEV,
	/** Calculates the estimated 50th percentile with the R-3 method with Linear Interpolation */
	EP50R3,
	/** Calculates the estimated 50th percentile with the R-7 method with Linear Interpolation */
	EP50R7,
	/** Calculates the estimated 75th percentile with the R-3 method with Linear Interpolation */
	EP75R3,
	/** Calculates the estimated 75th percentile with the R-7 methodwith Linear Interpolation */
	EP75R7,
	/** Calculates the estimated 90th percentile with the R-3 method with Linear Interpolation */
	EP90R3,
	/** Calculates the estimated 90th percentile with the R-7 method with Linear Interpolation */
	EP90R7,
	/** Calculates the estimated 95th percentile with the R-3 method with Linear Interpolation */
	EP95R3,
	/** Calculates the estimated 95th percentile with the R-7 method with Linear Interpolation */
	EP95R7,
	/** Calculates the estimated 99th percentile with the R-3 method with Linear Interpolation */
	EP99R3,
	/** Calculates the estimated 99th percentile with the R-7 method with Linear Interpolation */
	EP99R7,
	/** Calculates the estimated 999th percentile with the R-3 method with Linear Interpolation */
	EP999R3,
	/** Calculates the estimated 999th percentile with the R-7 method with Linear Interpolation */
	EP999R7,
	/** Selects the smallest data point with Maximum if missing */
	MIMMIN,
	/** Selects the largest data point with Minimum if missing */
	MIMMAX,
	/** Selects the smallest data point with Linear Interpolation */
	MIN,
	/** Selects the largest data point with Linear Interpolation */
	MAX,
	/** Calculates the 50th percentile with Linear Interpolation */
	P50,
	/** Calculates the 75th percentile with Linear Interpolation */
	P75,
	/** Calculates the 90th percentile with Linear Interpolation */
	P90,
	/** Calculates the 95th percentile with Linear Interpolation */
	P95,
	/** Calculates the 99th percentile with Linear Interpolation */
	P99,
	/** Calculates the 999th percentile with Linear Interpolation */
	P999,
	/** Adds the data points together with Linear Interpolation */
	SUM,
	/** Adds the data points together interpolating with Zero if missing */
	ZIMSUM;
	
	
	public static final Pattern DOWNSAMPLER_PATTERN;
	
	private static final Downsampler[] values = values();
	
	static {
		final StringBuilder b = new StringBuilder("(\\d+)([");
		for(Interval inter: Interval.values()) {
			b.append(inter.name().toLowerCase()).append("|");
		}
		b.deleteCharAt(b.length()-1).append("])\\-([");
		for(int i = 0; i < values.length; i++) {
			b.append(values[i].name().toLowerCase()).append("|");
		}
		b.deleteCharAt(b.length()-1).append("])\\-([");
		for(FillPolicy fp: FillPolicy.values()) {
			b.append(fp.name().toLowerCase()).append("|");
		}
		b.deleteCharAt(b.length()-1).append("])$");
		DOWNSAMPLER_PATTERN = Pattern.compile(b.toString(), Pattern.CASE_INSENSITIVE);
	}

	public static void main(String[] args) {
		System.out.println(DOWNSAMPLER_PATTERN);
	}


}
