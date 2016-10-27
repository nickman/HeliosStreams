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

/**
 * <p>Title: Aggregator</p>
 * <p>Description: The OpenTSDB supported aggregators as of 2.3. For Quantile details see https://en.wikipedia.org/wiki/Quantile.</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrichub.Aggregator</code></p>
 */

public enum Aggregator {
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
	/** Skips group by aggregation of all time series interpolating with Zero if missing */
	NONE,
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
	
	private Aggregator() {
		
	}

}

/*
Avg
Calculates the average of all values across the time span or across multiple time series. This function will perform linear interpolation across time series. It's useful for looking at gauge metrics. Note that even though the calculation will usually result in a float, if the data points are recorded as integers, an integer will be returned losing some precision.

Count
Returns the number of data points stored in the series or range. When used to aggregate multiple series, zeros will be substituted. It's best to use this when downsampling.

Dev
Calculates the standard deviation across a span or time series. This function will perform linear interpolation across time series. It's useful for looking at gauge metrics. Note that even though the calculation will usually result in a float, if the data points are recorded as integers, an integer will be returned losing some precision.

Estimated Percentiles
Calculates various percentiles using a choice of algorithms. These are useful for series with many data points as some data may be kicked out of the calculation. When used to aggregate multiple series, the function will perform linear interpolation. See Wikipedia for details. Implementation is through the Apache Math library.

Max
The inverse of min, it returns the largest data point from all of the time series or within a time span. This function will perform linear interpolation across time series. It's useful for looking at the upper bounds of gauge metrics.

MimMin
The "maximum if missing minimum" function returns only the smallest data point from all of the time series or within the time span. This function will not perform interpolation, instead it will return the maximum value for the type of data specified if the value is missing. This will return the Long.MaxValue for integer points or Double.MaxValue for floating point values. See Primitive Data Types for details. It's useful for looking at the lower bounds of gauge metrics.

MimMax
The "minimum if missing maximum" function returns only the largest data point from all of the time series or within the time span. This function will not perform interpolation, instead it will return the minimum value for the type of data specified if the value is missing. This will return the Long.MinValue for integer points or Double.MinValue for floating point values. See Primitive Data Types for details. It's useful for looking at the upper bounds of gauge metrics.

Min
Returns only the smallest data point from all of the time series or within the time span. This function will perform linear interpolation across time series. It's useful for looking at the lower bounds of gauge metrics.

None
(2.3) Skips group by aggregation. This aggregator is useful for fetching the raw data from storage as it will return a result set for every time series matching the filters. Note that the query will throw an exception if used with a downsampler.

Percentiles
Calculates various percentiles. When used to aggregate multiple series, the function will perform linear interpolation. Implementation is through the Apache Math library.

Sum
Calculates the sum of all data points from all of the time series or within the time span if down sampling. This is the default aggregation function for the GUI as it's often the most useful when combining multiple time series such as gauges or counters. It performs linear interpolation when data points fail to line up. If you have a distinct series of values that you want to sum and you do not need interpolation, look at zimsum

ZimSum
Calculates the sum of all data points at the specified timestamp from all of the time series or within the time span. This function does not perform interpolation, instead it substitues a 0 for missing data points. This can be useful when working with discrete values.
*/
