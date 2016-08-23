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
package com.heliosapm.streams.metrics.router.util;

import org.apache.kafka.streams.kstream.Reducer;

import com.heliosapm.streams.metrics.StreamedMetric;

/**
 * <p>Title: StreamedMetricReducer</p>
 * <p>Description: Combines two {@link StreamedMetric} instances into one, summing up the values of both.
 * This reducer is intended for metering so assumes that all values are long based.</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.router.util.StreamedMetricReducer</code></p>
 */

public class StreamedMetricLongSumReducer implements Reducer<StreamedMetric> {


	@Override
	public StreamedMetric apply(final StreamedMetric sm1, final StreamedMetric sm2) {
		return sm1.forValue(1L).update(sm2.forValue(1L).getLongValue());
	}

}
