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
 * <p>Title: FillPolicy</p>
 * <p>Description: The OpenTSDB supported downsampler fill policies as of 2.3.</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrichub.FillPolicy</code></p>
 */

public enum FillPolicy {
	/** The default behavior that does not emit missing values during serialization and performs linear interpolation (or otherwise specified interpolation) when aggregating series. */
	NONE,
	/** Emits a NaN in the serialization output when all values are missing in a series. Skips series in aggregations when the value is missing. */
	NAN,
	/** Same behavior as {@link #NAN} except that during serialization it emits a null instead of a NaN. */
	NULL,
	/** Substitutes a zero when a timestamp is missing. The zero value will be incorporated in aggregated results. */
	ZERO;
}
