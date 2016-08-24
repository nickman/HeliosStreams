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

import org.apache.kafka.streams.kstream.Window;

/**
 * <p>Title: ITimeWindowSummary</p>
 * <p>Description: Defines the computation that summarizes the reported timestamp of
 * a single metric reduced from a window of time. i.e. if a window starts at <b>T1</b>
 * and ends at <b>T2</b>, the timestamp could be reported as <b>T1</b>, or <b>T2</b>
 * or somewhere in between.</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.router.util.ITimeWindowSummary</code></p>
 */

public interface ITimeWindowSummary {
	/**
	 * Returns the summary timestamp for the passed window
	 * @param window The window representing a start time of <b>T1</b> and an end time of <b>T2</b>.
	 * @return the flattended time is ms.
	 */
	public long time(final Window window);
}
