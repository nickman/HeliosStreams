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
 * <p>Title: TimeWindowSummary</p>
 * <p>Description: Functional enumeration of some predefined {@link ITimeWindowSummary} implementations.</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.router.util.TimeWindowSummary</code></p>
 */

public enum TimeWindowSummary implements ITimeWindowSummary {
	/** The start of the window */
	START{
		@Override
		public long time(final Window window) {
			if(window==null) throw new IllegalArgumentException("The passed window was null");
			return window.start();
		}
		@Override
		public long time(final long[] triplet) {
			return triplet[0];
		}
	},
	/** The end of the window */
	END{
		@Override
		public long time(final Window window) {
			if(window==null) throw new IllegalArgumentException("The passed window was null");
			return window.end();
		}
		@Override
		public long time(final long[] triplet) {
			return triplet[1];
		}		
	},
	/** The middle of the window */
	MIDDLE{
		@Override
		public long time(final Window window) {
			if(window==null) throw new IllegalArgumentException("The passed window was null");
			final double diff = window.end() - window.start();
			if(diff==0D) return window.end();
			return window.start() + (long)diff/2;
		}
		@Override
		public long time(final long[] triplet) {
			final double diff = triplet[1] - triplet[0];
			if(diff==0D) return triplet[1];
			return triplet[0] + (long)diff/2;
		}		
	};
}
