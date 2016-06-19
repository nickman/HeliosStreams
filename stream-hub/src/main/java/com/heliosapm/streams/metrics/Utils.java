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

/**
 * <p>Title: Utils</p>
 * <p>Description: Statis utility methods</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.Utils</code></p>
 */

public class Utils {

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
	
//	public static byte[] renderSubmitMetric(final StringBuilder textLine) {
//		
//	}
	
	
	
	private Utils() {}

}
