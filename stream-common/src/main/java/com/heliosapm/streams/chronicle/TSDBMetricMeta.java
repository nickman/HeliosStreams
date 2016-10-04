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
package com.heliosapm.streams.chronicle;

import java.util.HashMap;
import java.util.TreeMap;

import com.heliosapm.streams.tracing.TagKeySorter;

/**
 * <p>Title: TSDBMetricMeta</p>
 * <p>Description: Represents a unique metric definition, not an instance</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.chronicle.TSDBMetricMeta</code></p>
 */

public class TSDBMetricMeta {
	/** The metric name */
	protected String metricName = null;
	/** The metric tsuid */
	protected byte[] tsuid = null;	
	/** The metric tags */
	protected final TreeMap<String, String> tags = new TreeMap<String, String>(TagKeySorter.INSTANCE);
	/** The tag key UIDs */
	protected final HashMap<String, byte[]> tagKeyUids = new HashMap<String, byte[]>(8);
	/** The tag value UIDs */
	protected final HashMap<String, byte[]> tagValueUids = new HashMap<String, byte[]>(8);
	
	/**
	 * Creates a new TSDBMetricMeta
	 */
	public TSDBMetricMeta() {
		// TODO Auto-generated constructor stub
	}

}
