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
package com.heliosapm.streams.opentsdb;

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import com.heliosapm.streams.metrics.Utils;
import com.heliosapm.utils.lang.StringHelper;

import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;

/**
 * <p>Title: InternalStatsCollector</p>
 * <p>Description: Internal stats collector to self-collect metrics in the TSDB.</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.opentsdb.InternalStatsCollector</code></p>
 */

public class InternalStatsCollector extends StatsCollector {
	protected final TSDB tsdb;
	protected final String host = ManagementFactory.getRuntimeMXBean().getName().split("@")[1];
	protected final String app = "tsdb";
	protected final Map<String, String> atags;
	
	/**
	 * Creates a new InternalStatsCollector
	 * @param tsdb The tsdb to write the metrics to
	 * @param prefix
	 */
	public InternalStatsCollector(final TSDB tsdb, final String prefix) {
		super(prefix);		
		this.tsdb = tsdb;
		final Map<String, String> tmp = new LinkedHashMap<String, String>();
		tmp.put("host", host);
		tmp.put("app", app);
		atags = Collections.unmodifiableMap(tmp);
		
	}
	
	@Override
	public void emit(final String datapoint) {
		// metricname, timestamp, value, [tags]
		try {
			final String[] parsedDatapoint = StringHelper.splitString(datapoint, ' ');
			//  <timestamp>, [<value>,] <metric-name>, <host>, <app> [,<tagkey1>=<tagvalue1>,<tagkeyn>=<tagvaluen>]
			long timestamp = Utils.toMsTime(parsedDatapoint[1]);
			
			if(parsedDatapoint[2].indexOf('.')!=-1) {
				tsdb.addPoint(parsedDatapoint[0], timestamp, Double.parseDouble(parsedDatapoint[2]), tagsFromArr(parsedDatapoint));
			} else {
				tsdb.addPoint(parsedDatapoint[0], timestamp, Long.parseLong(parsedDatapoint[2]), tagsFromArr(parsedDatapoint));
			}
			  
		} catch (Exception ex) {
			/* No Op */
		}
	}
	
	
	protected Map<String, String> tagsFromArr(final String...fields) {
		final Map<String, String> map = new LinkedHashMap<String, String>(atags);
		for(int i = 3; i < fields.length; i++) {
			final String[] tagPair = StringHelper.splitString(fields[3], '=', true);
			map.put(tagPair[0].trim().toLowerCase(), tagPair[1].trim().toLowerCase());
		}
		return map;
	}
	

}
