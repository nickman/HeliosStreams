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
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.heliosapm.streams.common.naming.AgentName;
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
	protected final Map<String, String> atags;
	/** Instance logger */
	protected final Logger log = LoggerFactory.getLogger(getClass());

	/**
	 * Creates a new InternalStatsCollector
	 * @param tsdb The tsdb to write the metrics to
	 * @param prefix
	 */
	public InternalStatsCollector(final TSDB tsdb, final String prefix) {
		super(prefix);		
		this.tsdb = tsdb;
		final Map<String, String> tmp = new LinkedHashMap<String, String>();
		final AgentName am = AgentName.getInstance();
		tmp.put("host", am.getHostName());
		tmp.put("app", am.getAppName());
		atags = Collections.unmodifiableMap(tmp);
		
	}
	
	// java.lang.IllegalArgumentException: Invalid metric name (
	// "tsd.pointsAdded.KafkaRPC.host=pp-dt-nwhi-01.app=opentsdb.rate.1m"): 
	// illegal character: =
	
	@Override
	public void emit(final String datapoint) {
		// metricname, timestamp, value, [tags]
		try {
			final String[] parsedDatapoint = StringHelper.splitString(datapoint.trim(), ' ', true);
			//  <timestamp>, [<value>,] <metric-name>, <host>, <app> [,<tagkey1>=<tagvalue1>,<tagkeyn>=<tagvaluen>]
			long timestamp = toMsTime(parsedDatapoint[1]);
			
			if(parsedDatapoint[2].indexOf('.')!=-1) {
				tsdb.addPoint(parsedDatapoint[0], timestamp, Double.parseDouble(parsedDatapoint[2]), tagsFromArr(parsedDatapoint));
			} else {
				tsdb.addPoint(parsedDatapoint[0], timestamp, Long.parseLong(parsedDatapoint[2]), tagsFromArr(parsedDatapoint));
			}
			  
		} catch (Throwable ex) {
			log.error("Failed to add data point [" + datapoint + "]", ex);
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
	 * Determines the number of digits in the passed long
	 * @param v The long to test
	 * @return the number of digits
	 */
	public static int digits(final long v) {
		if(v==0) return 1;
		return (int)(Math.log10(v)+1);
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
