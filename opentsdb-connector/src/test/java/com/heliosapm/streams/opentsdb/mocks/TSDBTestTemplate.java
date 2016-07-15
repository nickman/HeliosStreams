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
package com.heliosapm.streams.opentsdb.mocks;

import java.util.List;
import java.util.Map;

import com.heliosapm.streams.opentsdb.KafkaRPCTest;
import com.stumbleupon.async.Deferred;

import net.opentsdb.meta.Annotation;
import net.opentsdb.search.SearchPlugin;
import net.opentsdb.tsd.RTPublisher;
import net.opentsdb.tsd.RpcPlugin;

/**
 * <p>Title: TSDBTestTemplate</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.opentsdb.mocks.TSDBTestTemplate</code></p>
 */

public class TSDBTestTemplate extends EmptyTSDB {
	  /** Search indexer to use if configure */
	  private SearchPlugin search = null;
	  
	  /** Optional real time pulblisher plugin to use if configured */
	  private RTPublisher rt_publisher = null;
	  
	  /** List of activated RPC plugins */
	  private List<RpcPlugin> rpc_plugins = null;
	  
	  
	
   /**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.EmptyTSDB#addPoint(java.lang.String, long, double, java.util.Map)
	 */
	@Override
	public Deferred<Object> addPoint(String metric, long timestamp,
			double value, Map<String, String> tags) {
		KafkaRPCTest.latch.get().countDown();
		if (rt_publisher != null) {
			rt_publisher.publishDataPoint(metric, timestamp, value, tags, (metric + ":" + tags.toString()).getBytes());
	    }
		return Deferred.fromResult(null);
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.test.mocks.EmptyTSDB#addPoint(java.lang.String, long, long, java.util.Map)
	 */
	@Override
	public Deferred<Object> addPoint(String metric, long timestamp,
			long value, Map<String, String> tags) {
		KafkaRPCTest.latch.get().countDown();
		if (rt_publisher != null) {
			rt_publisher.publishDataPoint(metric, timestamp, value, tags, (metric + ":" + tags.toString()).getBytes());
	    }
		return Deferred.fromResult(null);
	}
	
	public void indexAnnotation(final Annotation note) {
		if (search != null) {
			search.indexAnnotation(note);
		} else {
			if( rt_publisher != null ) {
				rt_publisher.publishAnnotation(note);
			}
		}
	}
	
	

}
