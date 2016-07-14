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

import org.junit.BeforeClass;
import org.junit.Test;

import com.heliosapm.aop.retransformer.Retransformer;
import com.heliosapm.streams.opentsdb.mocks.TSDBTestTemplate;
import com.heliosapm.utils.time.SystemClock;

import net.opentsdb.core.TSDB;

/**
 * <p>Title: KafkaRPCTest</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.opentsdb.KafkaRPCTest</code></p>
 */

public class KafkaRPCTest extends BaseTest {
	private static TSDB tsdb = null;
	/**
	 * Creates the TSDB instance and plugin jar
	 */
	@BeforeClass
	public static void init() {
		
		createPluginJar(KafkaRPC.class);
		Retransformer.getInstance().transform(TSDB.class, TSDBTestTemplate.class);
		
		tsdb = newTSDB("coretest");
	}
	
	@Test
	public void go() {
		SystemClock.sleep(100000000);
	}
}
