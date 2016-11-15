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
package com.heliosapm.streams.collector.ds.pool;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.pool2.impl.GenericObjectPool;

import com.heliosapm.utils.collections.Props;
import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.jmx.JMXHelper;

/**
 * <p>Title: TestMQPool</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.ds.pool.TestMQPool</code></p>
 */

public class TestMQPool {

	public static final String TEST_PROPS = 
			"factory=com.heliosapm.streams.collector.ds.pool.impls.MQPCFPoolBuilder\n" + 
			"maxidle=3\n" +
			"maxtotal=5\n" +
			"minidle=1\n" +
			"blockonex=true\n" +
			"maxwait=10000\n" +
			"testonget=true\n" +
			"testoncreate=true\n" +
			"name=VMECSQMGR\n" +
			"mq.host=localhost\n" +
			"mq.port=1430\n" +
			"mq.channel=JBOSS.SVRCONN";

	
	/**
	 * Creates a new TestMQPool
	 */
	public TestMQPool() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			log("Pool Test");
			log(TEST_PROPS);
			JMXHelper.fireUpJMXMPServer(1077);
			final Properties p = Props.strToProps(TEST_PROPS);
			log("Props:" + p);
			final GenericObjectPool<Object> pool = (GenericObjectPool<Object>)PoolConfig.deployPool(p);
			pool.preparePool();
			log("Pool Deployed:" + pool.getNumIdle());
			final List<Object> objects = new ArrayList<Object>();
			for(int i = 0; i < 4; i++) {
				try {
					final Object o = pool.borrowObject();
					log("Borrowed:" + o);
					objects.add(o);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			log("Objects:" + objects.size());
			StdInCommandHandler.getInstance()
			.registerCommand("close", new Runnable(){
				public void run() {
					for(Object o: objects) {
						pool.returnObject(o);
					}
					objects.clear();
					try {
						pool.close();
					} catch (Exception ex) {
						ex.printStackTrace(System.err);
					}
				}
			})
			.run();
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
		}

	}

	public static void log(final Object msg) {
		System.out.println(msg);
		
	}
	
}
