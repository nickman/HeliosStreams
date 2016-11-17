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

import org.apache.commons.pool2.impl.GenericObjectPool;

/**
 * <p>Title: PoolAwareFactory</p>
 * <p>Description: An object factory that accepts the pool using the factory to be injected into it</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.ds.pool.PoolAwareFactory</code></p>
 */

public interface PoolAwareFactory<T> {
	/**
	 * Sets the factory on the implementor
	 * @param pool The pool
	 */
	public void setPool(GenericObjectPool<T> pool);
}
