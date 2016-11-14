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

import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * <p>Title: PooledObjectFactory</p>
 * <p>Description: Defines a factory that provides instances to populate a pool with</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.ds.pool.PooledObjectFactory</code></p>
 */

public class PooledObjectFactory<T> extends BasePooledObjectFactory<T> {
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** The pooled object supplier */
	protected final Supplier<T> supplier; 
	/** The pooled object validator */
	protected final Function<PooledObject<T>, Boolean> validator;
	/** The pooled object destroyer */
	protected final Function<PooledObject<T>, Void> closer;

	/**
	 * Creates a new PooledObjectFactory
	 * @param supplier The supplier lambda that will create the objects to be pooled
	 * @param validator The pooled object validator
	 * @param closer The pooled object destroyer
	 */
	public PooledObjectFactory(final Supplier<T> supplier, final Function<PooledObject<T>, Boolean> validator, final Function<PooledObject<T>, Void> closer) {
		this.supplier = supplier;
		this.validator = validator;
		this.closer = closer;
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.commons.pool2.BasePooledObjectFactory#create()
	 */
	@Override
	public T create() throws Exception {
		return supplier.get();
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.commons.pool2.BasePooledObjectFactory#wrap(java.lang.Object)
	 */
	@Override
	public PooledObject<T> wrap(final T obj) {
		return new DefaultPooledObject<T>(obj);
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.apache.commons.pool2.BasePooledObjectFactory#destroyObject(org.apache.commons.pool2.PooledObject)
	 */
	@Override
	public void destroyObject(final PooledObject<T> p) throws Exception {
		closer.apply(p);
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.apache.commons.pool2.BasePooledObjectFactory#validateObject(org.apache.commons.pool2.PooledObject)
	 */
	@Override
	public boolean validateObject(final PooledObject<T> p) {
		return validator.apply(p);
	}

}
