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

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

/**
 * <p>Title: PooledObjectFactoryBuilder</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.ds.pool.PooledObjectFactoryBuilder</code></p>
 */

public interface PooledObjectFactoryBuilder<T> {
//	/** The pooled object supplier */
//	protected final Supplier<T> supplier; 
//	/** The pooled object validator */
//	protected final Function<T, Boolean> validator;
//	/** The pooled object destroyer */
//	protected final Function<T, Void> closer;

	/**
	 * Creates the pooled object
	 * @return the pooled object
	 */
	public T create();
	/**
	 * Destroys the passed pooled object
	 * @param p The pooled object to destroy
	 */
	public void destroyObject(final PooledObject<T> p);
	
	/**
	 * Validates the passed pooled object
	 * @param p The pooled object to validate
	 * @return true if the object is valid, false otherwise
	 */
	public boolean validateObject(final PooledObject<T> p);
	
	/**
	 * Provides the supplier that will create pooled objects
	 * @return the supplier
	 */
	default public Supplier<T> supplier() {
		return new Supplier<T>() {
			@Override
			public T get() {				
				return create();
			}
		};
	}
	
	/**
	 * Provides the validating function that will validate pooled objects
	 * @return the validating function
	 */
	default public Function<PooledObject<T>, Boolean> validator() {
		return new Function<PooledObject<T>, Boolean>() {
			@Override
			public Boolean apply(final PooledObject<T> t) {
				return validateObject(t);
			}
		};
	}
	
	/**
	 * Provides the validating function that will validate pooled objects
	 * @return the validating function
	 */
	default public Function<PooledObject<T>, Void> closer() {
		return new Function<PooledObject<T>, Void>() {
			@Override
			public Void apply(final PooledObject<T> p) {
				try {
					destroyObject(p);					
				} catch (Exception x) {/* No Op */}
				return null;				
			}
		};
	}
	
	/**
	 * Creates a new PooledObjectFactory based on this builder
	 * @return a new PooledObjectFactory
	 */
	default public PooledObjectFactory<T> factory() {
		return new PooledObjectFactory<T>(supplier(), validator(), closer());
	}
	

	
	
}
