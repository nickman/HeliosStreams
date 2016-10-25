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
package com.heliosapm.streams.metrichub.impl;

import java.sql.ResultSet;
import java.util.NoSuchElementException;

/**
 * <p>Title: AbstractIndexProvidingIterator</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrichub.impl.AbstractIndexProvidingIterator</code></p>
 */
public abstract class AbstractIndexProvidingIterator<T> implements IndexProvidingIterator<T> {
	/** The result set driving the iterator */
	private final ResultSet rset;
	/** The retained "next" result */
	private boolean hasnext = true;
	/** The current "nexted" uitem */
	private T currentItem = null;

	/**
	 * Creates a new AbstractIndexProvidingIterator
	 * @param rset The result set driving the iterator
	 */
	public AbstractIndexProvidingIterator(final ResultSet rset) {
		this.rset = rset;
	}

	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.catalog.IndexProvidingIterator#pushBack()
	 */
	@Override
	public void pushBack() {
		currentItem = null;
		try {			
			if(!rset.previous()) {
				throw new RuntimeException("ResultSet was on first row");
			}			
		} catch (Exception ex) {
			throw new RuntimeException("Failed to pushback result set", ex);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext() {
		try {
			currentItem = null;
			hasnext = rset.next();
			return hasnext;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see java.util.Iterator#next()
	 */
	@Override
	public T next() {
		if(!hasnext) throw new NoSuchElementException();
		currentItem = build();
		return currentItem;
	}
	
	/**
	 * Builds the next item from the passed result set
	 * @return the built item
	 */
	protected abstract T build();
	
	/**
	 * Returns the index of the current item in scope
	 * @param t The current item in scope
	 * @return the index of the current item in scope
	 */
	protected abstract Object getIndex(T t);

	/**
	 * {@inheritDoc}
	 * @see java.util.Iterator#remove()
	 */
	@Override
	public void remove() {
		throw new UnsupportedOperationException("remove() not supported in this iterator");
		
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.catalog.IndexProvidingIterator#getIndex()
	 */
	@Override
	public Object getIndex() throws NoSuchElementException {
		if(!hasnext || currentItem==null) throw new NoSuchElementException();
		return getIndex(currentItem);
	}

}

