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
package com.heliosapm.streams.sqlbinder;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.sun.rowset.CachedRowSetImpl;

/**
 * <p>Title: TSDBCachedRowSetImpl</p>
 * <p>Description: Convenience cached rowset implementation that supports determining if the set is closed and can provide back the populating statement.</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.sqlbinder.TSDBCachedRowSetImpl</code></p>
 */

@SuppressWarnings("restriction")
public class TSDBCachedRowSetImpl extends CachedRowSetImpl {
	/**  */
	private static final long serialVersionUID = 6545448310433875828L;
	/** Indicates if the rowset is closed */
	protected boolean closed = true;
	/** The underlying statement that populated the rowset */
	protected Statement statement = null;
	

	/**
	 * Creates a new TSDBCachedRowSetImpl
	 * @throws SQLException thrown on any SQL error
	 */
	public TSDBCachedRowSetImpl() throws SQLException {
		super();
	}


	
	
	/**
	 * {@inheritDoc}
	 * @see com.sun.rowset.CachedRowSetImpl#size()
	 */	
	@Override
	public int size() {
		return super.size();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.sun.rowset.CachedRowSetImpl#populate(java.sql.ResultSet)
	 */
	@Override
	public void populate(final ResultSet rset) throws SQLException {
		closed = false;
		statement = rset.getStatement();
		super.populate(rset);
	}
	
	@Override
	public Statement getStatement() throws SQLException {		
		return statement;
	}
	
	@Override
	public boolean isClosed() throws SQLException {
		return closed;
	}
	
	@Override
	public void populate(final ResultSet rset, final int i) throws SQLException {
		closed = false;
		statement = rset.getStatement();
		super.populate(rset, i);
	}
	
	@Override
	public void close() throws SQLException {
		closed = true;		
		super.close();
	}

}
