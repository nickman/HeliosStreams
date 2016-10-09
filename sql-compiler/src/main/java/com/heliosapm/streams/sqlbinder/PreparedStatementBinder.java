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

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import javax.management.ObjectName;

/**
 * <p>Title: PreparedStatementBinder</p>
 * <p>Description: Defines the prepared statement binder and metrics tracker for a given SQL statement</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.sqlbinder.PreparedStatementBinder</code></p>
 */
public interface PreparedStatementBinder {
	
	/**
	 * Returns this binder's JMX ObjectName
	 * @return this binder's JMX ObjectName
	 */
	public ObjectName getObjectName();
	
	/**
	 * Binds the passed arguments against the passed PreparedStatement
	 * @param ps The PreparedStatement to bind against
	 * @param args The arguments to bind
	 */
	public void bind(PreparedStatement ps, Object...args);
	
	/**
	 * Unbinds the current row of the passed result set to an object array
	 * @param rset The ResultSet to extract the object array from
	 * @return the Object array
	 */
	public Object[] unbind(ResultSet rset);
	
	
	/**
	 * Returns the elapsed time that the specified percentage of executions fall within in ms.
	 * @param p The percentile to report (between 1 and 99 inclusive)
	 * @return the execution time percentile
	 */
	public long ptileExecTime(final int p);

	/**
	 * Records an execution time in ns
	 * @param ns The elapsed time to record in ns.
	 */
	public void recordExecTime(final long ns);
	
	/**
	 * Records a batch execution time in ns
	 * @param ns The elapsed time to record in ns.
	 */
	public void recordBatchExecTime(final long ns);
	
	/**
	 * Records a batch size
	 * @param size The number of statements executed in the batch
	 */
	public void recordBatchExecSize(final long size);
	
	/**
	 * Increments the error count
	 */
	public void recordError();

}
