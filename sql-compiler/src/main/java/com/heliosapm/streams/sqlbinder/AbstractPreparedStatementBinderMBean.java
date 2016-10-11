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


/**
 * <p>Title: AbstractPreparedStatementBinderMBean</p>
 * <p>Description: The JMX MBean interface for {@link PreparedStatementBinder} instances</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.sqlbinder.AbstractPreparedStatementBinderMBean</code></p>
 */

public interface AbstractPreparedStatementBinderMBean {
	
	/**
	 * Returns the SQL statement for this binder
	 * @return the SQL statement for this binder
	 */
	public String getSQL();
	
	/**
	 * Returns the number of times this binder has been executed
	 * @return the binder's execution count
	 */
	public long getExecutionCount();
	
	/**
	 * Returns the number of exceptions thrown when executing this binder's SQL
	 * @return the number of exceptions thrown when executing this binder's SQL
	 */
	public long getErrorCount();
	
	
	/**
	 * Resets the binder metrics
	 */
	public void resetMetrics();

	/**
	 * Returns the most recent execution time in ns.
	 * @return the most recent execution time in ns.
	 */
	public long getLastExecTime();

	/**
	 * Returns the minimum execution time in the metric window in ns.
	 * @return the minimum execution time in the metric window in ns.
	 */
	public long getMinExecTime();

	/**
	 * Returns the maximum execution time in the metric window in ns.
	 * @return the maximum execution time in the metric window in ns.
	 */
	public long getMaxExecTime();

	/**
	 * Returns the elapsed time that the specified percentage of executions fall within in ns.
	 * @param p The percentile to report (between 1 and 99 inclusive)
	 * @return the execution time percentile
	 */
	public long ptileExecTime(final int p);
	
	/**
	 * Returns the total execution time in the metric window in ns.
	 * @return the total execution time in the metric window in ns.
	 */
	public long getTotalExecTime();

	/**
	 * Returns the average execution time in the metric window in ns.
	 * @return the average execution time in the metric window in ns.
	 */
	public long getAvgExecTime();
	
	/**
	 * Returns the average batch execution time in the metric window in ns.
	 * @return the average batch execution time in the metric window in ns.
	 */
	public long getAvgBatchExecTime();
	
	/**
	 * Indicates if this statement should return auto gen keys
	 * @return true if this statement should return auto gen keys, false otherwise
	 */
	public boolean isAutoKeys();
	
}
