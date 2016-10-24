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
package com.heliosapm.streams.sqlbinder.sequence;

import javax.sql.DataSource;


/**
 * <p>Title: PostgresLocalSequenceCache</p>
 * <p>Description: LocalSequenceCache impl for PostgreSQL</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.sqlbinder.sequence.PostgresLocalSequenceCache</code></p>
 */

public class PostgresLocalSequenceCache extends LocalSequenceCache {

	/**
	 * Creates a new PostgresLocalSequenceCache
	 * @param increment The local sequence increment
	 * @param sequenceName The DB Sequence name, fully qualified if necessary
	 * @param dataSource The datasource to provide connections to refresh the sequence cache
	 */
	public PostgresLocalSequenceCache(final int increment, final String sequenceName, final DataSource dataSource) {
		super(increment, sequenceName, dataSource);
	}
	
	/**
	 * Initializes the SQL statement
	 */
	@Override
	protected void init() {
		seqSql = "SELECT nextval('" + sequenceName + "')";
	}

}
