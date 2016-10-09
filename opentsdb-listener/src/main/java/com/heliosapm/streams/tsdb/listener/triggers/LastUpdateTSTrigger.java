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
package com.heliosapm.streams.tsdb.listener.triggers;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.heliosapm.utils.time.SystemClock;

/**
 * <p>Title: LastUpdateTSTrigger</p>
 * <p>Description: Trigger to update the <b><code>LAST_UPDATE</code></b> timestamp on update statements.</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tsdb.listener.triggers.LastUpdateTSTrigger</code></p>
 */

public class LastUpdateTSTrigger extends AbstractTrigger {
	/** The JDBC index of the <b><code>LAST_UPDATE</code></b> timestamp column */
	private int tsColumnId = -1;
	/** The JDBC index of the <b><code>VERSION</code></b> number column */
	private int vColumnId = -1;
	
	/**
	 * {@inheritDoc}
	 * @see org.h2.api.Trigger#fire(java.sql.Connection, java.lang.Object[], java.lang.Object[])
	 */
	@Override
	public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
		newRow[tsColumnId] = SystemClock.getTimestamp();
		newRow[vColumnId] = ((Integer)oldRow[vColumnId])+1; 
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.sqlcat.impls.h2.triggers.AbstractTrigger#init(java.sql.Connection, java.lang.String, java.lang.String, java.lang.String, boolean, int)
	 */
	@Override
	public void init(Connection conn, String schemaName, String triggerName, String tableName, boolean before, int type) throws SQLException {
		super.init(conn, schemaName, triggerName, tableName, before, type);
		ResultSet rset = conn.getMetaData().getColumns(null, schemaName, tableName, "LAST_UPDATE");
		rset.next();
		tsColumnId = rset.getInt(17)-1;
		rset.close();
		rset = conn.getMetaData().getColumns(null, schemaName, tableName, "VERSION");
		rset.next();
		vColumnId = rset.getInt(17)-1;		
		this.log.debug("\n\t@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n\tV ID: {}, LAST_UPDATE ID: {} for table {}\n\t@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n", vColumnId, tsColumnId, tableName);
	}
}
