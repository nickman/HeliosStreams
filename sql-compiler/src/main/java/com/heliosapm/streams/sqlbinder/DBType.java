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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.heliosapm.utils.lang.StringHelper;

/**
 * <p>Title: DBType</p>
 * <p>Description: Functional enum to handle slight differences between supported databases.</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tsdb.listener.DBType</code></p>
 */

public enum DBType {
	POSTGRES("PostgreSQL", "com.impossibl.postgres.jdbc.PGDataSource", "jdbc:postgresql:*", "org.postgresql.ds.PGSimpleDataSource", "org.postgresql.Driver", "jdbc:pgsql:*", "com.impossibl.postgres.jdbc.PGDriver", "com.impossibl.postgres.jdbc.PGConnectionPoolDataSource"),
	ORACLE("Oracle", "oracle.jdbc.OracleDriver", "jdbc:oracle:*", "oracle.jdbc.pool.OracleDataSource"),
	H2("H2", "org.h2.jdbcx.JdbcDataSource", "org.h2.Driver", "jdbc:h2:*");
	
	private DBType(final String dbTypeName, final String...matchers) {
		this.dbTypeName = dbTypeName;
		final Set<String> patterns = new HashSet<String>(matchers.length);
		for(int i = 0; i < matchers.length; i++) {
			if(matchers[i]==null || matchers[i].trim().isEmpty()) continue;			
			patterns.add(matchers[i].trim().toLowerCase());
		}
		matchPatterns = patterns.toArray(new String[patterns.size()]);
	}
	
	/** The db type name */
	public final String dbTypeName;
	/** Patterns to match against the JDBC data source class name, driver class name or URL */
	private final String[] matchPatterns;
	
	private static final DBType[] values = values();
	
	/**
	 * Attempts to match the passed JDBC data source class name, driver class name or URL to a DBType
	 * @param targets The JDBC data source class name, driver class name and/or JDBC URL
	 * @return the DB matched against
	 */
	public static DBType getMatchingDBType(final String...targets) {
		for(String target: targets) {
			final String _target = target.trim().toLowerCase();
			for(DBType dbt: values) {
				for(String pattern : dbt.matchPatterns) {
					if(StringHelper.wildmatch(_target, pattern)) return dbt;
				}
			}
		}
		throw new IllegalArgumentException("Failed to find DBType match for any of [" + Arrays.toString(targets) + "]");
	}
	
	/**
	 * Attempts to match the passed JDBC data source class name, driver class name or URL to a DBType
	 * @param targets The JDBC data source class name, driver class name and/or JDBC URL
	 * @return the DB matched against
	 */
	public static DBType getMatchingDBType(final Collection<String> targets) {
		return getMatchingDBType(targets.toArray(new String[0]));
	}

}
