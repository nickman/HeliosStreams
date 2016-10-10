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
package test.com.heliosapm.streams.dbtests;

import java.sql.ResultSet;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.heliosapm.streams.sqlbinder.SQLWorker.ResultSetHandler;
import com.heliosapm.streams.tsdb.listener.DefaultDataSource;

/**
 * <p>Title: DefaultDBTest</p>
 * <p>Description: Basic DB tests against the default data source</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>test.com.heliosapm.streams.dbtests.DefaultDBTest</code></p>
 */

public class DefaultDBTest extends BaseTest {
	
	protected static DefaultDataSource dds = null;
	protected static JDBCHelper jdbcHelper = null;
	
	/**
	 * Starts the default data source
	 */
	@BeforeClass
	public static void startDataSource() {
		dds = DefaultDataSource.getInstance();
		jdbcHelper = new JDBCHelper(dds.getDataSource());
	}
	
	/**
	 * Stops the default data source
	 */
	@AfterClass
	public static void stopDataSource() {
		if(dds!=null) {
			try { dds.close(); } catch (Exception x) {/* No Op */}
		}
	}
	
	
	@Test
	public void testSQLWorkerCreate() throws Exception {
		int rc = ((Number)jdbcHelper.query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES")[0][0]).intValue();
		int rowCount = dds.getSQLWorker().sqlForInt("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES");
		Assert.assertEquals(rc, rowCount);
		int[] countedRows = new int[]{0};
		dds.getSQLWorker().executeQuery("SELECT * FROM INFORMATION_SCHEMA.TABLES", new ResultSetHandler(){
			@Override
			public boolean onRow(int rowId, int columnCount, ResultSet rset) {
				countedRows[0]++;
				return true;
			}
		});
		Assert.assertEquals(rc, countedRows[0]);
	}
	
	
/*
All DataTypes Used:
===================
VARCHAR-1
INTEGER-1
BIGINT-1
INTEGER-0
DOUBLE-1
TINYINT-0
BIGINT-0
TIMESTAMP-0
TIMESTAMP-1
SMALLINT-1
BOOLEAN-1
CHAR-0
VARCHAR-0	
 */
	
}
