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
package test.com.heliosapm.tsdbex.sqlbinder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;
/**
 * <p>Title: JDBCHelper</p>
 * <p>Description: JDBC helper utility class</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>test.com.heliosapm.tsdbex.sqlbinder.JDBCHelper</code></p>
 */
public class JDBCHelper {
	private final DataSource ds;
	
	
	public void killDb() {
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = ds.getConnection();
			ps = conn.prepareStatement("DROP ALL OBJECTS");			
			ps.executeUpdate();
			if(!conn.getAutoCommit()) {
				conn.commit();
			}
		} catch (Exception e) {
			System.err.println("Drop Schema [TESTDB] Failed:" + e);
		} finally {
			try { ps.close(); } catch (Exception e) {}
			try { conn.close(); } catch (Exception e) {}
		}

	}

	/**
	 * Creates a new JDBCHelper
	 * @param ds The underlyng data source
	 */
	public JDBCHelper(DataSource ds) {
		super();
		this.ds = ds;
	}
	
	
	/**
	 * Executes the SQL script in the passed file.
	 * @param fileName The sql script to run
	 */
	public void runSql(String fileName) {
		executeUpdate("RUNSCRIPT FROM '" + fileName + "'");
	}
	
	/**
	 * Executes the SQL script read from the named classpath resource
	 * @param resourceNames The names of classpath resources containing a SQL script
	 */
	public void runSqlFromResource(String...resourceNames) {
		for(String resourceName : resourceNames) {
			if(resourceName==null || resourceName.trim().isEmpty()) continue;
			InputStream is = null;
			FileOutputStream fos = null;
			File f = null;
			try {
				is = JDBCHelper.class.getClassLoader().getResourceAsStream(resourceName);
				f = File.createTempFile("tmp", ".sql");
				fos = new FileOutputStream(f);
				byte[] buff = new byte[8096];
				int bytesRead = -1;
				while((bytesRead = is.read(buff))!=-1) {
					fos.write(buff, 0, bytesRead);
				}
				is.close();
				is = null;
				fos.flush();
				fos.close();
				fos = null;
				runSql(f.getAbsolutePath());
			} catch (Exception ex) {
				throw new RuntimeException("Failed to execute SQL Script from resource [" + resourceName + "]", ex);
			} finally {
				if(is!=null) try { is.close(); } catch (Exception e) {}
				if(fos!=null) try { fos.close(); } catch (Exception e) {}
				if(f!=null) f.delete();
			}
		}
	}
	
	/**
	 * Executes the passed SQL as an update and returns the result code
	 * @param sql The update SQL
	 * @return the result code
	 */
	public int executeUpdate(CharSequence sql) {
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = ds.getConnection();
			ps = conn.prepareStatement(sql.toString());
			int x = ps.executeUpdate();
			if(!conn.getAutoCommit()) {
				conn.commit();
			}
			return x;
		} catch (Exception e) {
			throw new RuntimeException("Update for [" + sql + "] failed", e);
		} finally {
			try { ps.close(); } catch (Exception e) {}
			try { conn.close(); } catch (Exception e) {}
		}
	}
	
	/**
	 * Creates a new JDBCHelper and returns it.
	 * @param ds The data source to use
	 * @param initSql Optional init SQL. Ignored if null
	 * @return the created JDBCHelper
	 */
	public static JDBCHelper getInstance(DataSource ds, String initSql) {
		JDBCHelper helper = new JDBCHelper(ds);
		if(initSql!=null) {
			helper.executeUpdate(initSql);
		}
		return helper;
	}
	
	/**
	 * Creates a new JDBCHelper and returns it.
	 * @param ds The data source to use
	 * @return the created JDBCHelper
	 */
	public static JDBCHelper getInstance(DataSource ds) {
		return getInstance(ds, null);
	}
	
	
	
	
	/** Cache of SQL statement parameter types */
	protected static final Map<String, int[]> TYPE_CACHE = new ConcurrentHashMap<String, int[]>();
	
	
	/**
	 * @param sql
	 * @return
	 */
	public int queryForInt(CharSequence sql) {
		Object[][] result = query(sql);
		if(result.length==1 && result[0].length==1) {
			return ((Number)result[0][0]).intValue();
		} else {
			throw new RuntimeException("Query did not return 1 row and 1 column");
		}
	}
	
	/**
	 * Executes the passed SQL and returns the resulting rows maps of values keyed by column name within a map keyed by rownumber (starting with zero)  
	 * @param sql The SQL to execute
	 * @return the results
	 */
	public Map<Integer, Map<String, Object>> result(CharSequence sql) {
		Map<Integer, Map<String, Object>> results = new TreeMap<Integer, Map<String, Object>>();
		Map<Integer, String> colNumToName;
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rset = null;
		try {
			conn = ds.getConnection();
			ps = conn.prepareStatement(sql.toString());
			rset = ps.executeQuery();
			int colCount = rset.getMetaData().getColumnCount();
			colNumToName  = new HashMap<Integer, String>(colCount);
			ResultSetMetaData rsmd = rset.getMetaData();
			for(int i = 1; i <= colCount; i++) {
				colNumToName.put(i, rsmd.getColumnLabel(i));
			}
			int rowNum = 0;
			while(rset.next()) {
				Map<String, Object> row = new HashMap<String, Object>(colCount);
				results.put(rowNum, row);
				for(int i = 1; i <= colCount; i++) {
					row.put(colNumToName.get(i), rset.getObject(i));
				}
				rowNum++;
			}
			return results;
		} catch (Exception e) {
			throw new RuntimeException("Query for [" + sql + "] failed", e);
		} finally {
			try { rset.close(); } catch (Exception e) {}
			try { ps.close(); } catch (Exception e) {}
			try { conn.close(); } catch (Exception e) {}
		}
	}
	
	/**
	 * Executes the passed SQL and returns the results in a 2D object array
	 * @param sql The SQL query to executer
	 * @return the results of the query
	 */
	public Object[][] query(CharSequence sql) {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rset = null;
		
		Vector<Object[]> rows = new Vector<Object[]>();
		try {
			conn = ds.getConnection();
			ps = conn.prepareStatement(sql.toString());
			rset = ps.executeQuery();
			int colCount = rset.getMetaData().getColumnCount();
			while(rset.next()) {
				Object[] row = new Object[colCount];
				for(int i = 1; i <= colCount; i++) {
					row[i-1] = rset.getObject(i);
				}
				rows.add(row);
			}
			Object[][] result = new Object[rows.size()][];
			int cnt = 0;
			for(Object[] row: rows) {
				result[cnt] = row;
				cnt++;
			}
			return result;
		} catch (Exception e) {
			throw new RuntimeException("Query for [" + sql + "] failed", e);
		} finally {
			try { rset.close(); } catch (Exception e) {}
			try { ps.close(); } catch (Exception e) {}
			try { conn.close(); } catch (Exception e) {}
		}
		
	}
}
