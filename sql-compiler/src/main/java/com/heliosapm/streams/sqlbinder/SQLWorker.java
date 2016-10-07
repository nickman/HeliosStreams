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

import java.io.File;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.tuples.NVP;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.CtNewConstructor;
import javassist.CtNewMethod;
import javassist.LoaderClassPath;

/**
 * <p>Title: SQLWorker</p>
 * <p>Description: A functional wrapper class for JDBC operations to handle all the messy stuff.
 * No magic here. Just raw, low level overloading.</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.sqlbinder.SQLWorker</code></p>
 */

public class SQLWorker {
	/** The data source this SQLWorker will use */
	protected final DataSource dataSource;
	/** The binder factory for the data source */
	protected final BinderFactory binderFactory;
	/** The JDBC URL of the data source's database */
	protected final String dbUrl;
	
	/** The size of the sliding window tracking SQLWorker execution times */
	protected final int windowSize;
	
	/** Map of binders keyed by a prepared statement that started a batch op */
	private final Map<PreparedStatement, PreparedStatementBinder> psToBinders = Collections.synchronizedMap(new WeakHashMap<PreparedStatement, PreparedStatementBinder>(128, 0.75f));

	
	/** Static class Logger */
	protected static final Logger log = LogManager.getLogger(SQLWorker.class);
	
	/** A map of SQLWorkers keyed by their data source */
	protected static final Map<String, SQLWorker> workers = new ConcurrentHashMap<String, SQLWorker>();

	/** A class array of a ResultSet */
	protected static final Class<?>[] RSET_IFACE = {ResultSet.class};
	
	/**
	 * Acquires a SQLWorker for the passed DataSource
	 * @param dataSource The data source this SQLWorker will use
	 * @return The acquired SQLWorker
	 */
	public static SQLWorker getInstance(DataSource dataSource) {
		Connection conn = null;
		String url = null;
		try {
			conn = dataSource.getConnection();
			url = conn.getMetaData().getURL();
			SQLWorker worker = workers.get(url);
			if(worker==null) {
				synchronized(workers) {
					worker = workers.get(url);
					if(worker==null) {
						worker = new SQLWorker(url, dataSource);
						workers.put(url, worker);
						log.info("\n\t=======================================\n\tCreated SQLWorker for [{}]\n\t=======================================\n", url);
					}
				}
			}
			return worker;
		} catch (Exception ex) {
			throw new RuntimeException("Failed to get connection from datasource [" + dataSource + "]", ex);
		} finally {
			if(conn!=null) try { conn.close(); } catch (Exception x) { /* No Op */ }
		}
	}

	/**
	 * Creates a new SQLWorker
	 * @param dbUrl The URL of the DB
	 * @param dataSource The data source this SQLWorker will use
	 */
	private SQLWorker(String dbUrl, DataSource dataSource) {
		this.dataSource = dataSource;
		this.dbUrl = dbUrl;
		windowSize = ConfigurationHelper.getIntSystemThenEnvProperty("com.heliosapm.sqlworker.windowsize", 100);
		binderFactory = new BinderFactory(this.dataSource, windowSize);
		StdInCommandHandler.getInstance().registerCommand("sqlworkers", new Runnable() {
			@Override
			public void run() {
				StringBuilder b = new StringBuilder("\n\tSQLWorkers Status & Metrics\n\t=========================================\n");
				b.append(printStats());
				System.out.println(b.toString());
			}
		});
	}
	
	/**
	 * Returns the number of binders held in state, bound to a transient PreparedStatement
	 * @return the number of binders held in state, bound to a transient PreparedStatement
	 */
	public int getPsToBinderCount() {
		return psToBinders.size();
	}
	
	/**
	 * <p>Title: ResultSetHandler</p>
	 * <p>Description: Defines an object that can be passed into a SQLWorker query and handle the result row.</p> 
	 * <p>Company: Helios Development Group LLC</p>
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>net.opentsdb.catalog.SQLWorker.ResultSetHandler</code></p>
	 */
	public static interface ResultSetHandler {
		/**
		 * Callback on the next row. Implementations should not call {@link ResultSet#next()} unless 
		 * it is intended to skip rows.
		 * @param rowId The row sequence id, starting at zero.
		 * @param rset The result set at the next logical row
		 * @return true to contine processing, false otherwise
		 */
		public boolean onRow(int rowId, ResultSet rset);
	}
	
	/**
	 * Creates a ResultSet proxy that will close the parent statement and connection when the result set is closed.
	 * @param rset The resultset to proxy
	 * @param st The parent statement
	 * @param conn The parent connection
	 * @return the resultset proxy
	 */
	protected static ResultSet getAutoCloseResultSet(final ResultSet rset, final Statement st, final Connection conn) {
		return (ResultSet) Proxy.newProxyInstance(st.getClass().getClassLoader(), RSET_IFACE, new InvocationHandler() {
			@Override
			public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
				try {
					return method.invoke(rset, args);
				} finally {
					if("close".equals(method.getName())) {
						try { st.close(); } catch (Exception x) { /* No Op */ }
						if(conn!=null) try { conn.close(); } catch (Exception x) { /* No Op */ }
					}
				}
			}
		});
	}
	
	/**
	 * Executes the query bound in the passed prepared statement, recording the elapsed time or error in the passed binder
	 * @param ps The prepared statement to execute
	 * @param binder The binder to record in
	 * @return the result set returned from the prepared statement execution
	 * @throws Exception thrown on any error
	 */
	private ResultSet executeQuery(final PreparedStatement ps, final PreparedStatementBinder binder) throws Exception {
		final long startTime = System.nanoTime();
		try {
			final ResultSet rset = ps.executeQuery();
			binder.recordExecTime(System.nanoTime() - startTime);
			return rset;
		} catch (Exception ex) {
			binder.recordError();
			throw ex;
		}
	}
	
	public long nextSeq(final String seqName) {
		return sqlForLong("SELECT " + seqName + ".NEXTVAL FROM DUAL");
	}
	
	/**
	 * Executes the update bound in the passed prepared statement, recording the elapsed time or error in the passed binder
	 * @param ps The prepared statement to execute
	 * @param binder The binder to record in
	 * @return the result code returned from the prepared statement update
	 * @throws Exception thrown on any error
	 */
	private int executeUpdate(final PreparedStatement ps, final PreparedStatementBinder binder) throws Exception {
		final long startTime = System.nanoTime();
		try {
			final int resultCode = ps.executeUpdate();
			binder.recordExecTime(System.nanoTime() - startTime);
			return resultCode;
		} catch (Exception ex) {
			binder.recordError();
			throw ex;
		}
	}
	
	/**
	 * Executes the statement bound in the passed prepared statement, recording the elapsed time or error in the passed binder
	 * @param ps The prepared statement to execute
	 * @param binder The binder to record in
	 * @throws Exception thrown on any error
	 */
	private void execute(final PreparedStatement ps, final PreparedStatementBinder binder) throws Exception {
		final long startTime = System.nanoTime();
		try {
			ps.execute();
			if(psToBinders.remove(ps)!=null) {
				binder.recordBatchExecTime(System.nanoTime() - startTime);
			} else {
				binder.recordExecTime(System.nanoTime() - startTime);
			}
		} catch (Exception ex) {
			binder.recordError();
			throw ex;
		}
	}
	
	
	
	/**
	 * Executes the passed query and returns the actual connected result set, 
	 * meaning the caller should close the statemnt and connection via {@link #close(ResultSet)}.
	 * @param sqlText The SQL query
	 * @param args The query bind arguments
	 * @return A result set for the query
	 */	
	public ResultSet executeRawQuery(String sqlText, Object...args) {
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			final AbstractPreparedStatementBinder binder = binderFactory.getBinder(sqlText);
			conn = dataSource.getConnection();
			ps = conn.prepareStatement(sqlText);
			binder.bind(ps, args);
			return executeQuery(ps, binder);
		} catch (Exception ex) {
			throw new RuntimeException("SQL Query Failure [" + sqlText + "]", ex);
		}				
	}
	
	/**
	 * Closes the passed resultset and the associated statement and connection
	 * @param rset The result set to close
	 */
	public void close(ResultSet rset) {
		try {
			Statement st = rset.getStatement();
			Connection conn = st.getConnection();
			try { rset.close(); } catch (Exception x) { /* No Op */ }
			try { st.close(); } catch (Exception x) { /* No Op */ }
			try { conn.close(); } catch (Exception x) { /* No Op */ }				
			
		} catch (Exception ex) {
			throw new RuntimeException("Failed to close result set resources", ex);
		}
	}
	
	/**
	 * Executes the passed query and returns a result set
	 * @param conn An optional connection. If not supplied, a new connection will be acquired, and closed when used.
	 * @param sqlText The SQL query
	 * @param disconnected true to read all rows and return a disconnected resultset, false to return the connected result set
	 * @param args The query bind arguments
	 * @return A result set for the query
	 */
	public ResultSet executeQuery(final Connection conn, final String sqlText, final boolean disconnected, final Object...args) {
		return executeQuery(conn, sqlText, 0, disconnected, args);
	}
	
	
	/**
	 * Executes the passed query and returns a result set
	 * @param conn An optional connection. If not supplied, a new connection will be acquired, and closed when used.
	 * @param sqlText The SQL query
	 * @param fetchSize The prepared statement fetch size hint
	 * @param disconnected true to read all rows and return a disconnected resultset, false to return the connected result set
	 * @param args The query bind arguments
	 * @return A result set for the query
	 */
	public ResultSet executeQuery(Connection conn, final String sqlText, final int fetchSize, final boolean disconnected, final Object...args) {		
		PreparedStatement ps = null;
		ResultSet rset = null;
		final boolean newConn = conn==null;
		
		try {
			final AbstractPreparedStatementBinder binder = binderFactory.getBinder(sqlText);
			if(newConn) {
				conn = dataSource.getConnection();
			}
			ps = conn.prepareStatement(sqlText);	
			ps.setFetchSize(fetchSize);
			binder.bind(ps, args);
			
			rset = executeQuery(ps, binder);
			if(disconnected) {
				TSDBCachedRowSetImpl crs = new TSDBCachedRowSetImpl();
				crs.populate(rset);
				return crs;
			}
			return getAutoCloseResultSet(rset, ps, newConn ? conn : null);
		} catch (Exception ex) {
			throw new RuntimeException("SQL Query Failure [" + sqlText + "]", ex);
		} finally {
			if(disconnected) {				
				if(rset!=null) try { rset.close(); } catch (Exception x) { /* No Op */ }
				if(ps!=null) try { ps.close(); } catch (Exception x) { /* No Op */ }
				if(newConn && conn!=null) try { conn.close(); } catch (Exception x) { /* No Op */ }				
			} 
		}		
	}
	
	/**
	 * Executes a query with a {@link ResultSetHandler} that handles the returned rows.
	 * @param conn An optional connection. If not supplied, a new connection will be acquired, and closed when used.
	 * @param sqlText The SQL query
	 * @param rowHandler The row handler to handle the rows returned
	 * @param args The bind variables
	 * @return the number of rows retrieved
	 */
	public int executeQuery(Connection conn, String sqlText, ResultSetHandler rowHandler, Object...args) {
		PreparedStatement ps = null;
		ResultSet rset = null;
		final boolean newConn = conn==null;
		try {
			final AbstractPreparedStatementBinder binder = binderFactory.getBinder(sqlText);
			if(newConn) {
				conn = dataSource.getConnection();
			}
			ps = conn.prepareStatement(sqlText);
			binder.bind(ps, args);
			int rowId = 0;
			rset = executeQuery(ps, binder);
			while(rset.next()) {
				if(!rowHandler.onRow(rowId, rset)) break;
			}
			return rowId+1;
		} catch (Exception ex) {
			throw new RuntimeException("SQL Query Failure [" + sqlText + "]", ex);
		} finally {
			if(rset!=null) try { rset.close(); } catch (Exception x) { /* No Op */ }
			if(ps!=null) try { ps.close(); } catch (Exception x) { /* No Op */ }
			if(newConn && conn!=null) try { conn.close(); } catch (Exception x) { /* No Op */ }				
		}		
	}
	
	/**
	 * Executes a query using a new connection with a {@link ResultSetHandler} that handles the returned rows.
	 * @param sqlText The SQL query
	 * @param rowHandler The row handler to handle the rows returned
	 * @param args The bind variables
	 * @return the number of rows retrieved
	 */
	public int executeQuery(String sqlText, ResultSetHandler rowHandler, Object...args) {
		return executeQuery(null, sqlText, rowHandler, args);
	}
	
	/**
	 * Executes the passed query and returns a result set
	 * @param sqlText The SQL query
	 * @param disconnected true to read all rows and return a disconnected resultset, false to return the connected result set
	 * @param args The query bind arguments
	 * @return A result set for the query
	 */
	public ResultSet executeQuery(final String sqlText, final boolean disconnected, final Object...args) {
		return executeQuery(sqlText, 0, disconnected, args);
	}	
	
	
	/**
	 * Executes the passed query and returns a result set
	 * @param sqlText The SQL query
	 * @param fetchSize The prepared statement fetch size hint
	 * @param disconnected true to read all rows and return a disconnected resultset, false to return the connected result set
	 * @param args The query bind arguments
	 * @return A result set for the query
	 */
	public ResultSet executeQuery(String sqlText, final int fetchSize, boolean disconnected, Object...args) {
		return executeQuery(null, sqlText, fetchSize, disconnected, args);
	}
	
	/**
	 * Executes the passed update statement
	 * @param conn An optional connection. If not supplied, a new connection will be acquired, and closed when used.
	 * @param sqlText The update SQL text
	 * @param args The bind arguments
	 * @return either (1) the row count for SQL Data Manipulation Language (DML) statements or (2) 0 for SQL statements that return nothing 
	 */
	public int executeUpdate(Connection conn, String sqlText, Object...args) {
		boolean newConn = conn==null;
		PreparedStatement ps = null;
		try {
			final AbstractPreparedStatementBinder binder = binderFactory.getBinder(sqlText);
			if(newConn) {
				conn = dataSource.getConnection();
			}
			ps = conn.prepareStatement(sqlText);
			binder.bind(ps, args);
			int updateResult = executeUpdate(ps, binder);
			if(newConn) conn.commit();
			return updateResult;
		} catch (Exception ex) {
			throw new RuntimeException("SQL Update Failure [" + sqlText + "]", ex);
		} finally {
			if(ps!=null) try { ps.close(); } catch (Exception x) { /* No Op */ }
			if(newConn && conn!=null) try { conn.close(); } catch (Exception x) { /* No Op */ }
		}		
	}

	/**
	 * Executes the passed update statement
	 * @param sqlText The update SQL text
	 * @param args The bind arguments
	 * @return either (1) the row count for SQL Data Manipulation Language (DML) statements or (2) 0 for SQL statements that return nothing 
	 */
	public int executeUpdate(String sqlText, Object...args) {
		return executeUpdate(null, sqlText, args);
	}
	
	/**
	 * Executes the passed statement
	 * @param conn An optional connection. If not supplied, a new connection will be acquired, and closed when used.
	 * @param sqlText The update SQL text
	 * @param args The bind arguments
	 */
	public void execute(Connection conn, String sqlText, Object...args) {		
		PreparedStatement ps = null;
		boolean newConn = conn==null;		
		try {
			final AbstractPreparedStatementBinder binder = binderFactory.getBinder(sqlText);
			if(newConn) {
				conn = dataSource.getConnection();
			}
			ps = conn.prepareStatement(sqlText);
			binder.bind(ps, args);
			execute(ps, binder);
			if(newConn) conn.commit();
		} catch (Exception ex) {
			throw new RuntimeException("SQL Update Failure [" + sqlText + "]", ex);
		} finally {
			if(ps!=null) try { ps.close(); } catch (Exception x) { /* No Op */ }
			if(newConn && conn!=null) try { conn.close(); } catch (Exception x) { /* No Op */ }
		}		
	}
	
	/**
	 * Executes the passed statement
	 * @param sqlText The update SQL text
	 * @param args The bind arguments
	 */
	public void execute(String sqlText, Object...args) {
		execute(null, sqlText, args);
	}
	
	/**
	 * Returns a formated string displaying the status of all binders where the SQL of the binder matches the passed expression.
	 * If the passed expression is null, all binders are displayed.
	 * @param filterExpression An optional regex expression
	 * @return the formated stats
	 */
	public String printStats(final String filterExpression) {
		final StringBuilder b = new StringBuilder(4096);
		final Pattern p = filterExpression==null ? null : Pattern.compile(filterExpression, Pattern.CASE_INSENSITIVE);
		for(Map.Entry<String, AbstractPreparedStatementBinder> entry: binderFactory.binders.entrySet()) {
			if(p==null || p.matcher(entry.getKey()).matches()) {
				b.append(entry.getValue()).append("\n");
			}
		}
		return b.toString();
	}
	
	/**
	 * Returns a formated string displaying the status of all binders
	 * @return the formated stats
	 */
	public String printStats() {
		return printStats(null);
	}
	
	

	/**
	 * Adds a batch to a prepared statement
	 * @param conn The current connection
	 * @param ps The prepared statement. If null, it will be prepared
	 * @param sqlText The SQL batch statement
	 * @param args The bind arguments
	 * @return the batched prepared statement
	 */
	public PreparedStatement batch(Connection conn, PreparedStatement ps, String sqlText, Object...args) {
		try {
			final AbstractPreparedStatementBinder binder = binderFactory.getBinder(sqlText);
			if(ps==null) {
				ps = conn.prepareStatement(sqlText);
				psToBinders.put(ps, binder);
			}
			binder.bind(ps, args);
			ps.addBatch();
			return ps;
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			throw new RuntimeException("SQL Batch Failure [" + sqlText + "]", ex);
		}				
	}
	
	/**
	 * Removes and returns the binder for the passed prepared statement
	 * @param ps The prepared statement to get the binder for
	 * @return the binder or null if one was not found
	 */
	public PreparedStatementBinder getBinder(final PreparedStatement ps) {
		return psToBinders.remove(ps);
	}

	/**
	 * Executes the passed query statement and returns the first column of the first row as a boolean
	 * @param conn An optional connection. If not supplied, a new connection will be acquired, and closed when used.
	 * @param sqlText The query SQL text
	 * @param args The bind arguments
	 * @return true if the first int from the first column of the first row is > 0, otherwise false
	 */
	public boolean sqlForBool(Connection conn, String sqlText, Object...args) {
		int x = sqlForInt(conn, sqlText, args);
		log.debug("sqlForBool [{}] --> [{}]", sqlText, x>0);
		return x > 0;
	}

	/**
	 * Executes the passed query statement and returns the first column of the first row as a boolean
	 * @param sqlText The query SQL text
	 * @param args The bind arguments
	 * @return true if the first int from the first column of the first row is > 0, otherwise false
	 */
	public boolean sqlForBool(String sqlText, Object...args) {
		return sqlForBool(null, sqlText, args);
	}
	
	
	/**
	 * Executes the passed query statement and returns the first column of the first row as an int
	 * Will throw a runtime exception if no rows are returned.
	 * @param sqlText The query SQL text
	 * @param args The bind argumentsclazz
	 * @return The int from the first column of the first row
	 */
	public int sqlForInt(String sqlText, Object...args) {
		return sqlForInt(null, sqlText, args);
	}
	
	/**
	 * Executes the passed query statement and returns the first column of the first row as an int
	 * @param sqlText The query SQL text
	 * @param defaultValue The default value to return if no rows are returned
	 * @param args The bind argumentsclazz
	 * @return The int from the first column of the first row
	 */
	public int sqlForInt(String sqlText, final Integer defaultValue, Object...args) {
		return sqlForInt(null, sqlText, defaultValue, args);
	}
	
	
	/**
	 * Executes the passed query statement and returns the first column of the first row as a string
	 * @param conn An optional connection. If not supplied, a new connection will be acquired, and closed when used.
	 * @param sqlText The query SQL text
	 * @param args The bind arguments
	 * @return The string from the first column of the first row
	 */
	public String sqlForString(Connection conn, String sqlText, Object...args) {		
		PreparedStatement ps = null;
		ResultSet rset = null;
		boolean newConn = conn==null;
		try {
			final AbstractPreparedStatementBinder binder = binderFactory.getBinder(sqlText);
			if(newConn) {
				conn = dataSource.getConnection();
			}
			ps = conn.prepareStatement(sqlText);
			binder.bind(ps, args);
			rset = executeQuery(ps, binder);
			if(!rset.next()) return null;
			return rset.getString(1);
		} catch (Exception ex) {
			throw new RuntimeException("SQL Query Failure [" + sqlText + "]", ex);
		} finally {
			if(rset!=null) try { rset.close(); } catch (Exception x) { /* No Op */ }
			if(ps!=null) try { ps.close(); } catch (Exception x) { /* No Op */ }
			if(newConn && conn!=null) try { conn.close(); } catch (Exception x) { /* No Op */ }
		}
	}
	
	/**
	 * Executes the passed query statement and returns the first column of the first row as a string
	 * @param sqlText The query SQL text
	 * @param args The bind arguments
	 * @return The string from the first column of the first row
	 */
	public String sqlForString(String sqlText, Object...args) {		
		return sqlForString(null, sqlText, args);
	}
	
	/**
	 * Executes the passed query statement and returns the first column of the first row as an int.
	 * Will throw a runtime exception if no rows are returned.
	 * @param conn An optional connection. If not supplied, a new connection will be acquired, and closed when used.
	 * @param sqlText The query SQL text
	 * @param args The bind arguments
	 * @return The int from the first column of the first row
	 */
	public int sqlForInt(Connection conn, final String sqlText, final Object...args) {
		return sqlForInt(conn, sqlText, null, args);
	}
	
	/**
	 * Executes the passed query statement and returns the first column of the first row as an int
	 * @param conn An optional connection. If not supplied, a new connection will be acquired, and closed when used.
	 * @param sqlText The query SQL text
	 * @param defaultValue The default value to return if no rows are found
	 * @param args The bind arguments
	 * @return The int from the first column of the first row
	 */
	public int sqlForInt(Connection conn, final String sqlText, final Integer defaultValue, final Object...args) {		
		PreparedStatement ps = null;
		ResultSet rset = null;
		boolean newConn = conn==null;
		try {
			final AbstractPreparedStatementBinder binder = binderFactory.getBinder(sqlText);
			if(newConn) {
				conn = dataSource.getConnection();			
			}
			ps = conn.prepareStatement(sqlText);
			binder.bind(ps, args);
			rset = executeQuery(ps, binder);
			if(!rset.next()) {
				if(defaultValue==null) throw new RuntimeException("Query [" + sqlText + "] returned no rows but default value was null");
				return defaultValue;
			}
			return rset.getInt(1);
		} catch (Exception ex) {
			throw new RuntimeException("SQL Query Failure [" + sqlText + "]", ex);
		} finally {
			if(rset!=null) try { rset.close(); } catch (Exception x) { /* No Op */ }
			if(ps!=null) try { ps.close(); } catch (Exception x) { /* No Op */ }
			if(newConn && conn!=null) try { conn.close(); } catch (Exception x) { /* No Op */ }
		}
	}
	
	/**
	 * Executes the passed query statement and returns the first column of the first row as a long
	 * @param conn An optional connection. If not supplied, a new connection will be acquired, and closed when used.
	 * @param defaultValue The default value to return if no rows are found. Ignored if null, but if so, 
	 * and no rows are returned, will throw an exception.
	 * @param sqlText The query SQL text
	 * @param args The bind arguments
	 * @return The long from the first column of the first row
	 */
	public long sqlForLong(Connection conn, final Long defaultValue, final String sqlText, final Object...args) {
		PreparedStatement ps = null;
		ResultSet rset = null;
		boolean newConn = conn==null;
		try {
			final AbstractPreparedStatementBinder binder = binderFactory.getBinder(sqlText);
			if(newConn) {
				conn = dataSource.getConnection();			
			}
			ps = conn.prepareStatement(sqlText);
			binder.bind(ps, args);
			rset = executeQuery(ps, binder);
			if(!rset.next()) {
				if(defaultValue!=null) {
					return defaultValue;
				}
			}
			return rset.getLong(1);
		} catch (Exception ex) {
			throw new RuntimeException("SQL Query Failure [" + sqlText + "]", ex);
		} finally {
			if(rset!=null) try { rset.close(); } catch (Exception x) { /* No Op */ }
			if(ps!=null) try { ps.close(); } catch (Exception x) { /* No Op */ }
			if(newConn && conn!=null) try { conn.close(); } catch (Exception x) { /* No Op */ }
		}
	}
	
	
	
	/**
	 * Executes the passed query statement and returns the first column of the first row as a long
	 * @param conn An optional connection. If not supplied, a new connection will be acquired, and closed when used.
	 * @param sqlText The query SQL text
	 * @param args The bind arguments
	 * @return The long from the first column of the first row
	 */
	public long sqlForLong(final Connection conn, final String sqlText, final Object...args) {
		return sqlForLong(conn, null, sqlText, args);
	}
	
	
	/**
	 * Executes the passed query statement and returns the first column of the first row as a long
	 * @param sqlText The query SQL text
	 * @param args The bind arguments
	 * @return The long from the first column of the first row
	 */
	public long sqlForLong(final String sqlText, final Object...args) {
		return sqlForLong(null, null, sqlText, args);
	}
	
	/**
	 * Executes the passed query statement and returns the first column of the first row as a long
	 * @param defaultValue The default value to return if no rows are found. Ignored if null, but if so, 
	 * and no rows are returned, will throw an exception.
	 * @param sqlText The query SQL text
	 * @param args The bind arguments
	 * @return The long from the first column of the first row
	 */
	public long sqlForLong(final Long defaultValue, final String sqlText, final Object...args) {
		return sqlForLong(null, defaultValue, sqlText, args);
	}
	

	/**
	 * Executes the passed query statement and returns the first column of the first row as a double
	 * @param conn An optional connection. If not supplied, a new connection will be acquired, and closed when used.
	 * @param defaultValue The default value to return if no rows are found. Ignored if null, but if so, 
	 * and no rows are returned, will throw an exception.
	 * @param sqlText The query SQL text
	 * @param args The bind arguments
	 * @return The double from the first column of the first row
	 */
	public double sqlForDouble(Connection conn, final Double defaultValue, final String sqlText, final Object...args) {
		PreparedStatement ps = null;
		ResultSet rset = null;
		boolean newConn = conn==null;
		try {
			final AbstractPreparedStatementBinder binder = binderFactory.getBinder(sqlText);
			if(newConn) {
				conn = dataSource.getConnection();			
			}
			ps = conn.prepareStatement(sqlText);
			binder.bind(ps, args);
			rset = executeQuery(ps, binder);
			if(!rset.next()) {
				if(defaultValue!=null) {
					return defaultValue;
				}
			}
			return rset.getDouble(1);
		} catch (Exception ex) {
			throw new RuntimeException("SQL Query Failure [" + sqlText + "]", ex);
		} finally {
			if(rset!=null) try { rset.close(); } catch (Exception x) { /* No Op */ }
			if(ps!=null) try { ps.close(); } catch (Exception x) { /* No Op */ }
			if(newConn && conn!=null) try { conn.close(); } catch (Exception x) { /* No Op */ }
		}
	}
	
	/**
	 * Executes the passed query statement and returns the first column of the first row as a double
	 * @param conn An optional connection. If not supplied, a new connection will be acquired, and closed when used.
	 * @param sqlText The query SQL text
	 * @param args The bind arguments
	 * @return The double from the first column of the first row
	 */
	public double sqlForDouble(final Connection conn, final String sqlText, final Object...args) {
		return sqlForDouble(conn, null, sqlText, args);
	}
	
	
	/**
	 * Executes the passed query statement and returns the first column of the first row as a double
	 * @param sqlText The query SQL text
	 * @param args The bind arguments
	 * @return The double from the first column of the first row
	 */
	public double sqlForDouble(final String sqlText, final Object...args) {
		return sqlForDouble(null, null, sqlText, args);
	}
	
	/**
	 * Executes the passed query statement and returns the first column of the first row as a double
	 * @param defaultValue The default value to return if no rows are found. Ignored if null, but if so, 
	 * and no rows are returned, will throw an exception.
	 * @param sqlText The query SQL text
	 * @param args The bind arguments
	 * @return The double from the first column of the first row
	 */
	public double sqlForDouble(final Double defaultValue, final String sqlText, final Object...args) {
		return sqlForDouble(null, defaultValue, sqlText, args);
	}
	
	
	/**
	 * Executes the passed query statement and returns the first column of the first row as a BigDecimal
	 * @param conn An optional connection. If not supplied, a new connection will be acquired, and closed when used.
	 * @param defaultValue The default value to return if no rows are found. Ignored if null, but if so, 
	 * and no rows are returned, will throw an exception.
	 * @param sqlText The query SQL text
	 * @param args The bind arguments
	 * @return The BigDecimal from the first column of the first row
	 */
	public BigDecimal sqlForBigDecimal(Connection conn, final BigDecimal defaultValue, final String sqlText, final Object...args) {
		PreparedStatement ps = null;
		ResultSet rset = null;
		boolean newConn = conn==null;
		try {
			final AbstractPreparedStatementBinder binder = binderFactory.getBinder(sqlText);
			if(newConn) {
				conn = dataSource.getConnection();			
			}
			ps = conn.prepareStatement(sqlText);
			binder.bind(ps, args);
			rset = executeQuery(ps, binder);
			if(!rset.next()) {
				if(defaultValue!=null) {
					return defaultValue;
				}
			}
			BigDecimal bd = rset.getBigDecimal(1);
			if(bd==null) throw new Exception("Null value returned for sqlForBigDecimal");
			return bd;
		} catch (Exception ex) {
			throw new RuntimeException("SQL Query Failure [" + sqlText + "]", ex);
		} finally {
			if(rset!=null) try { rset.close(); } catch (Exception x) { /* No Op */ }
			if(ps!=null) try { ps.close(); } catch (Exception x) { /* No Op */ }
			if(newConn && conn!=null) try { conn.close(); } catch (Exception x) { /* No Op */ }
		}
	}
	
	/**
	 * Executes the passed query statement and returns the first column of the first row as a BigDecimal
	 * @param conn An optional connection. If not supplied, a new connection will be acquired, and closed when used.
	 * @param sqlText The query SQL text
	 * @param args The bind arguments
	 * @return The BigDecimal from the first column of the first row
	 */
	public BigDecimal sqlForBigDecimal(final Connection conn, final String sqlText, final Object...args) {
		return sqlForBigDecimal(conn, null, sqlText, args);
	}
	
	
	/**
	 * Executes the passed query statement and returns the first column of the first row as a BigDecimal
	 * @param sqlText The query SQL text
	 * @param args The bind arguments
	 * @return The BigDecimal from the first column of the first row
	 */
	public BigDecimal sqlForBigDecimal(final String sqlText, final Object...args) {
		return sqlForBigDecimal(null, null, sqlText, args);
	}
	
	/**
	 * Executes the passed query statement and returns the first column of the first row as a BigDecimal
	 * @param defaultValue The default value to return if no rows are found. Ignored if null, but if so, 
	 * and no rows are returned, will throw an exception.
	 * @param sqlText The query SQL text
	 * @param args The bind arguments
	 * @return The BigDecimal from the first column of the first row
	 */
	public BigDecimal sqlForBigDecimal(final BigDecimal defaultValue, final String sqlText, final Object...args) {
		return sqlForBigDecimal(null, defaultValue, sqlText, args);
	}
	
	/**
	 * Executes the passed query statement and returns the first column of the first row as a RowId
	 * @param conn An optional connection. If not supplied, a new connection will be acquired, and closed when used.
	 * and no rows are returned, will throw an exception.
	 * @param sqlText The query SQL text
	 * @param args The bind arguments
	 * @return The RowId from the first column of the first row
	 */
	public RowId sqlForRowId(Connection conn, final String sqlText, final Object...args) {
		PreparedStatement ps = null;
		ResultSet rset = null;
		boolean newConn = conn==null;
		try {
			final AbstractPreparedStatementBinder binder = binderFactory.getBinder(sqlText);
			if(newConn) {
				conn = dataSource.getConnection();			
			}
			ps = conn.prepareStatement(sqlText);
			binder.bind(ps, args);
			rset = executeQuery(ps, binder);
			if(!rset.next()) {
				throw new Exception("No Rows returned for RowId Query");
			}
			final RowId rowId = rset.getRowId(1);
			if(rowId==null) throw new Exception("Null value returned for sqlForRowId");
			return rowId;
		} catch (Exception ex) {
			throw new RuntimeException("SQL Query Failure [" + sqlText + "]", ex);
		} finally {
			if(rset!=null) try { rset.close(); } catch (Exception x) { /* No Op */ }
			if(ps!=null) try { ps.close(); } catch (Exception x) { /* No Op */ }
			if(newConn && conn!=null) try { conn.close(); } catch (Exception x) { /* No Op */ }
		}
	}
	

	/**
	 * Executes the passed query statement and returns the first column of the first row as a BigInteger
	 * @param conn An optional connection. If not supplied, a new connection will be acquired, and closed when used.
	 * @param defaultValue The default value to return if no rows are found. Ignored if null, but if so, 
	 * and no rows are returned, will throw an exception.
	 * @param sqlText The query SQL text
	 * @param args The bind arguments
	 * @return The BigInteger from the first column of the first row
	 */
	public BigInteger sqlForBigInteger(Connection conn, final BigInteger defaultValue, final String sqlText, final Object...args) {
		PreparedStatement ps = null;
		ResultSet rset = null;
		boolean newConn = conn==null;
		try {
			final AbstractPreparedStatementBinder binder = binderFactory.getBinder(sqlText);
			if(newConn) {
				conn = dataSource.getConnection();			
			}
			ps = conn.prepareStatement(sqlText);
			binder.bind(ps, args);
			rset = executeQuery(ps, binder);
			if(!rset.next()) {
				if(defaultValue!=null) {
					return defaultValue;
				}
			}
			BigDecimal bd = rset.getBigDecimal(1);
			if(bd==null) throw new Exception("Null value returned for sqlForBigInteger");
			return bd.toBigInteger();
		} catch (Exception ex) {
			throw new RuntimeException("SQL Query Failure [" + sqlText + "]", ex);
		} finally {
			if(rset!=null) try { rset.close(); } catch (Exception x) { /* No Op */ }
			if(ps!=null) try { ps.close(); } catch (Exception x) { /* No Op */ }
			if(newConn && conn!=null) try { conn.close(); } catch (Exception x) { /* No Op */ }
		}
	}
	
	/**
	 * Executes the passed query statement and returns the first column of the first row as a BigInteger
	 * @param conn An optional connection. If not supplied, a new connection will be acquired, and closed when used.
	 * @param sqlText The query SQL text
	 * @param args The bind arguments
	 * @return The BigInteger from the first column of the first row
	 */
	public BigInteger sqlForBigInteger(final Connection conn, final String sqlText, final Object...args) {
		return sqlForBigInteger(conn, null, sqlText, args);
	}
	
	
	/**
	 * Executes the passed query statement and returns the first column of the first row as a BigInteger
	 * @param sqlText The query SQL text
	 * @param args The bind arguments
	 * @return The BigInteger from the first column of the first row
	 */
	public BigInteger sqlForBigInteger(final String sqlText, final Object...args) {
		return sqlForBigInteger(null, null, sqlText, args);
	}
	
	/**
	 * Executes the passed query statement and returns the first column of the first row as a BigInteger
	 * @param defaultValue The default value to return if no rows are found. Ignored if null, but if so, 
	 * and no rows are returned, will throw an exception.
	 * @param sqlText The query SQL text
	 * @param args The bind arguments
	 * @return The BigInteger from the first column of the first row
	 */
	public BigInteger sqlForBigInteger(final BigInteger defaultValue, final String sqlText, final Object...args) {
		return sqlForBigInteger(null, defaultValue, sqlText, args);
	}
	
	
//	public static void main(String[] args) {
//		// SELECT * FROM INFORMATION_SCHEMA.TYPE_INFO WHERE AUTO_INCREMENT = ?
//		try {
//			log("BinderTest");
//			log("LF:%s", LoggerFactory.getILoggerFactory().getClass().getName());
//			LoggerContext lc = (LoggerContext)LoggerFactory.getILoggerFactory();
//			lc.getLogger(SQLWorker.class).setLevel(Level.DEBUG);
//			JdbcConnectionPool pool = JdbcConnectionPool.create("jdbc:h2:mem:test", "sa", "sa");
//			BinderFactory factory = new BinderFactory(pool);
//			PreparedStatementBinder psb = factory.getBinder("SELECT * FROM INFORMATION_SCHEMA.TYPE_INFO WHERE AUTO_INCREMENT = ?");
//			log("Binder: [%s]", psb);
//		} catch (Exception ex) {
//			ex.printStackTrace(System.err);
//		}
//	}
	
	/**
	 * Simple formatted out logger
	 * @param format the message format
	 * @param args The message tokens
	 */
	public static void log(String format, Object...args) {
		System.out.println(String.format(format, args));
	}
	
	
	/**
	 * <p>Title: BinderFactory</p>
	 * <p>Description: Factory class for generating PreparedStatementBinders for a given SQL statement</p> 
	 * <p>Company: Helios Development Group LLC</p>
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>net.opentsdb.catalog.SQLWorker.BinderFactory</code></p>
	 */
	public static class BinderFactory {
		/** The Javassist classpool */
		protected final ClassPool classPool = new ClassPool();
		/** A cache of binders keyed by the SQL statement */
		protected final Map<String, AbstractPreparedStatementBinder> binders = new ConcurrentHashMap<String, AbstractPreparedStatementBinder>();
		/** A class naming key */
		protected final AtomicLong serial = new AtomicLong(0L);
		/** The datasource providing connections for this binder factory */
		protected final DataSource ds;
		
		
		/** The size of the sliding window tracking SQLWorker execution times */
		protected final int windowSize;
		
		/** The PreparedStatementBinder Ct Interface */
		protected final CtClass binderIface;
		/** The SQLException ctclass */
		protected final CtClass[] sqlEx;
		/** The signature to the abstract base class ctor  */
		protected final CtClass[] parentCtorSig;
		
		/** The String ctclass */
		protected final CtClass str;
		/** The AtomicLong ctclass */
		protected final CtClass atomicL;
		
		/** The Object ctclass */
		protected final CtClass obj;
		
		/** The PreparedStatementBinder bind CtMethod */
		protected final CtMethod bindMethod;
		/** The AbstractPreparedStatementBinder parent class CtClass */
		protected final CtClass parentClass;
		
		/** An empty CtClass array const */
		protected static final CtClass[] EMPTY_ARR = {};
		
		/**
		 * Creates a new BinderFactory
		 * @param ds The datasource providing connections for this binder factory
		 * @param windowSize The size of the sliding window tracking SQLWorker execution times
		 */
		public BinderFactory(final DataSource ds, final int windowSize) {
			this.ds = ds;
			this.windowSize = windowSize;
			Connection conn = null;
			Statement st = null;
			try {
				classPool.appendClassPath(new LoaderClassPath(PreparedStatementBinder.class.getClassLoader()));
				conn = this.ds.getConnection();
				st = conn.createStatement();
				classPool.appendClassPath(new LoaderClassPath(st.getClass().getClassLoader()));
				binderIface = classPool.get(PreparedStatementBinder.class.getName());
				parentClass = classPool.get(AbstractPreparedStatementBinder.class.getName());
				bindMethod = parentClass.getDeclaredMethod("doBind");
				
				sqlEx = new CtClass[] {classPool.get(SQLException.class.getName())};
				str = classPool.get(String.class.getName());
				obj = classPool.get(Object.class.getName());
				atomicL = classPool.get(AtomicLong.class.getName());
				parentCtorSig = new CtClass[]{str, CtClass.intType};
			} catch (Exception ex) {
				throw new RuntimeException("Failed to create PreparedStatementBinder CtClass IFace", ex);
			} finally {
				if(st!=null) try { st.close(); } catch (Exception x) { /* No Op */ }
				if(conn!=null) try { conn.close(); } catch (Exception x) { /* No Op */ }
			}
		}
		
		/**
		 * Returns a PreparedStatementBinder for the passed SQL statement
		 * @param sqlText The SQL statement to create a binder for
		 * @return the built PreparedStatementBinder instance
		 */
		public AbstractPreparedStatementBinder getBinder(String sqlText) {
			AbstractPreparedStatementBinder psb = binders.get(sqlText);
			if(psb==null) {
				synchronized(binders) {
					psb = binders.get(sqlText);
					if(psb==null) {
						psb = buildBinder(sqlText);
						binders.put(sqlText, psb);
					}
				}
			}
			return psb;
		}
		
		/**
		 * Builds a PreparedStatementBinder
		 * @param sqlText The SQL statement to create a binder for
		 * @return the built PreparedStatementBinder instance
		 */
		protected AbstractPreparedStatementBinder buildBinder(final String sqlText) {
			Connection conn = null;
			PreparedStatement ps = null;
			AbstractPreparedStatementBinder psb = null;
			try {
				final String className = "PreparedStatementBinder" + serial.incrementAndGet();
				conn = ds.getConnection();				
				ps = conn.prepareStatement(sqlText);//.unwrap(AbstractPreparedStatementBinder.ORA_PS_CLASS);
				log.info("UNMODIFIED PMD: {}", dumpParameterMetaData(ps.getParameterMetaData()));
				final ParameterMetaData pmd;
				final int returningBinds;
				if(isReturning(sqlText)) {
					NVP<Integer,ParameterMetaData> nvp = getNonReturningPMD(ds, sqlText);
					pmd = nvp.getValue();
					returningBinds = nvp.getKey();
				} else {
					returningBinds = 0;
					pmd = ps.getParameterMetaData();
				}
				
				
				
				
				
				CtClass binderClazz = classPool.makeClass(className, parentClass);
				
				
//				
//				CtMethod getm = CtNewMethod.copy(getSqlMethod, binderClazz, null);
//				getm.setBody("{ return sqltext; }");
//				getm.setModifiers(getm.getModifiers() & ~Modifier.ABSTRACT);
//				binderClazz.addMethod(getm);
				
				CtMethod bindm = CtNewMethod.copy(bindMethod, binderClazz, null);
//				CtNewMethod.copy(bindMethod, binderClazz, null);
//				bindm.setExceptionTypes(sqlEx);
				StringBuilder b = new StringBuilder("{ execCounter.incrementAndGet(); ");
				final int inBinds = pmd.getParameterCount();
				for(int i = 0; i < returningBinds; i++) {
					// registerOutputParameter(final PreparedStatement ps, final int index, final int type) 
					b.append("registerOutputParameter($1, ").append(i+1 + inBinds).append(", ").append(Types.VARCHAR).append(");");
				}
				for(int i = 0; i < inBinds; i++) {
					final int sqlType = pmd.getParameterType(i+1);
					String objRef = new StringBuilder("$2[").append(i).append("]").toString();
					b.append("if($2[").append(i).append("]==null) {");
					b.append("$1.setNull(").append(i+1).append(", ").append(sqlType).append(");");
					b.append("} else {");
					if(isBindTimestamp(sqlType)) {
						b.append("if( Number.class.isInstance(").append(objRef).append(") ) {");
						b.append("$1.setObject(").append(i+1).append(", new java.sql.Timestamp(objectToMs(").append(objRef).append(")), ").append(sqlType).append(");");
						b.append("} else {");
						b.append("$1.setObject(").append(i+1).append(", ").append(objRef).append(", ").append(sqlType).append("); }");
					} else {
						b.append("$1.setObject(").append(i+1).append(", ").append(objRef).append(", ").append(sqlType).append(");");
					}					
					b.append("}");
				}
				b.append("}");
				bindm.setBody(b.toString());				
				binderClazz.addMethod(bindm);
				binderClazz.addConstructor(CtNewConstructor.make(parentCtorSig, EMPTY_ARR, binderClazz));
				binderClazz.writeFile(SAVE_DIR);
				Class<AbstractPreparedStatementBinder> javaClazz = binderClazz.toClass(SQLWorker.class.getClassLoader(), SQLWorker.class.getProtectionDomain());
				
				psb = javaClazz.getConstructor(String.class, int.class).newInstance(sqlText, windowSize);
				JMXHelper.registerMBean(psb, psb.getObjectName());
				binderClazz.detach();
				return psb;
			} catch (Exception ex) {
				throw new RuntimeException("Failed to build PreparedStatementBinder for statement [" + sqlText + "]", ex);
			} finally {
				if(ps!=null) try { ps.close(); } catch (Exception x) { /* No Op */ }
				if(conn!=null) try { conn.close(); } catch (Exception x) { /* No Op */ }				
			}
		}
		
	}
	
	
	/** The directory to save compiled classes in */
	public static final String SAVE_DIR = (System.getProperty("java.io.tmpdir") + File.separator + "sqlWorker" + File.separator).replace(File.separator + File.separator,  File.separator);
	
	public static final String RETURNING_TOKEN = " returning ";
	public static final int RETURNING_TOKEN_LENGTH = RETURNING_TOKEN.length();
	
	public static boolean isReturning(final String sqlText) {
		final String sql = sqlText.trim().toLowerCase();
		final int index = sql.lastIndexOf(RETURNING_TOKEN);
		if(index==-1) return false;
		return sql.substring(index).indexOf('?')!=-1;
	}
	
	public static NVP<Integer, ParameterMetaData> getNonReturningPMD(final DataSource ds, final String sql) {
		PreparedStatement nonRetPs = null;
		PreparedStatement unwrappedPs = null;
		Connection conn = null;
		try {
			final int index = sql.toLowerCase().lastIndexOf(RETURNING_TOKEN);
			final String retSql = sql.substring(index);
			int numberOfBinds = 0;
			for(char c: retSql.toCharArray()) {
				if(c=='?') numberOfBinds++;
			}
			final String newSql = sql.substring(0, index);
			log.info("NEW SQL: {}", newSql);
			conn = ds.getConnection();
			nonRetPs = conn.prepareStatement(newSql);
			unwrappedPs = nonRetPs.unwrap(AbstractPreparedStatementBinder.ORA_PS_CLASS);			
			final ParameterMetaData pmd = unwrappedPs.getParameterMetaData();
			log.info("NEW SQL PMD: {}", dumpParameterMetaData(pmd));
			return new NVP<Integer, ParameterMetaData>(numberOfBinds, pmd);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		} finally {
			if(conn!=null) try { conn.close(); } catch (Exception x) {/* No Op */}
			if(nonRetPs!=null) try { nonRetPs.close(); } catch (Exception x) {/* No Op */}
		}
	}
	
	public static String dumpParameterMetaData(final ParameterMetaData pmd) {
		try {
			final StringBuilder b = new StringBuilder("ParameterMetaData [");
			final int pcount = pmd.getParameterCount();
			b.append("\n\tParam Count:").append(pcount);
			for(int i = 1; i <= pcount; i++) {
				b.append("\n\tParam#").append(i);
				try {
					b.append("\n\t\tTypeCode:");
					b.append(pmd.getParameterType(i));
				} catch (Exception ex) {
					b.append("N/A");
				}
				try {
					b.append("\n\t\tTypeName:");
					b.append(pmd.getParameterType(i));
				} catch (Exception ex) {
					b.append("N/A");
				}
				try {
					b.append("\n\t\tMode:");
					b.append(pmd.getParameterMode(i));
				} catch (Exception ex) {
					b.append("N/A");
				}				
			}
			return b.append("\n]").toString();
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
	
	/**
	 * Determines if the passed {@link java.sql.Types} type code can 
	 * be bound as a new {@link java.sql.Timestamp} using the value as a long based time.
	 * @param type The sql type code
	 * @return true if the type is a time type
	 */
	public static boolean isBindTimestamp(final int type) {
		return type==Types.TIMESTAMP || type==Types.DATE;
	}

	
}
