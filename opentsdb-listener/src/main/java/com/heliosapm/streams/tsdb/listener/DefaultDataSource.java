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
package com.heliosapm.streams.tsdb.listener;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.management.ObjectName;
import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.h2.tools.Server;

import com.heliosapm.streams.common.metrics.SharedMetricsRegistry;
import com.heliosapm.streams.sqlbinder.SQLWorker;
import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.jmx.JMXManagedScheduler;
import com.heliosapm.utils.jmx.JMXManagedThreadFactory;
import com.heliosapm.utils.lang.StringHelper;
import com.heliosapm.utils.url.URLHelper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * <p>Title: DefaultDataSource</p>
 * <p>Description: Provides pooled JDBC connections for the listener to write metas to a DB.</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tsdb.listener.DefaultDataSource</code></p>
 */

public class DefaultDataSource implements Closeable {
	/** The singleton instance */
	private static volatile DefaultDataSource instance = null;
	/** The singleton instance ctor lock */
	private static final Object lock = new Object();
	
	/** The configuration key for the JDBC data source class name */
	public static final String CONFIG_DS_CLASS = "db.datasource.classname";
	/** The default JDBC data source class name */
	public static final String DEFAULT_DS_CLASS = "org.h2.jdbcx.JdbcDataSource";
	/** The configuration key for the JDBC user name */
	public static final String CONFIG_DS_USER = "db.datasource.username";
	/** The default JDBC user name */
	public static final String DEFAULT_DS_USER = "sa";
	/** The configuration key for the JDBC user password */
	public static final String CONFIG_DS_PW = "db.datasource.password";
	/** The default JDBC user password */
	public static final String DEFAULT_DS_PW = "";
	/** The configuration key for the JDBC URL */
	public static final String CONFIG_DS_URL = "db.url";
	/** The default JDBC URL */
	public static final String DEFAULT_DS_URL = "jdbc:h2:mem:tsdb;JMX=TRUE;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE;MVCC=TRUE";
	/** The configuration key for enabling cached prepared statements */
	public static final String CONFIG_DS_CACHEDPS = "db.datasource.cachedps";
	/** The default cached prepared statement enablement */
	public static final boolean DEFAULT_DS_CACHEDPS = true;
	/** The configuration key for the max number of cached prepared statements */
	public static final String CONFIG_DS_MAXCACHEDPS = "db.datasource.cachedps.max";
	/** The default max cached prepared statements */
	public static final int DEFAULT_DS_MAXCACHEDPS = 2048;
	/** The configuration key for the max connection pool size */
	public static final String CONFIG_DS_MAXCONNS = "db.datasource.conns.max";
	/** The default max connection pool size */
	public static final int DEFAULT_DS_MAXCONNS = 2048;
	/** The configuration key for the minimum idle connections in the pool */
	public static final String CONFIG_DS_MINCONNS = "db.datasource.conns.min";
	/** The default minimum idle connections in the pool */
	public static final int DEFAULT_DS_MINCONNS = 2;
	/** The configuration key for the connection test sql */
	public static final String CONFIG_DS_TESTSQL = "db.testsql";
	/** The default connection test sql */
	public static final String DEFAULT_DS_TESTSQL = "SELECT SYSDATE";
	/** The configuration key for the connection timeout in ms. */
	public static final String CONFIG_DS_CONNTIMEOUT = "db.datasource.conns.timeout";
	/** The default connection timeout in ms. */
	public static final long DEFAULT_DS_CONNTIMEOUT = 1002;
	/** The configuration key for enabling autocommit */
	public static final String CONFIG_DS_AUTOCOMMIT = "db.datasource.autocommit";
	/** The default autocommit enablement */
	public static final boolean DEFAULT_DS_AUTOCOMMIT = false;
	/** The configuration key for enabling hikari mbean registration  */
	public static final String CONFIG_DS_MBEANS = "db.datasource.mbeans";
	/** The default hikari mbean registration enablement */
	public static final boolean DEFAULT_DS_MBEANS = true;
	/** The configuration key for the data source name */
	public static final String CONFIG_DS_NAME = "db.datasource.name";
	/** The default data source name */
	public static final String DEFAULT_DS_NAME = "TSDBCatalogInMem";

	/** The configuration key for the data source initialization DDL */
	public static final String CONFIG_DS_DDL = "db.datasource.ddl";
	/** The default data source name */
	public static final String[] DEFAULT_DS_DDL = {"ddl/h2/catalog.sql"};
	
	/** The configuration key for the tcp server port (for H2) */
	public static final String CONFIG_DS_TCP = "db.tcp.port";
	/** The default tcp server port (for H2) */
	public static final int DEFAULT_DS_TCP = 7082;
	/** The configuration key for the http server port */
	public static final String CONFIG_DS_HTTP = "db.http.port";
	/** The default http server port */
	public static final int DEFAULT_DS_HTTP = 7083;
	
	public static void main(String[] args) {
		final DefaultDataSource dds = getInstance();
		StdInCommandHandler.getInstance().registerCommand("stop", new Runnable(){
			public void run() {
				try { dds.close(); } catch (Exception ex) {
					ex.printStackTrace(System.err);
				}
				System.exit(0);
			}
		}).run();
	}
	
	
	/** The data source */
	protected HikariDataSource ds = null;
	/** The SQLWorker instance for this data source */
	protected final SQLWorker sqlWorker;
	/** The scheduled executor */
	protected final JMXManagedScheduler scheduler;
	/** The scheduled executor JMX MBean ObjectName */
	protected final ObjectName schedulerObjectName;
	/** The H2 DDL resource */
	protected String[] ddlResources = null;	
	
	
	/** The H2 TCP Port */
	protected int tcpPort = -1;
	/** The H2 TCP Allow Others */
	protected boolean tcpAllowOthers = true;
	/** The H2 HTTP Port */
	protected int httpPort = -1;
	/** The H2 HTTP Allow Others */
	protected boolean httpAllowOthers = true;
	
	/** The container/manager for the TCP Listener */
	protected Server tcpServer = null;
	/** The container/manager for the Web Listener */
	protected Server httpServer = null;
	
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	
	/**
	 * Acquires the default data source instance
	 * @return the default data source instance
	 */
	public static DefaultDataSource getInstance() {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					instance = new DefaultDataSource(null);  // FIXME
				}
			}
		}
		return instance;
	}
	
	private DefaultDataSource(final Properties properties) {
		log.info(">>>>> Initializing DataSource....");
		HikariConfig config = new HikariConfig();
		ddlResources = ConfigurationHelper.getArraySystemThenEnvProperty(CONFIG_DS_DDL, DEFAULT_DS_DDL, properties);
		config.setMetricRegistry(SharedMetricsRegistry.getInstance());
		config.setDataSourceClassName(ConfigurationHelper.getSystemThenEnvProperty(CONFIG_DS_CLASS, DEFAULT_DS_CLASS, properties));
//		config.setDriverClassName(ConfigurationHelper.getSystemThenEnvProperty(CONFIG_DS_CLASS, DEFAULT_DS_CLASS, properties));
		config.setJdbcUrl(ConfigurationHelper.getSystemThenEnvProperty(CONFIG_DS_URL, DEFAULT_DS_URL, properties));
		config.addDataSourceProperty("url",ConfigurationHelper.getSystemThenEnvProperty(CONFIG_DS_URL, DEFAULT_DS_URL, properties));
		config.addDataSourceProperty("user", ConfigurationHelper.getSystemThenEnvProperty(CONFIG_DS_USER, DEFAULT_DS_USER, properties));
		config.addDataSourceProperty("password", ConfigurationHelper.getSystemThenEnvProperty(CONFIG_DS_PW, DEFAULT_DS_PW, properties));
		config.setUsername(ConfigurationHelper.getSystemThenEnvProperty(CONFIG_DS_USER, DEFAULT_DS_USER, properties));
		config.setPassword(ConfigurationHelper.getSystemThenEnvProperty(CONFIG_DS_PW, DEFAULT_DS_PW, properties));
//		config.addDataSourceProperty("cachePrepStmts", "" + ConfigurationHelper.getBooleanSystemThenEnvProperty(CONFIG_DS_CACHEDPS, DEFAULT_DS_CACHEDPS, properties));
//		config.addDataSourceProperty("prepStmtCacheSize", "" + ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_DS_MAXCACHEDPS, DEFAULT_DS_MAXCACHEDPS, properties));
//		config.addDataSourceProperty("prepStmtCacheSqlLimit", "" + ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_DS_MAXCACHEDPS, DEFAULT_DS_MAXCACHEDPS, properties));
		config.setMaximumPoolSize(ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_DS_MAXCONNS, DEFAULT_DS_MAXCONNS, properties));
		config.setMinimumIdle(ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_DS_MINCONNS, DEFAULT_DS_MINCONNS, properties));
		config.setConnectionTestQuery(ConfigurationHelper.getSystemThenEnvProperty(CONFIG_DS_TESTSQL, DEFAULT_DS_TESTSQL, properties));
		config.setConnectionTimeout(ConfigurationHelper.getLongSystemThenEnvProperty(CONFIG_DS_CONNTIMEOUT, DEFAULT_DS_CONNTIMEOUT, properties));
		config.setAutoCommit(ConfigurationHelper.getBooleanSystemThenEnvProperty(CONFIG_DS_AUTOCOMMIT, DEFAULT_DS_AUTOCOMMIT, properties));
		config.setRegisterMbeans(ConfigurationHelper.getBooleanSystemThenEnvProperty(CONFIG_DS_MBEANS, DEFAULT_DS_MBEANS, properties));
		config.setPoolName(ConfigurationHelper.getSystemThenEnvProperty(CONFIG_DS_NAME, DEFAULT_DS_NAME, properties));
		config.setThreadFactory(JMXManagedThreadFactory.newThreadFactory("HikariPool-" + config.getPoolName(), true));
		log.info("Config: {}", config.toString());
		schedulerObjectName = JMXHelper.objectName("com.heliosapm.streams.tsdb.listener:service=Scheduler,name=JDBCConnectionPool");
		scheduler = new JMXManagedScheduler(schedulerObjectName, "JDBCConnectionPool", 2, true);
		//config.setScheduledExecutorService(scheduler);
		ds = new HikariDataSource(config);
		sqlWorker = SQLWorker.getInstance(ds);		
		startServers(properties);
		runDDL(properties);
		log.info("<<<<< DataSource Initialized.");
		
	}
	
	
	protected void runDDL(final Properties properties) {
		if(ddlResources.length > 0) {
			final String dsClassName = ds.getDataSourceClassName(); 
			if(dsClassName!=null && dsClassName.equals(DEFAULT_DS_CLASS)) {
			
				log.info(">>>>> Initializing DDL....");
				String pResource = null;
				Connection conn = null;
				Statement st = null;
				File tmpSqlFile = null;
				try {
					conn = ds.getConnection();
					conn.setAutoCommit(true);
					st = conn.createStatement();
					for(String rez: ddlResources) {
						
						pResource = rez; 
						log.info("Processing DDL Resource [{}]", rez);
						final URL sourceUrl = URLHelper.toURL(rez);
						final String sqlText = StringHelper.resolveTokens(URLHelper.getTextFromURL(sourceUrl), properties);
						if(tmpSqlFile==null) tmpSqlFile = File.createTempFile(sourceUrl.getFile().replace(File.separator, "").replace("/", ""), ".tmp");
						URLHelper.writeToURL(URLHelper.toURL(tmpSqlFile), sqlText, false);
						st.execute("RUNSCRIPT FROM '" + tmpSqlFile + "'");
						log.info("DDL Resource [{}] Processed", rez);						
					}
				} catch (Exception ex) {
					log.error("Failed to run DDL Resource [" + pResource + "]", ex);
					throw new RuntimeException("Failed to run DDL Resource [" + pResource + "]", ex);
				} finally {					
					if(st!=null) try { st.close(); } catch (Exception x) { /* No Op */ }
					if(conn!=null) try { conn.close(); } catch (Exception x) { /* No Op */ }
					if(tmpSqlFile!=null) tmpSqlFile.delete();
				}
				log.info("<<< DDL Initialized.");
			} else {
				log.warn("Executing DDL against non H2 Databases Not Supported: {}", ds.getDriverClassName());
			}
		}
	}
	
	protected void startServers(final Properties properties) {
		final String dsClassName = ds.getDataSourceClassName(); 
		if(dsClassName!=null && dsClassName.equals(DEFAULT_DS_CLASS)) {
			tcpPort = ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_DS_TCP, DEFAULT_DS_TCP, properties);
			if(tcpPort!=-1) {
				final List<String> args = new ArrayList<String>(Arrays.asList("-tcp", "-tcpDaemon", "-tcpPort", "" + tcpPort, "-tcpAllowOthers"));
				log.info("Starting H2 TCP Listener on port [{}]. Allow Others:[{}]", tcpPort, tcpAllowOthers);
				tcpServer = new Server();
				try {
					tcpServer.runTool(args.toArray(new String[0]));
				} catch (Exception ex) {
					log.error("Failed to start TCP Server on port [" + tcpPort + "]", ex);
				}
			}
		}
		httpPort = ConfigurationHelper.getIntSystemThenEnvProperty(CONFIG_DS_HTTP, DEFAULT_DS_HTTP, properties);
		if(httpPort!=-1) {
			final List<String> args = new ArrayList<String>(Arrays.asList("-web", "-webDaemon", "-webPort", "" + httpPort, "-webAllowOthers"));
			log.info("Starting H2 Web Listener on port [{}]. Allow Others:[{}]", httpPort, httpAllowOthers);
			httpServer = new Server();
			try {
				httpServer.runTool(args.toArray(new String[0]));
			} catch (Exception ex) {
				log.error("Failed to start Web Server on port [" + tcpPort + "]", ex);
			}
		}
	}
	
	/**
	 * <p>Closes the datasource and resets the singleton</p>
	 * {@inheritDoc}
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() throws IOException {
		log.info(">>>>> Closing DataSource....");
		if(tcpServer!=null) try { tcpServer.stop(); } catch (Exception x) {/* No Op */}
		if(httpServer!=null) try { httpServer.stop(); } catch (Exception x) {/* No Op */}
		try { ds.close(); } catch (Exception x) {/* No Op */}
		scheduler.shutdown();
		try { JMXHelper.unregisterMBean(schedulerObjectName); } catch (Exception x) {/* No Op */}
		instance = null;
		log.info("<<<<< DataSource Closed.");		
	}

	/**
	 * Returns this datasource's SQLWorker
	 * @return this datasource's SQLWorker
	 */
	public SQLWorker getSQLWorker() {
		return sqlWorker;
	}
	
	/**
	 * Acquires a pooled connection
	 * @return a pooled connection
	 * @throws SQLException thrown if the datasource fails to cough up a connection
	 */
	public Connection getConnection() throws SQLException {
		return ds.getConnection();
	}


	/**
	 * Returns the hikari-cp data source
	 * @return the hikari-cp data source
	 */
	public DataSource getDataSource() {
		return ds;
	}
	
	/**
	 * Returns the inner data source
	 * @return the inner data source
	 */
	public DataSource getInnerDataSource() {
		return ds.getDataSource();
	}
	
	
}


//Database	Driver	DataSource class
//Apache Derby	Derby	org.apache.derby.jdbc.ClientDataSource
//Firebird	Jaybird	org.firebirdsql.pool.FBSimpleDataSource
//H2	H2	org.h2.jdbcx.JdbcDataSource
//HSQLDB	HSQLDB	org.hsqldb.jdbc.JDBCDataSource
//IBM DB2	IBM JCC	com.ibm.db2.jcc.DB2SimpleDataSource
//IBM Informix	IBM Informix	com.informix.jdbcx.IfxDataSource
//MS SQL Server	Microsoft	com.microsoft.sqlserver.jdbc.SQLServerDataSource
//MySQL	Connector/J	com.mysql.jdbc.jdbc2.optional.MysqlDataSource
//MySQL/MariaDB	MariaDB	org.mariadb.jdbc.MySQLDataSource
//Oracle	Oracle	oracle.jdbc.pool.OracleDataSource
//OrientDB	OrientDB	com.orientechnologies.orient.jdbc.OrientDataSource
//PostgreSQL	pgjdbc-ng	com.impossibl.postgres.jdbc.PGDataSource
//PostgreSQL	PostgreSQL	org.postgresql.ds.PGSimpleDataSource
//SAP MaxDB	SAP	com.sap.dbtech.jdbc.DriverSapDB
//SQLite	xerial	org.sqlite.SQLiteDataSource
//SyBase	jConnect	com.sybase.jdbc4.jdbc.SybDataSource