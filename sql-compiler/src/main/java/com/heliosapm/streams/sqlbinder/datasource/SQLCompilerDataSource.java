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
package com.heliosapm.streams.sqlbinder.datasource;

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.ObjectName;
import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.h2.tools.Server;

import com.heliosapm.streams.sqlbinder.DBType;
import com.heliosapm.streams.sqlbinder.SQLWorker;
import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.jmx.JMXManagedScheduler;
import com.heliosapm.utils.jmx.JMXManagedThreadFactory;
import com.heliosapm.utils.lang.StringHelper;
import com.heliosapm.utils.url.URLHelper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * <p>Title: SQLCompilerDataSource</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.sqlbinder.datasource.SQLCompilerDataSource</code></p>
 */

public class SQLCompilerDataSource implements Closeable {

	/** The singleton instances keyed by jdbc url / user */
	private static final ConcurrentHashMap<String, SQLCompilerDataSource> dataSources = new ConcurrentHashMap<String, SQLCompilerDataSource>();
	/** The serial number generator to create unique ds names if one is not supplied */
	private static final AtomicInteger dsSerial = new AtomicInteger();
	
	/** The scheduled executor JMX MBean ObjectName */
	protected static final ObjectName schedulerObjectName = JMXHelper.objectName("com.heliosapm.streams.sqlbinder:service=Scheduler,type=DataSourceEviction");
	/** The scheduled executor */
	protected static final JMXManagedScheduler scheduler = new JMXManagedScheduler(schedulerObjectName, "SQLCompilerDataSourceScheduler", 4, true);
	
	

	/** The configuration key for the JDBC data source name */
	public static final String CONFIG_DS_NAME = "db.datasource.name";
	/** The default JDBC data source name template */
	public static final String DEFAULT_DS_NAME = "%sDataSource#%s";
	
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

	/** The configuration key for the data source initialization DDL */
	public static final String CONFIG_DS_DDL = "db.datasource.ddl";
	/** The default data source name */
	public static final String[] DEFAULT_DS_DDL = {};
	
	/** The configuration key for the tcp server port (for H2) */
	public static final String CONFIG_DS_TCP = "db.tcp.port";
	/** The default tcp server port (for H2) */
	public static final int DEFAULT_DS_TCP = 7082;
	/** The configuration key for the http server port */
	public static final String CONFIG_DS_HTTP = "db.http.port";
	/** The default http server port */
	public static final int DEFAULT_DS_HTTP = 7083;
	
	public static void main(String[] args) {
//		final String user = "tsdb";
//		System.setProperty(DefaultDataSource.CONFIG_DS_CLASS, "org.postgresql.ds.PGSimpleDataSource");
//		System.setProperty(DefaultDataSource.CONFIG_DS_URL, "jdbc:postgresql://localhost:5432/" + user);
//		System.setProperty(DefaultDataSource.CONFIG_DS_USER, user);
//		System.setProperty(DefaultDataSource.CONFIG_DS_PW, user);
//		System.setProperty(DefaultDataSource.CONFIG_DS_TESTSQL, "SELECT current_timestamp");

//		System.setProperty(DefaultDataSource.CONFIG_DS_CLASS, "oracle.jdbc.pool.OracleDataSource");
//		System.setProperty(DefaultDataSource.CONFIG_DS_URL, "jdbc:oracle:thin:@//localhost:1521/XE");
//		System.setProperty(DefaultDataSource.CONFIG_DS_USER, user);
//		System.setProperty(DefaultDataSource.CONFIG_DS_PW, user);
//		System.setProperty(DefaultDataSource.CONFIG_DS_TESTSQL, "SELECT SYSDATE FROM DUAL");
		
		final SQLCompilerDataSource dds = getInstance(System.getProperties());
		final SQLWorker sqlWorker = SQLWorker.getInstance(dds.getDataSource());
		System.out.println("DB Type: [" + sqlWorker.getDBProductName() + "]");
//		StdInCommandHandler.getInstance().registerCommand("stop", new Runnable(){
//			public void run() {
//				try { dds.close(); } catch (Exception ex) {
//					ex.printStackTrace(System.err);
//				}
//				System.exit(0);
//			}
//		}).run();
	}
	
	/** The data source name */
	public final String name;
	/** The data source type */
	public final DBType dbType;
	
	/** The data source */
	protected final HikariDataSource ds;
	/** The SQLWorker instance for this data source */
	protected final SQLWorker sqlWorker;
	/** The H2 DDL resource */
	protected final String[] ddlResources;	
	
	
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
	 * Acquires the named data source instance
	 * @param name The data source name
	 * @return the data source instance
	 */
	public static SQLCompilerDataSource getInstance(final String name) {
		return dataSources.get(name.trim());
	}
	
	/**
	 * Acquires the default data source instance
	 * @return the default data source instance
	 */
	public static SQLCompilerDataSource getInstance(final Properties props) {
		final String dsName = ConfigurationHelper.getSystemThenEnvProperty(CONFIG_DS_NAME, "#~#Unlikeliez!~#~", props);
		SQLCompilerDataSource ds = dataSources.get(dsName);
		if(ds==null) {
			synchronized(dataSources) {
				ds = dataSources.get(dsName);
				if(ds==null) {
					ds = new SQLCompilerDataSource(props);
					dataSources.put(ds.name, ds);
					props.setProperty(CONFIG_DS_NAME, ds.name);
				}
			}
		}
		return ds;
	}
	
	
	private SQLCompilerDataSource(final Properties properties) {
		log.info(">>>>> Initializing DataSource....");
		HikariConfig config = new HikariConfig();
		final String url = ConfigurationHelper.getSystemThenEnvProperty(CONFIG_DS_URL, DEFAULT_DS_URL, properties);
		final String dataSourceClassName = ConfigurationHelper.getSystemThenEnvProperty(CONFIG_DS_CLASS, DEFAULT_DS_CLASS, properties);
		dbType = DBType.getMatchingDBType(url, dataSourceClassName);
		String tmp = ConfigurationHelper.getSystemThenEnvProperty(CONFIG_DS_NAME, null, properties);
		if(tmp==null) {
			name = String.format(DEFAULT_DS_NAME, dbType.name(), dsSerial.incrementAndGet());
		} else {
			name = tmp.trim();
		}
		ddlResources = ConfigurationHelper.getArraySystemThenEnvProperty(CONFIG_DS_DDL, DEFAULT_DS_DDL, properties);
//		config.setMetricRegistry(SharedMetricsRegistry.getInstance());
		config.setDataSourceClassName(dataSourceClassName);
//		config.setDriverClassName(ConfigurationHelper.getSystemThenEnvProperty(CONFIG_DS_CLASS, DEFAULT_DS_CLASS, properties));
		config.setJdbcUrl(ConfigurationHelper.getSystemThenEnvProperty(CONFIG_DS_URL, DEFAULT_DS_URL, properties));
		config.addDataSourceProperty("url",url);
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
//		config.setScheduledExecutorService(scheduler);
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
						log.info("DDL temp file: [{}]", sourceUrl.getFile().replace(File.separator, "").replace("/", "") + ".tmp");
						if(tmpSqlFile==null) tmpSqlFile = File.createTempFile("opentsdb-listener-ddl", ".tmp");
						tmpSqlFile.deleteOnExit();
						log.info("DDL temp file: [{}]", tmpSqlFile);
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
	
	public String getName() {
		return name;
	}
	
	public DBType getDBType() {
		return dbType;
	}
	
	/**
	 * <p>Closes the datasource and resets the singleton</p>
	 * {@inheritDoc}
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() throws IOException {
		log.info(">>>>> Closing DataSource [{}].....", name);
		dataSources.remove(name);
		if(tcpServer!=null) try { tcpServer.stop(); } catch (Exception x) {/* No Op */}
		if(httpServer!=null) try { httpServer.stop(); } catch (Exception x) {/* No Op */}
		try { ds.close(); } catch (Exception x) {/* No Op */}		
		log.info("<<<<< [{}] DataSource Closed.", name);		
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