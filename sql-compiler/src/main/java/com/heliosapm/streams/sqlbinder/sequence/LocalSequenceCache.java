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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Title: LocalSequenceCache</p>
 * <p>Description: A local in-vm cache for DB sequence ranges</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.sqlbinder.sequence.sequence.LocalSequenceCache</code></p>
 * TODO: Need to be able to vary cache size definitions for different situations. e.g. synch from HBase will need xtra-large chunks.
 */

public class LocalSequenceCache implements ISequenceCache {
	/** The local sequence increment */
	protected final long increment;
	/** The DB Sequence name, fully qualified if necessary  */
	protected final String sequenceName;
	/** The current local sequence value */
	protected final AtomicLong currentValue = new AtomicLong(0);
	/** The current ceiling on the local sequence value */
	protected final AtomicLong ceiling = new AtomicLong(0);
	/** Atomic switch indicating a refresh is required, i.e. if true, we're good to go, otherwise a refresh is needed */
	protected final AtomicBoolean rangeIsFresh = new AtomicBoolean(false);
	/** The datasource to provide connections to refresh the sequence cache */
	protected final DataSource dataSource;
	/** The SQL used to retrieve the next sequence value */
	protected String seqSql;
	/** Instance logger */
	protected final Logger log;
	
	
	
	/**
	 * Creates a new LocalSequenceCache
	 * @param increment The local sequence increment
	 * @param sequenceName The DB Sequence name, fully qualified if necessary
	 * @param dataSource The datasource to provide connections to refresh the sequence cache
	 */
	public LocalSequenceCache(final long increment, String sequenceName, DataSource dataSource) {		
		log = LoggerFactory.getLogger(getClass().getName() + "." + sequenceName);
		this.increment = increment;
		this.sequenceName = sequenceName;
		this.dataSource = dataSource;
		init();
		refresh();
		log.info("Created LocalSequenceCache [{}]", sequenceName);
	}
	

	/**
	 * 
	 */
	@Override
	public void reset() {
		currentValue.set(0);
		ceiling.set(0);
		rangeIsFresh.set(false);
	}

	
	/**
	 * Initializes the SQL statement
	 */
	protected void init() {
		seqSql = "SELECT " + sequenceName + ".NEXTVAL FROM DUAL";
	}
	

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.sqlbinder.sequence.ISequenceCache#next()
	 */
	@Override
	public long next() {
		long next = -1;
		for(;;) {
			if(rangeIsFresh.compareAndSet(true, (next = currentValue.incrementAndGet())<ceiling.get())) {
				return next;
			}
			rangeIsFresh.set(false);
			refresh();
		}
	}
	
	/**
	 * Refreshes the  sequence range
	 */
	protected void refresh() {
		log.info("Refreshing....Current: [{}]", currentValue.get());
		final long startTime = System.currentTimeMillis();
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rset = null;
		final long target = ceiling.get() + increment;
		long retrieved = 0;
		int loops = 0;
		try {
			conn = dataSource.getConnection();
			ps = conn.prepareStatement(seqSql);
			while(retrieved < target) {
				rset = ps.executeQuery();
				rset.next();
				retrieved += rset.getLong(1);
				rset.close(); rset = null;
				loops++;
				if(loops>increment) {
					throw new RuntimeException("Refresh loops exceeded increment [" + loops + "/" + increment + "]");
				}
			}
			ceiling.set(retrieved);
			currentValue.set(retrieved-increment);
			
			rangeIsFresh.set(true);
			log.info("Refreshed loops:{} current:{} ceiling:{}   Elapsed: {} ms.", loops, currentValue.get(), ceiling.get(), System.currentTimeMillis()-startTime);
		} catch (Exception ex) {
			throw new RuntimeException("Failed to refresh sequence [" + sequenceName + "]", ex);
		} finally {
			if(rset!=null) try { rset.close(); } catch (Exception x) {/* No Op */}
			if(ps!=null) try { ps.close(); } catch (Exception x) {/* No Op */}
			if(conn!=null) try { conn.close(); } catch (Exception x) {/* No Op */}			
		}
	}
	
	
//	public static void main(String[] args) {
//		// jdbc:oracle:thin:@192.168.1.23:1521:ORCL
//		// oracle.jdbc.driver.OracleDriver
//		Properties p = new Properties();
//		p.setProperty(ICatalogDataSource.JDBC_POOL_JDBCDRIVER, "oracle.jdbc.driver.OracleDriver");
//		p.setProperty(ICatalogDataSource.JDBC_POOL_JDBCURL, "jdbc:oracle:thin:@192.168.1.23:1521:ORCL");
//		p.setProperty(ICatalogDataSource.JDBC_POOL_USERNAME, "TSDB");
//		p.setProperty(ICatalogDataSource.JDBC_POOL_PASSWORD, "tsdb");
//		CatalogDataSource cds = CatalogDataSource.getInstance();
//		cds.initialize(null, p);
//		System.out.println(p);
//		int loops = 1000;
//		LocalSequenceCache lsc = new OracleLocalSequenceCache(50, "TEST_SEQ", cds.getDataSource());
//		Set<Long> sequences = new HashSet<Long>(loops);
//		try {
//			for(int i = 0; i < loops; i++) {
//				long n = lsc.next();
//				if(!sequences.add(n)) {
//					throw new RuntimeException("Unexpected Dup:" + n);
//				}
//				
//			}
//		} finally {
//			cds.shutdown();
//		}
//	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format(
				"LocalSequenceCache [sequenceName=%s, increment=%s]",
				sequenceName, increment);
	}
	
	
	


}
