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

import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.ObjectName;

import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.unsafe.collections.ConcurrentLongSlidingWindow;

/**
 * <p>Title: AbstractPreparedStatementBinder</p>
 * <p>Description: The abstract base class for {@link PreparedStatementBinder} concrete instances</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.sqlbinder.AbstractPreparedStatementBinder</code></p>
 */

@SuppressWarnings("unchecked")
public abstract class AbstractPreparedStatementBinder implements AbstractPreparedStatementBinderMBean, PreparedStatementBinder {
	/** The underlying SQL */
	protected final String sqltext;
	/** The execution counter */
	protected final AtomicLong execCounter = new AtomicLong(0L);	
	/** The error counter */
	protected final AtomicLong errorCounter = new AtomicLong(0L);

	/** A sliding window of ps execution elapsed times in ns. */
	protected final ConcurrentLongSlidingWindow execTimes;
	/** A sliding window of ps batch execution elapsed times in ns. */
	protected final ConcurrentLongSlidingWindow batchExecTimes;
	/** A sliding window of ps batch size in statements batched */
	protected final ConcurrentLongSlidingWindow batchSizes;
	/** The JMX ObjectName for this binder */
	final ObjectName objectName;
	
	/** The lowest threshold of milliseconds at which a long value is considered to be in milliseconds 
	 * rather than seconds.This means that millisecond based timestamps will only
	 *  be interpreted properly if the date/time represented is approximately <b><code>Sat Jul 18 16:11:09 EDT 1970</code></b>
	 *  or a long value of <b><code>17179869184</code></b>. This also the lowest long value where 
	 *  {@link Long#numberOfLeadingZeros(long)} is less than <b><code>30</code></b> */
	public static final long LOWEST_MS_TIME = 17179869184L; //30;
	
	public static final Class<? extends PreparedStatement> ORA_PS_CLASS;
	public static final Method ORA_PS_REGOUT;
	
	static {
		Class<? extends PreparedStatement> tmpClass = null;
		Method tmpMethod = null;
		try {
			tmpClass = (Class<? extends PreparedStatement>) Class.forName("oracle.jdbc.OraclePreparedStatement");
			tmpMethod = tmpClass.getDeclaredMethod("registerReturnParameter", int.class, int.class);
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
		}
		ORA_PS_CLASS = tmpClass;
		ORA_PS_REGOUT = tmpMethod;
	}
  
	/**
	 * Creates a new AbstractPreparedStatementBinder
	 * @param sqltext The underlying SQL statement
	 * @param windowSize The size of the sliding windows tracking ps execution times
	 */
	public AbstractPreparedStatementBinder(final String sqltext, final int windowSize) {
		this.sqltext = sqltext;
		execTimes = new ConcurrentLongSlidingWindow(windowSize);
		batchExecTimes = new ConcurrentLongSlidingWindow(windowSize);
		batchSizes = new ConcurrentLongSlidingWindow(windowSize);
		objectName = JMXHelper.objectName(getClass().getSuperclass().getPackage().getName() + ":type=SQLBinder,name=" +  getClass().getSimpleName());
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.sqlbinder.PreparedStatementBinder#getObjectName()
	 */
	@Override
	public ObjectName getObjectName() {		
		return objectName;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.sqlbinder.PreparedStatementBinder#bind(java.sql.PreparedStatement, java.lang.Object[])
	 */
	@Override
	public void bind(final PreparedStatement ps, final Object... args) {		
		try {
			doBind(ps.unwrap(ORA_PS_CLASS), args);
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			throw new RuntimeException(ex);
		}
	}
	
	public abstract void doBind(final PreparedStatement ps, final Object... args);
	
	
	
	public void registerOutputParameter(final PreparedStatement ps, final int index, final int type) {
		try {
			ORA_PS_REGOUT.invoke(ps, index, type);
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			throw new RuntimeException(ex);			
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		final long avgElapsedNs = getAvgExecTime();
		final long avgElapsedMs = TimeUnit.MILLISECONDS.convert(avgElapsedNs, TimeUnit.NANOSECONDS);
		final long avgBatchElapsedNs = getAvgBatchExecTime();
		final long avgBatchElapsedMs = TimeUnit.MILLISECONDS.convert(avgBatchElapsedNs, TimeUnit.NANOSECONDS);
		
		return new StringBuilder("[")
			.append(getClass().getSimpleName())
			.append("]:")
			.append("Execs:").append(getExecutionCount())
			.append(", Errors:").append(getErrorCount())
			.append(", AvgElapsed(ms):").append(avgElapsedMs)	
			.append(", AvgElapsed(ns):").append(avgElapsedNs)
			.append(", AvgBatchElapsed(ms):").append(avgBatchElapsedMs)	
			.append(", AvgBatchElapsed(ns):").append(avgBatchElapsedNs)
			
			.append("\n\t").append(sqltext)
			.toString();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.sqlbinder.AbstractPreparedStatementBinderMBean#getSQL()
	 */
	@Override
	public String getSQL() {
		return sqltext;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.sqlbinder.AbstractPreparedStatementBinderMBean#getExecutionCount()
	 */
	@Override
	public long getExecutionCount() {
		return execCounter.get();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.sqlbinder.AbstractPreparedStatementBinderMBean#getErrorCount()
	 */
	@Override
	public long getErrorCount() {		
		return errorCounter.get();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.sqlbinder.PreparedStatementBinder#recordError()
	 */
	@Override
	public void recordError() {
		errorCounter.incrementAndGet();		
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.sqlbinder.AbstractPreparedStatementBinderMBean#resetMetrics()
	 */
	@Override
	public void resetMetrics() {
		execTimes.clear();
		execCounter.set(0L);
		errorCounter.set(0L);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.sqlbinder.AbstractPreparedStatementBinderMBean#getLastExecTime()
	 */
	@Override
	public long getLastExecTime() {
		return execTimes.getNewest();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.sqlbinder.AbstractPreparedStatementBinderMBean#getMinExecTime()
	 */
	@Override
	public long getMinExecTime() {
		return execTimes.min();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.sqlbinder.AbstractPreparedStatementBinderMBean#getMaxExecTime()
	 */
	@Override
	public long getMaxExecTime() {
		return execTimes.max();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.sqlbinder.PreparedStatementBinder#ptileExecTime(int)
	 */
	@Override
	public long ptileExecTime(final int p) {
		return execTimes.percentile(p);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.sqlbinder.AbstractPreparedStatementBinderMBean#getTotalExecTime()
	 */
	@Override
	public long getTotalExecTime() {
		return execTimes.sum();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.sqlbinder.AbstractPreparedStatementBinderMBean#getAvgExecTime()
	 */
	@Override
	public long getAvgExecTime() {
		return execTimes.avg();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.sqlbinder.AbstractPreparedStatementBinderMBean#getAvgBatchExecTime()
	 */
	@Override
	public long getAvgBatchExecTime() {
		return batchExecTimes.avg();
	}

	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.sqlbinder.PreparedStatementBinder#recordExecTime(long)
	 */
	@Override
	public void recordExecTime(final long ns) {
		execTimes.insert(ns);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.sqlbinder.PreparedStatementBinder#recordBatchExecTime(long)
	 */
	@Override
	public void recordBatchExecTime(final long ns) {
		batchExecTimes.insert(ns);
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.sqlbinder.PreparedStatementBinder#recordBatchExecSize(long)
	 */
	@Override
	public void recordBatchExecSize(final long size) {
		batchSizes.insert(size);		
	}
	
	/**
	 * Converts the passed value to milliseconds if it is determined to be in seconds.
	 * Otherwise returns the passed value.
	 * @param utc The value to convert to ms.
	 * @return the value in ms.
	 */
	public static long longToMs(final long utc) {		
		if(utc!=0 && isSeconds(utc)) {
			return utc*1000;
		}
		return utc;
	}
	
	/**
	 * Converts the passed value to milliseconds if it is determined to be in seconds.
	 * Otherwise returns the passed value.
	 * @param utc The value to convert to ms.
	 * @return the value in ms.
	 */
	public static long objectToMs(final Object utc) {
		return longToMs(((Number)utc).longValue());
	}
	
	/**
	 * Returns true if the passed long value should be interpreted as milliseconds, false otherwise
	 * @param value The long value to test
	 * @return true if the passed long value should be interpreted as milliseconds, false otherwise
	 */
	public static boolean isMillis(final long value) {
		return value >= LOWEST_MS_TIME;
	}
	
	/**
	 * Returns true if the passed long value should be interpreted as seconds, false otherwise
	 * @param value The long value to test
	 * @return true if the passed long value should be interpreted as seconds, false otherwise
	 */
	public static boolean isSeconds(final long value) {
		return value < LOWEST_MS_TIME;
	}	
	
		
}
