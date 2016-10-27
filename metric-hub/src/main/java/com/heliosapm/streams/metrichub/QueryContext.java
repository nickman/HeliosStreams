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
package com.heliosapm.streams.metrichub;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.heliosapm.streams.metrichub.serialization.TSDBTypeSerializer;

import net.opentsdb.utils.JSON;

/**
 * <p>Title: QueryContext</p>
 * <p>Description: The query context is a set of configuration items and query state fields representing
 * the currently executed query. It accompanies the query and is returned with the result since queries 
 * can be split into sequential pages so as to not overwhelm resources. It can also accompany query requests 
 * and responses on remote invocations.</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrichub.QueryContext</code></p>
 */

public class QueryContext {
	/** Static class logger */
	@JsonIgnore
	protected static final Logger log = LogManager.getLogger(QueryContext.class);

	/** The maximum size of each page of results to be returned */
	protected int pageSize = 100;
	/** The timeout on each submitted request in ms. */
	protected long timeout = 3000L;
	/** The timestamp at which the running asynch request will be expired */
	@JsonIgnore
	private long timeLimit = -1L;
	/** The elapsed time of the last request in ms. */
	protected long elapsed = -1L;
	/** Indicates if the query will be continuous, self re-executing until are pages are returned */
	protected boolean continuous = false;
	/** The JSON seralization format to use for serializing the response objects for this query context */
	protected String format = "DEFAULT";
	/** Indicates hard expired */
	protected boolean expired = false;
	/** Indicates if retrieved TSMetas should have the metric and tag UIDMetas populated. See {@link MetaReader#readTSMetas(java.sql.ResultSet, boolean)} */
	protected boolean includeUIDs = false;
	

	/** Recorded context events such as timings etc. */
	protected final Map<String, Object> ctx = new LinkedHashMap<String, Object>();

	
	/** The starting index for the next chunk */
	protected Object nextIndex = null;
	/** Indicates if the full result set has exhausted */
	protected boolean exhausted = false;
	/** The maximum cummulative number of items to be returned within this context */
	protected int maxSize = 5000;
	/** The cummulative number of items retrieved within this context */
	protected int cummulative = 0;
	
	public static enum QueryPhase {
		/** The start timestamp of the current query */
		START,
		SQLPREPARED,
		SQLEXECUTED,
		RSETITER;
	}
	
	/**
	 * Resets the non-configuration fields of this query context
	 * @return this query context
	 */
	public QueryContext reset() {
		nextIndex = null;
		exhausted = false;		
		cummulative = 0;
		ctx.clear();
		timeLimit = -1L;
		elapsed = -1L;
		expired = false;
		return this;
	}

	/**
	 * Returns the page size set which is the maximum number of items returned in each call
	 * @return the pageSize the page size
	 */
	public final int getPageSize() {
		return pageSize;
	}


	/**
	 * Sets the page size set which is the maximum number of items returned in each call.
	 * The default is 100.
	 * @param pageSize the pageSize to set
	 * @return this Query Options
	 */
	public final QueryContext setPageSize(int pageSize) {
		if(pageSize < 1) throw new IllegalArgumentException("Invalid page size: [" + pageSize + "]");
		this.pageSize = pageSize;
		return this;
	}

	/**
	 * Returns the next index to start from
	 * @return the nextIndex
	 */
	public final Object getNextIndex() {
		return nextIndex;
	}


	/**
	 * Sets the next index to start from 
	 * @param nextIndex the nextIndex to set
	 * @return this Query Options
	 */
	public final QueryContext setNextIndex(Object nextIndex) {
		this.nextIndex = nextIndex;
		return this;
	}
	
	

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("QueryContext [pageSize=").append(pageSize);
		builder.append(", timeout=").append(timeout);
		builder.append(", continuous=").append(continuous);		
		builder.append(", timeLimit=").append(timeLimit);
		builder.append(", format=").append(format);
		builder.append(", elapsed=").append(elapsed);
		builder.append(", maxSize=").append(maxSize);
		builder.append(", exhausted=").append(exhausted);
		builder.append(", expired=").append(isExpired());
		builder.append(", cummulative=").append(cummulative);
		if(nextIndex != null) {
			builder.append(", nextIndex=").append(nextIndex);
		}
		if(!ctx.isEmpty()) {
			builder.append(", ctx:");
			for(Map.Entry<String, Object> e: ctx.entrySet()) {
				builder.append(e.getKey()).append(":").append(e.getValue()).append(", ");
			}
			builder.deleteCharAt(builder.length()-1); builder.deleteCharAt(builder.length()-1);
		}
		builder.append("]");
		return builder.toString();
	}
	
	/**
	 * Indicates if the query should be run again for continuity
	 * @return true to continue, false otherwise
	 */
	public boolean shouldContinue() {
		final boolean cont = continuous && nextIndex != null && !isExpired() && !isExhausted() && cummulative < maxSize;
		if(cont) this.timeLimit=-1L;
		return cont;
	}
	
	/**
	 * Adds an internal context value
	 * @param key The key
	 * @param value The value
	 * @return this QueryContext
	 */
	public QueryContext addCtx(final String key, final Object value) {
		ctx.put(key, value);
		return this;
	}
	
	/**
	 * Clears the internal context
	 * @return this QueryContext
	 */
	public QueryContext clearCtx() {
		ctx.clear();
		return this;
	}
	
	/**
	 * Returns a copy of the internal context
	 * @return a copy of the internal context
	 */
	public Map<String, Object> getCtx() {
		return new LinkedHashMap<String, Object>(ctx);
	}
	
	public String debugContinue() {
		return new StringBuilder("\nQueryContext Continue State [")
			.append("\n\tContinuous:").append(continuous)
			.append("\n\tHas Next Index:").append(nextIndex != null)
			.append("\n\tNot Expired:").append(!isExpired())
			.append("\n\tNot Exhausted:").append(!isExhausted())
			.append("\n\tNot At Max:").append((cummulative < maxSize))
			.append("\n\t===============================\n\tWill Continue:").append(
					continuous && nextIndex != null && !isExpired() && !isExhausted() && cummulative < maxSize
			)
			.append("\n]")
			.toString();
	}

	/**
	 * Indicates if the query's full result set is exausted
	 * @return true if the query's full result set is exausted, false if more data is available
	 */
	public boolean isExhausted() {
		return exhausted;
	}
	
	/**
	 * Calculates the item count limit on the next call which is the lesser of:<ol>
	 * 	<li>The configured page size</li>
	 *  <li>The maximum item count minus the number of cummulative items already retrieved</li>
	 * </ol>
	 * @return the item count limit on the next call
	 */
	public int getNextMaxLimit() {
		return Math.min((continuous ? maxSize : pageSize), maxSize - cummulative);
	}

	/**
	 * Indicates if the query should be for a count only
	 * @param exhausted true to set the full result set as exhausted, false to reset
	 * @return this QueryContext
	 */
	public QueryContext setExhausted(boolean exhausted) {
		this.exhausted = exhausted;
		return this;
	}

	/**
	 * Returns the configured maximum cummulative number of items to be returned within this context
	 * @return the maxSize the configured maximum cummulative number of items
	 */
	public final int getMaxSize() {
		return maxSize;
	}

	/**
	 * Sets the configured maximum cummulative number of items to be returned within this context
	 * @param maxSize the maximum cummulative number of items
	 * @return this QueryContext
	 */
	public final QueryContext setMaxSize(int maxSize) {
		this.maxSize = maxSize;
		return this;
	}

	/**
	 * Returns the cummulative number of items retrieved within this context
	 * @return the cummulative number of items
	 */
	public final int getCummulative() {
		return cummulative;
	}

	/**
	 * Increments the cummulative by the passed amount
	 * @param cummulative the amount to increment by
	 * @return this QueryContext
	 */
	public final QueryContext incrementCummulative(int cummulative) {
		this.cummulative += cummulative;
		return this;
	}

	/**
	 * Returns the timeout on each submitted request in ms. 
	 * @return the timeout in ms.
	 */
	public final long getTimeout() {
		return timeout;
	}

	/**
	 * Sets the timeout on each submitted request in ms.
	 * @param timeout the timeout in ms.
	 * @return this QueryContext
	 */
	public final QueryContext setTimeout(long timeout) {
		this.timeout = timeout;
		return this;
	}

	/**
	 * Indicates if retrieved TSMetas should have the metric and tag UIDMetas populated. 
	 * See {@link MetaReader#readTSMetas(java.sql.ResultSet, boolean)}
	 * @return true to fully populate TSMetas, false otherwise
	 */
	public boolean isIncludeUIDs() {
		return includeUIDs;
	}

	/**
	 * Specifies if retrieved TSMetas should have the metric and tag UIDMetas populated. 
	 * See {@link MetaReader#readTSMetas(java.sql.ResultSet, boolean)}
	 * @param includeUIDs true to fully populate TSMetas, false otherwise
	 */
	public void setIncludeUIDs(boolean includeUIDs) {
		this.includeUIDs = includeUIDs;
	}

	/**
	 * Returns the timestamp at which the running asynch request will be expired 
	 * @return the expired timestamp
	 */
	@JsonIgnore
	public final long getTimeLimit() {
		return timeLimit;
	}
	
	/**
	 * Indicates if the last submitted request in this context has expired
	 * according to the start time and timeout. Also updates the elapsed time.
	 * Should be called when the async response arrives.
	 * @return true if the request has expired, false otherwise.
	 */
	public final boolean isExpired() {
		if(expired) return true;
		if(timeout < 1) return false;
		final long now = System.currentTimeMillis();
		try {
			if(timeLimit!=-1L) {				
				elapsed = now - timeLimit + timeout;
				log.debug("\n\t***************\n\tTime Limit: {}\n\tNow: {}\n\tTimeout: {}\n\tElapsed: {}\n\t***************\n", timeLimit, now, timeout, elapsed);
				boolean exp =  now > timeLimit;
				timeLimit = -1L;
				expired = exp;
				return exp;
			} 
			return false;
		} finally {
			if(continuous) {
				timeLimit = now + timeout;
			}
		}
	}
	

	/**
	 * Starts the expiration timeout clock.
	 * We might be disbling timeout, but we use this to capture the start time.
	 * @return this QueryContext
	 */
	public QueryContext startExpiry() {
		// TODO: SET START HERE
		addCtx("QStart", System.currentTimeMillis());
		if(timeout > 0) {
			if(this.timeLimit==-1L) {
				this.timeLimit = System.currentTimeMillis() + timeout;
				log.debug("\n\t**********************\n\tTimeLimit set with timeout [{}] to [{}]\n", timeout, timeLimit);
			}
		}
		return this;
	}
	
	public String reportElapsed() {
		final StringBuilder b = new StringBuilder("Query Phase Elapsed Times:");
		final Long start = (Long)ctx.get("QStart");
		final Long prepared = (Long)ctx.get("SQLPrepared");
		final Long executed = (Long)ctx.get("SQLExecuted");
		final Long iterated = (Long)ctx.get("SQLRSetIter");
		final Long flushed = (Long)ctx.get("StreamFlushed");
		if(start!=null && prepared!=null) {
			b.append("\n\tPrepared: ").append(prepared - start).append(" ms.");
		}
		if(prepared!=null && executed!=null) {
			b.append("\n\tExecuted: ").append(executed - prepared).append(" ms.");
		}
		if(executed!=null && iterated!=null) {
			b.append("\n\tIterated: ").append(iterated - executed).append(" ms.");
		}
		if(flushed!=null && iterated!=null) {
			b.append("\n\tFlushed: ").append(flushed - iterated).append(" ms.");
		}		
		return b.toString();
	}
	

	/**
	 * Returns the elapsed time of the last request in ms. 
	 * @return the elapsed time
	 */
	public final long getElapsed() {
		return elapsed;
	}

	/**
	 * Indicates if the query will be continuous, self re-executing until are pages are returned
	 * @return true if continuous, false if the caller will handle 
	 */
	public boolean isContinuous() {
		return continuous;
	}

	/**
	 * Sets the continuity of the query
	 * @param continuous true if continuous, false if the caller will handle 
	 * @return this QueryContext
	 */
	public QueryContext setContinuous(boolean continuous) {
		this.continuous = continuous;
		return this;
	}

	/**
	 * Returns the JSON seralization format name to use for serializing the response objects for this query context 
	 * @return the serializer format name
	 */
	public String getFormat() {
		return format;
	}

	/**
	 * Sets the JSON seralization format to use for serializing the response objects for this query context 
	 * @param serializer the serializer name to set
	 * @return this query context
	 */
	public QueryContext setFormat(final String serializer) {
		if(serializer==null || serializer.trim().isEmpty()) throw new IllegalArgumentException("The passed serialization name was null or empty");
		final String serName = serializer.trim().toUpperCase();
		try {
			TSDBTypeSerializer.valueOf(serName);
		} catch (Exception ex) {
			throw new IllegalArgumentException("The passed serialization name [" + serializer + "] was invalid. Valid names are:" + Arrays.toString(TSDBTypeSerializer.values()));
		}
		this.format = serName;
		return this;
	}
	
	/**
	 * Returns the context's currently configured JSON ObjectMapper
	 * @return the context's currently configured JSON ObjectMapper
	 */
	@JsonIgnore
	public ObjectMapper getMapper() {
		try {
			return TSDBTypeSerializer.valueOf(format).getMapper();
		} catch (Exception ex) {
			return TSDBTypeSerializer.DEFAULT.getMapper();
		}
	}

	/**
	 * Serializes this QueryContext to JSON and returns the byte array
	 * @return the byte array containing the JSON
	 */
	public byte[] toJSON() {
		return JSON.serializeToBytes(this);
	}


}

