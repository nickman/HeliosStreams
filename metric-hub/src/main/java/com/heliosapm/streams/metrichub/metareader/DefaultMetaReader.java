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
package com.heliosapm.streams.metrichub.metareader;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.heliosapm.streams.json.JSONException;
import com.heliosapm.streams.json.JSONOps;
import com.heliosapm.streams.metrichub.MetaReader;
import com.heliosapm.streams.metrichub.impl.AbstractIndexProvidingIterator;
import com.heliosapm.streams.metrichub.impl.IndexProvidingIterator;
import com.heliosapm.streams.sqlbinder.SQLWorker;

import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;

/**
 * <p>Title: DefaultMetaReader</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrichub.metareader.DefaultMetaReader</code></p>
 */

public class DefaultMetaReader implements MetaReader {
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	
	/** The sql worker in case we need to make additional calls to the DB */
	protected final SQLWorker sqlWorker;
	
	/** The TSMeta class tags field so we can set the tags reflectively */
	private static final Field TSMETA_TAGS_FIELD;
	/** The TSMeta class metric field so we can set the metric reflectively */
	private static final Field TSMETA_METRIC_FIELD;
	
	/** The SQL to retrieve the tags for a TSMeta */
	public static final String TSMETA_TAGS_SQL = "SELECT 'TAGK' as TAG_TYPE, F.PORDER, K.* FROM TSD_TAGK K, TSD_FQN_TAGPAIR F, TSD_TAGPAIR P WHERE F.XUID = P.XUID AND K.XUID = P.TAGK AND F.FQNID = ? " + 
												 "UNION ALL " + 
												 "SELECT 'TAGV' as TAG_TYPE, F.PORDER, V.* FROM TSD_TAGV V, TSD_FQN_TAGPAIR F, TSD_TAGPAIR P WHERE F.XUID = P.XUID AND V.XUID = P.TAGV AND F.FQNID = ? " + 
												 "ORDER BY 2";
	
	
	/**
	 * Creates a new DefaultMetaReader
	 * @param sqlWorker a sql worker in case we need to make additional calls to the DB
	 */
	public DefaultMetaReader(final SQLWorker sqlWorker) {
		this.sqlWorker = sqlWorker;

	}
	
	/** A JSON representation of an empty map */
	public static final String EMPTY_MAP = "{}";

	static {
		try {
			TSMETA_TAGS_FIELD = TSMeta.class.getDeclaredField("tags");
			TSMETA_TAGS_FIELD.setAccessible(true);
			TSMETA_METRIC_FIELD = TSMeta.class.getDeclaredField("metric");
			TSMETA_METRIC_FIELD.setAccessible(true);
		} catch (Exception ex) {
			throw new RuntimeException("Failed to read TSMeta fields", ex);
		}
	}
	
	
	
	
	/**
	 * Validates that the named value is not null, or if a {@link CharSequence} insance, 
	 * that it is not empty
	 * @param name The name of the value being validate
	 * @param t The object to validate
	 * @return The validated object
	 * @throws JSONException  thrown if the object fails validation
	 */
	protected static <T> T nvl(String name, T t) throws JSONException {
		if(
			t==null ||
			(t instanceof CharSequence && ((CharSequence)t).toString().trim().isEmpty())
		) throw new JSONException("The passed [" + name + "] was null or empty");
		return t;
	}
	
	/**
	 * Validates that the named value is not null, or if a {@link CharSequence} insance, 
	 * that it is not empty and returns it as a string.
	 * @param name The name of the value being validate
	 * @param t The object to validate
	 * @return The validated object as a string
	 * @throws JSONException  thrown if the object fails validation
	 */
	protected static String nvls(String name, Object t) throws JSONException {
		return nvl(name, t).toString().trim();
	}
	
	/**
	 * Converts a millisecond based timestamp to a unix seconds based timestamp
	 * @param time The millisecond timestamp to convert
	 * @return a unix timestamp
	 */
	public static long mstou(long time) {
		return TimeUnit.SECONDS.convert(time, TimeUnit.MILLISECONDS);
	}
	
	/**
	 * Converts a unix second based timestamp to a long millisecond based timestamp
	 * @param time The unix timestamp to convert
	 * @return a long millisecond timestamp
	 */
	public static long utoms(long time) {
		return TimeUnit.MILLISECONDS.convert(time, TimeUnit.SECONDS);
	}
	

	

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetaReader#readUIDMetas(java.sql.ResultSet, java.lang.String)
	 */
	@Override
	public List<UIDMeta> readUIDMetas(final ResultSet rset, final String uidType) {
		return readUIDMetas(rset, (uidType==null || uidType.trim().isEmpty()) ? null : UniqueIdType.valueOf(uidType.trim().toUpperCase()));
	}
	
	/**
	 * Parses the passed JSON source and returns as a map
	 * @param source The JSON source to pass
	 * @return the parsed map
	 */
	public static Map<String, String> readMap(String source) {
		return JSONOps.parseToObject(nvl("JSON Source", source).toString().trim(), Map.class);
	}
	
	

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetaReader#readUIDMetas(java.sql.ResultSet, net.opentsdb.uid.UniqueId.UniqueIdType)
	 */
	@Override
	public List<UIDMeta> readUIDMetas(final ResultSet rset, final UniqueIdType uidType) {
		if(rset==null) throw new IllegalArgumentException("The passed result set was null");
		List<UIDMeta> uidMetas = new ArrayList<UIDMeta>();
		
		try {
			while(rset.next()) {
				UniqueIdType utype = null;
				if(uidType!=null) {
					utype = uidType;
				} else {
					utype = UniqueIdType.valueOf(rset.getString("TAG_TYPE"));
				}
				UIDMeta meta = new UIDMeta(utype, UniqueId.stringToUid(rset.getString("XUID")), rset.getString("NAME"));
				meta.setCreated(mstou(rset.getTimestamp("CREATED").getTime()));
				String mapStr = rset.getString("CUSTOM");
				if(mapStr!=null) {
					meta.setCustom((HashMap<String, String>) readMap(mapStr));
				}
				meta.setDescription(rset.getString("DESCRIPTION"));
				meta.setNotes(rset.getString("NOTES"));
				meta.setDisplayName(rset.getString("DISPLAY_NAME"));
				uidMetas.add(meta);
			}
		} catch (Exception ex) {
			throw new RuntimeException("Failed to read UIDMetas from ResultSet", ex);
		}
		return uidMetas;
	}
	
	/**
	 * Builds a UIDMeta from the current row in the passed result set
	 * @param rset The result set to read from
	 * @param type THe UIDMeta type to build
	 * @return the built UIDMeta
	 */
	private UIDMeta buildUIDMeta(final ResultSet rset, final UniqueIdType type) {
		try {
			UIDMeta meta = new UIDMeta(type, UniqueId.stringToUid(rset.getString("XUID")), rset.getString("NAME"));
			meta.setCreated(mstou(rset.getTimestamp("CREATED").getTime()));
			String mapStr = rset.getString("CUSTOM");
			if(mapStr!=null) {
				meta.setCustom((HashMap<String, String>) readMap(mapStr));
			}
			meta.setDescription(rset.getString("DESCRIPTION"));
			meta.setNotes(rset.getString("NOTES"));
			meta.setDisplayName(rset.getString("DISPLAY_NAME"));
			return meta;
		} catch (Exception ex) {
			throw new RuntimeException("Failed to build UIDMeta of type [" + type + "]", ex);
		}
	}
	

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetaReader#readTSMetas(java.sql.ResultSet)
	 */
	@Override
	public List<TSMeta> readTSMetas(final ResultSet rset) {
		return readTSMetas(rset, false);
	}
	
	/**
	 * Deep loads the metric and tag UIDs into the passed TSMeta
	 * @param conn The connection
	 * @param tsMeta The TSMeta to load
	 * @param metricUid The TSMeta's metric UID
	 * @param fqnid The TSMeta's DB PK
	 *	TODO: this badly needs a cache.
	 */
	protected void loadUIDs(final Connection conn, final TSMeta tsMeta, final String metricUid, final long fqnid) {
		try {
			final ResultSet rset = sqlWorker.executeQuery(conn, "SELECT * FROM TSD_METRIC WHERE XUID = ?", true, metricUid);
			final UIDMeta metric = readUIDMetas(rset, UniqueIdType.METRIC).iterator().next();
			rset.close();
			final ArrayList<UIDMeta> tags = (ArrayList<UIDMeta>)readUIDMetas(sqlWorker.executeQuery(conn, TSMETA_TAGS_SQL, true, fqnid, fqnid), "");
			setUIDs(tsMeta, tags, metric);
		} catch (Exception ex) {
			throw new RuntimeException("Failed to load UIDs for TSMeta [" + tsMeta + "]", ex);
		}
	}
	

	/**
	 * Builds a TSMeta from the current row in the passed result set
	 * @param rset The result set to read from
	 * @param includeUIDs true to load UIDs, false otherwise
	 * @return the built TSMeta
	 */
	protected TSMeta buildTSMeta(final ResultSet rset, final boolean includeUIDs) {
		try {
			TSMeta meta = new TSMeta(UniqueId.stringToUid(rset.getString("TSUID")), mstou(rset.getTimestamp("CREATED").getTime()));
			String mapStr = rset.getString("CUSTOM");
			if(mapStr!=null) {
				meta.setCustom((HashMap<String, String>) readMap(mapStr));
			}
			meta.setDescription(rset.getString("DESCRIPTION"));
			meta.setNotes(rset.getString("NOTES"));
			meta.setDisplayName(rset.getString("DISPLAY_NAME"));
			meta.setDataType(rset.getString("DATA_TYPE"));
			meta.setMax(rset.getDouble("MAX_VALUE"));
			meta.setMin(rset.getDouble("MIN_VALUE"));
			meta.setRetention(rset.getInt("RETENTION"));
			meta.setUnits(rset.getString("UNITS"));
			final long fqnId = rset.getLong("FQNID");
			if(includeUIDs) {
				loadUIDs(
						rset.isClosed() ? null : rset.getStatement().getConnection(), 
							meta, rset.getString("METRIC_UID"), fqnId);
			}
			return meta;
		} catch (Exception ex) {
			throw new RuntimeException("Failed to read TSMetas from ResultSet", ex);
		}
	}
	

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetaReader#readTSMetas(java.sql.ResultSet, boolean)
	 */
	@Override
	public List<TSMeta> readTSMetas(final ResultSet rset, final boolean includeUIDs) {
		if(rset==null) throw new IllegalArgumentException("The passed result set was null");
		List<TSMeta> tsMetas = new ArrayList<TSMeta>(128);
		try {
			while(rset.next()) {
				tsMetas.add(buildTSMeta(rset, includeUIDs));
			}
		} catch (Exception ex) {
			throw new RuntimeException("Failed to read TSMetas from ResultSet", ex);
		}
		return tsMetas;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetaReader#iterateTSMetas(java.sql.ResultSet)
	 */
	@Override
	public IndexProvidingIterator<TSMeta> iterateTSMetas(final ResultSet rset) {
		return iterateTSMetas(rset, false);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetaReader#iterateTSMetas(java.sql.ResultSet, boolean)
	 */
	@Override
	public IndexProvidingIterator<TSMeta> iterateTSMetas(final ResultSet rset, final boolean includeUIDs) {
		return new AbstractIndexProvidingIterator<TSMeta>(rset) {
			@Override
			protected TSMeta build() {
				try {
					return buildTSMeta(rset, includeUIDs);
				} catch (Exception ex) {
					throw new RuntimeException("Failed to build TSMeta", ex);
				}
			}
			@Override
			protected Object getIndex(TSMeta t) {
				return t.getTSUID();
			}
		};
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetaReader#iterateUIDMetas(java.sql.ResultSet, net.opentsdb.uid.UniqueId.UniqueIdType)
	 */
	@Override
	public IndexProvidingIterator<UIDMeta> iterateUIDMetas(final ResultSet rset, final UniqueIdType uidType) {
		return new AbstractIndexProvidingIterator<UIDMeta>(rset) {

			@Override
			protected UIDMeta build() {
				return buildUIDMeta(rset, uidType);
			}

			@Override
			protected Object getIndex(final UIDMeta t) {
				return t.getUID();
			}
		};
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrichub.MetaReader#readAnnotations(java.sql.ResultSet)
	 */
	@Override
	public List<Annotation> readAnnotations(final ResultSet rset) {
		if(rset==null) throw new IllegalArgumentException("The passed result set was null");
		List<Annotation> annotations = new ArrayList<Annotation>();
		try {
			ResultSetMetaData rsmd = rset.getMetaData();
			boolean hasTsUid = false;
			String tsUidColName = null;
			int colCount = rsmd.getColumnCount();
			for(int i = 0; i < colCount; i++) {
				if(rsmd.getColumnName(i+1).equalsIgnoreCase("TSUID")) {
					tsUidColName = rsmd.getColumnName(i+1);
					hasTsUid = true;
					break;
				}
			}
			
			while(rset.next()) {
				Annotation meta = new Annotation();
				meta.setDescription(rset.getString("DESCRIPTION"));
				meta.setNotes(rset.getString("NOTES"));
				if(hasTsUid) {
					meta.setTSUID(rset.getString(tsUidColName));
				} else {
					long fqnId = rset.getLong("FQNID");
					meta.setTSUID(getTSUIDForFQNId(fqnId));
				}
				meta.setStartTime(mstou(rset.getTimestamp("START_TIME").getTime()));
				Timestamp ts = rset.getTimestamp("END_TIME");
				if(ts!=null) {
					meta.setEndTime(mstou(ts.getTime()));
				}
				String mapStr = rset.getString("CUSTOM");
				if(mapStr!=null) {
					meta.setCustom((HashMap<String, String>) readMap(mapStr));
				}
				
				annotations.add(meta);
			}
		} catch (Exception ex) {
			throw new RuntimeException("Failed to read Annotations from ResultSet", ex);
		}
		return annotations;		
	}
	
	/**
	 * Returns the TSUID for the passed FQNID
	 * @param fqnId The FQNID of the TSMeta to get the TSUID for
	 * @return the TSUID
	 */
	public String getTSUIDForFQNId(long fqnId) {
		return sqlWorker.sqlForString("SELECT TSUID from TSD_TSMETA WHERE FQNID = ?", fqnId);
	}

	
	
	/**
	 * Reflectively sets the tags and metric on the passed TSMeta
	 * @param tsMeta The TSMeta to update
	 * @param tags The tags to set
	 * @param metric The metric to update
	 * @return The updated TSMeta
	 * FIXME:  implement a new TSMeta pojo so we don't have to do this.
	 */
	protected static TSMeta setUIDs(TSMeta tsMeta, ArrayList<UIDMeta> tags, UIDMeta metric) {
		setTSMetaTags(tsMeta, tags);
		setTSMetaMetric(tsMeta, metric);
		return tsMeta;
	}
	
	/**
	 * Reflectively sets the metric on the passed TSMeta
	 * @param tsMeta The TSMeta to set the metric on
	 * @param metric The metric UIDMeta to set
	 * @return The updated TSMeta
	 */
	protected static TSMeta setTSMetaMetric(TSMeta tsMeta, UIDMeta metric) {
		try {
			TSMETA_METRIC_FIELD.set(tsMeta, metric);
			return tsMeta;
		} catch (Exception ex) {
			throw new RuntimeException("Failed to set metric on TSMeta [" + tsMeta + "]", ex); 
		}
	}
	
	/**
	 * Reflectively sets the tags on the passed TSMeta
	 * @param tsMeta The TSMeta to set the tags on
	 * @param tags The tags to set
	 * @return The updated TSMeta
	 */
	protected static TSMeta setTSMetaTags(TSMeta tsMeta, ArrayList<UIDMeta> tags) {
		try {
			TSMETA_TAGS_FIELD.set(tsMeta, tags);
			return tsMeta;
		} catch (Exception ex) {
			throw new RuntimeException("Failed to set tags on TSMeta [" + tsMeta + "]", ex); 
		}
	}
	
	
	
	
	

}
