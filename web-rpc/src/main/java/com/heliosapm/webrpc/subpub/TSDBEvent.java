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
package com.heliosapm.webrpc.subpub;

import java.util.LinkedHashMap;
import java.util.Map;

import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.search.SearchQuery;
import net.opentsdb.uid.UniqueId;

import com.lmax.disruptor.EventFactory;
import com.stumbleupon.async.Deferred;

/**
 * <p>Title: TSDBEvent</p>
 * <p>Description: Encapsulates an OpenTSDB callback to a plugin</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.webrpc.subpub.TSDBEvent</code></p>
 */

public class TSDBEvent  {
	/** The event type */
	public TSDBEventType eventType;
	/** The metric name for data point publications */
	public String metric = null;
	/** The timestamp for data point publications */
	public long timestamp = -1;
	/** The value for long based data point publications */
	public long longValue = -1;
	/** The value for double based data point publications */
	public double doubleValue = -1;
	/** The annotation to index for annotation indexing or deletion events */
	public Annotation annotation = null;
	
	/** The datapoint tags for data point publications */
	public Map<String,String> tags = null;
	/** The tsuid in byte format for data point publications */
	public byte[] tsuidBytes = null;
	/** The tsuid in string format for tsuid deletion events */
	public String tsuid = null;
	/** The UIDMeta for UIDMeta indexing and deletion events */
	public UIDMeta uidMeta = null;
	/** The TSMeta for TSMeta indexing events */
	public TSMeta tsMeta = null;
	/** The search query for search events */
	public SearchQuery searchQuery = null;
	/** The deferred search query result for search events */
	public Deferred<SearchQuery> deferred;
	
	/** Indicates that the associated object should be synced to store through the TSDB */
	public boolean synToStore = false;
	
	/**
	 * Creates a new TSDBEvent
	 */
	protected TSDBEvent() {

	}
	
	/**
	 * Returns this event instance as a search event
	 * @return this event instance as a search event
	 */
	public TSDBSearchEvent asSearchEvent() {
		if(eventType!=TSDBEventType.SEARCH) {
			throw new RuntimeException("Cannot cast this event to Search. Event Type is [" + eventType + "] Class is [" + getClass().getName() + "]");
		}
		return new TSDBSearchEvent(this);
	}
	
	/**
	 * Nulls out all the fields.
	 */
	public void reset() {
		annotation = null;
		deferred = null;
		doubleValue = -1;		
		eventType = null;
		longValue = -1;		
		metric = null;
		searchQuery = null;
		synToStore = false;
		tags = null;		
		timestamp = -1;
		tsMeta = null;
		tsuid = null;
		tsuidBytes = null;
		uidMeta = null;
		
	}
	
	 /** The event factory for TSDBEvents */
	public final static EventFactory<TSDBEvent> EVENT_FACTORY = new EventFactory<TSDBEvent>() {
		 @Override
		public TSDBEvent newInstance() {
			 return new TSDBEvent();
		 }
	 };
	 
	/**
	 * Loads this event for a search event
	 * @param searchQuery The search query to translate
	 * @param toComplete The deferred to complete when the query execution completes (or errors out)
	 */
	public void executeQuery(SearchQuery searchQuery, Deferred<SearchQuery> toComplete) {
		 this.eventType = TSDBEventType.SEARCH;
		 this.searchQuery = searchQuery;
		 this.deferred = toComplete;
	 }
	 
	
	/**
	 * Loads this event for an annotation deletion
	 * @param annotation The annotation to delete
	 * @return the loaded event
	 */
	public TSDBEvent deleteAnnotation(Annotation annotation) {
		this.eventType = TSDBEventType.ANNOTATION_DELETE;
		this.annotation = annotation;
		return this;
	}

	/**
	 * Loads this event for an annotation indexing
	 * @param annotation The annotation to index
	 * @return the loaded event
	 */
	public TSDBEvent indexAnnotation(Annotation annotation) {
		this.eventType = TSDBEventType.ANNOTATION_INDEX;
		this.annotation = annotation;
		return this;
	}

	/**
	 * Loads this event for a TSMeta deletion
	 * @param tsuid The tsuid name to delete
	 * @return the loaded event
	 */
	public TSDBEvent deleteTSMeta(String tsuid) {
		this.eventType = TSDBEventType.TSMETA_DELETE;
		this.tsuid = tsuid;	
		return this;
	}
	
	/**
	 * Loads this event for a TSMeta indexing
	 * @param tsMeta The tsuid to index
	 * @return the loaded event
	 */
	public TSDBEvent indexTSMeta(TSMeta tsMeta) {
		this.eventType = TSDBEventType.TSMETA_INDEX;
		this.tsMeta = tsMeta;
		return this;
	}
	
	/**
	 * Loads this event for a UIDMeta deletion
	 * @param uidMeta The UIDMeta to delete
	 * @return the loaded event
	 */
	public TSDBEvent deleteUIDMeta(UIDMeta uidMeta) {
		this.eventType = TSDBEventType.UIDMETA_DELETE;
		this.uidMeta = uidMeta;	
		return this;
	}
	
	/**
	 * Loads this event for a UIDMeta indexing
	 * @param uidMeta The UIDMeta to index
	 * @return the loaded event
	 */
	public TSDBEvent indexUIDMeta(UIDMeta uidMeta) {
		this.eventType = TSDBEventType.UIDMETA_INDEX;
		this.uidMeta = uidMeta;	
		return this;
	}
	
	/**
	 * Loads this event for a double value data point publication
	 * @param metric The name of the metric associated with the data point
	 * @param timestamp Timestamp as a Unix epoch in seconds or milliseconds (depending on the TSD's configuration)
	 * @param value Value for the data point
	 * @param tags The metric tags
	 * @param tsuid Time series UID for the value
	 * @return the loaded event
	 */
	public TSDBEvent publishDataPoint(String metric, long timestamp, double value, Map<String,String> tags, byte[] tsuid) {
		this.eventType = TSDBEventType.DPOINT_DOUBLE;
		this.metric = metric;
		this.timestamp = timestamp;
		this.doubleValue = value;
		this.tags = new LinkedHashMap<String, String>(tags);
		this.tsuidBytes = tsuid;
		this.tsuid = UniqueId.uidToString(tsuid);
		return this;
	}
	
	/**
	 * Loads this event for a long value data point publication
	 * @param metric The name of the metric associated with the data point
	 * @param timestamp Timestamp as a Unix epoch in seconds or milliseconds (depending on the TSD's configuration)
	 * @param value Value for the data point
	 * @param tags The metric tags
	 * @param tsuid Time series UID for the value
	 * @return the loaded event
	 */
	public TSDBEvent publishDataPoint(String metric, long timestamp, long value, Map<String,String> tags, byte[] tsuid) {
		this.eventType = TSDBEventType.DPOINT_LONG;
		this.metric = metric;
		this.timestamp = timestamp;
		this.longValue = value;
		this.tags = new LinkedHashMap<String, String>(tags);
		this.tsuidBytes = tsuid;
		this.tsuid = UniqueId.uidToString(tsuid);
		return this;
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		if(eventType==null) return "Empty TSDBEvent";
		StringBuilder b = new StringBuilder("TSDBEvent ").append(eventType.name()).append("[");
		switch(eventType) {
		case ANNOTATION_DELETE: 
		case ANNOTATION_INDEX:
			b.append(annotation);
			break;
		case DPOINT_DOUBLE:
			b.append(doubleValue);
			break;
		case DPOINT_LONG:
			b.append(longValue);
			break;
		case SEARCH:
			b.append(searchQuery);
			break;
		case TSMETA_DELETE:
			b.append(tsuid);
			break;
		case TSMETA_INDEX:
			b.append(tsMeta);
			break;			
		case UIDMETA_DELETE:
		case UIDMETA_INDEX:
			b.append(uidMeta);
			break;
		default:
			break;			
		}
		return b.append("]").toString();
	}


}
