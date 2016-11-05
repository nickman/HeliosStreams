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

import com.stumbleupon.async.Deferred;

import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.search.SearchQuery;

/**
 * <p>Title: TSDBSearchEvent</p>
 * <p>Description: Type specific spoofing for strongly typed async dispatchers like Guava EventBus.</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.webrpc.subpub.TSDBSearchEvent</code></p>
 */
public class TSDBSearchEvent extends TSDBEvent {
	/**
	 * Creates a new TSDBSearchEvent
	 */
	public TSDBSearchEvent() {
		super();
	}
	
	
	/**
	 * Creates a new TSDBSearchEvent from an event
	 * @param event the event to copy from
	 */
	public TSDBSearchEvent(TSDBEvent event) {
		annotation = event.annotation;
		deferred = event.deferred;
		doubleValue = event.doubleValue;		
		eventType = event.eventType;
		longValue = event.longValue;		
		metric = event.metric;
		searchQuery = event.searchQuery;
		tags = event.tags;		
		timestamp = event.timestamp;
		tsMeta = event.tsMeta;
		tsuid = event.tsuid;
		tsuidBytes = event.tsuidBytes;
		uidMeta = event.uidMeta;
	}
	
	

	
	/**
	 * {@inheritDoc}
	 * @see org.helios.tsdb.plugins.event.TSDBEvent#deleteAnnotation(net.opentsdb.meta.Annotation)
	 */
	@Override
	public TSDBSearchEvent deleteAnnotation(Annotation annotation) {
		super.deleteAnnotation(annotation);
		return this;
	}

	/**
	 * {@inheritDoc}
	 * @see org.helios.tsdb.plugins.event.TSDBEvent#indexAnnotation(net.opentsdb.meta.Annotation)
	 */
	@Override
	public TSDBSearchEvent indexAnnotation(Annotation annotation) {
		super.indexAnnotation(annotation);
		return this;
	}

	/**
	 * {@inheritDoc}
	 * @see org.helios.tsdb.plugins.event.TSDBEvent#deleteTSMeta(java.lang.String)
	 */
	@Override
	public TSDBSearchEvent deleteTSMeta(String tsuid) {
		super.deleteTSMeta(tsuid);
		return this;
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.helios.tsdb.plugins.event.TSDBEvent#indexTSMeta(net.opentsdb.meta.TSMeta)
	 */
	@Override
	public TSDBSearchEvent indexTSMeta(TSMeta tsMeta) {
		super.indexTSMeta(tsMeta);
		return this;
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.helios.tsdb.plugins.event.TSDBEvent#deleteUIDMeta(net.opentsdb.meta.UIDMeta)
	 */
	@Override
	public TSDBSearchEvent deleteUIDMeta(UIDMeta uidMeta) {
		super.deleteUIDMeta(uidMeta);
		return this;
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.helios.tsdb.plugins.event.TSDBEvent#indexUIDMeta(net.opentsdb.meta.UIDMeta)
	 */
	@Override
	public TSDBSearchEvent indexUIDMeta(UIDMeta uidMeta) {
		super.indexUIDMeta(uidMeta);
		return this;
	}
	
	/**
	 * Prepares and returns a search event
	 * @param searchQuery The query to create an event for
	 * @param toComplete The deferred to complete when the query execution completes (or errors out)
	 * @return the loaded event
	 */
	public TSDBSearchEvent executeQueryEvent(SearchQuery searchQuery, Deferred<SearchQuery> toComplete) {
		 super.executeQuery(searchQuery, toComplete);
		 return this;
    }

	

}