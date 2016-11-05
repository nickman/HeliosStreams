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
package com.heliosapm.webrpc.jsonservice;

import java.util.Collection;

import com.fasterxml.jackson.core.JsonGenerator;
import com.heliosapm.webrpc.subpub.AbstractSubscriber;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * <p>Title: JSONSubscriber</p>
 * <p>Description: Represents a proxy to a JSON service subscriber</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.webrpc.jsonservice.JSONSubscriber</code></p>
 * @param <T> The assumed type of the object types that will be delivered to this subscriber
 */

public class JSONSubscriber<T> extends AbstractSubscriber<T> implements GenericFutureListener<Future<? super Void>> {
	/** The JSONRequest initiating the subscription */
	protected final JSONRequest request;
	
	
	/**
	 * Creates a new JSONSubscriber
	 * @param request The JSONRequest initiating the subscription
	 * @param types The event types we're interested in
	 */
	public JSONSubscriber(final JSONRequest request, final com.heliosapm.webrpc.subpub.TSDBEventType... types) {
		super(JSONSubscriber.class.getSimpleName() + ":" + request.channel.id().asShortText(), types);
		this.request = request;
		this.request.channel.closeFuture().addListener(this);
	}

	/**
	 * Determines what the Subscriber id would be (or is) for the passed JSONRequest
	 * @param request The request to get the Subscriber id for
	 * @return the Subscriber id
	 */
	public static String getId(final JSONRequest request) {
		return JSONSubscriber.class.getSimpleName() + ":" + request.channel.id().asShortText();
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.helios.tsdb.plugins.remoting.subpub.AbstractSubscriber#accept(java.util.Collection)
	 */
	@Override
	public void accept(Collection<T> events) {		
		final JSONResponse response = request.response(ResponseType.SUB);
		try {
			JsonGenerator jgen = response.writeHeader(true);
			jgen.writeFieldName("msg");
			jgen.writeStartArray();
			for(T t: events) {
				jgen.writeObject(t);
			}
			jgen.writeEndArray();
			response.closeGenerator();
		} catch (Exception ex) {
			log.error("Failed to write out accepted events", ex);
		}
	}

	@Override
	public void operationComplete(final Future<? super Void> f) throws Exception {
		
	}

}
