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

/**
 * <p>Title: RequestType</p>
 * <p>Description: Defines the incoming request type. Intended to provide the client with the meta-data
 * so it can setup pending request handling correctly.</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>org.helios.tsdb.plugins.remoting.json.RequestType</code></p>
 */

public enum RequestType {
	/** Simple request response */
	REQUEST("req"),
	/** Compound request response with potentially multiple responses */
	MREQUEST("mreq"),	
	/** Initiates a subscription */
	SUBSCRIBE("sub"),
	/** Cancels a subscription */
	UNSUBSCRIBE("xsub");
	
	private RequestType(final String code) {
		this.code = code;
	}
	
	/** The request type code */
	public final String code;
}
