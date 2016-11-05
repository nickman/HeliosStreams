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
 * <p>Title: ResponseType</p>
 * <p>Description: Enumerates the JSON response types</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>org.helios.tsdb.plugins.remoting.json.ResponseType</code></p>
 */

public enum ResponseType {
	/** Response flag for an error message */
	ERR("err"),  // undefined continuation
	/** Response flag for a request response */
	RESP("resp"), // one time response
	/** Response flag for a request response with a finite number of responses */
	MRESP("mresp"), // n-times response ending with a final event
	/** Response flag for a request response with a finite number of responses */
	XMRESP("xmresp"), // final event on an MRESP sequence	
	/** Response flag for a subscription event delivery */
	SUB("sub"),  // infinite response
	/** Response flag for a subscription start confirm */
	SUB_STARTED("subst"),  // starts a sequence of infinite responses 
	/** Response flag for a subscription stop notification */
	SUB_STOPPED("xmsub"), // ends a sequence of infinite responses
	/** Response flag for a growl */
	GROWL("growl");  // infinite response, no starter, no ender
	
	
	private ResponseType(final String code) {
		this.code = code;
	}
	
	/** The code for the response type */
	public final String code;

}
