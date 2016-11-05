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

import io.netty.channel.Channel;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * <p>Title: JSONSubConfirm</p>
 * <p>Description: A specialized JSONResponse for confirming a caller's subscription request</p>
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><b><code>org.helios.tsdb.plugins.remoting.json.JSONSubConfirm</code></b>
 */

public class JSONSubConfirm extends JSONResponse {
	/** The subscription key that uniquely identified the subscription from the client's perspective */
	@JsonProperty("subkey")
	protected final String subKey;


	/**
	 * Creates a new JsonSubConfirm
	 * @param reRequestId The client provided request ID that this response is being sent for
	 * @param type The type flag. Currently "err" for an error message, "resp" for a response, "sub" for subcription event
	 * @param subKey The subscription key that uniquely identified the subscription from the client's perspective
	 * @param channel The channel the sub-confirm will be written to
	 * @param request The parent request
	 */
	public JSONSubConfirm(long reRequestId, ResponseType type, String subKey, Channel channel, JSONRequest request) {
		super(reRequestId, type, channel, request);
		this.subKey = subKey;
	}

}
