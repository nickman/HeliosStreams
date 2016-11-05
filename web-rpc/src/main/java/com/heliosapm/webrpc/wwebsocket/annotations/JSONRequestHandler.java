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
package com.heliosapm.webrpc.wwebsocket.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.heliosapm.webrpc.wwebsocket.RequestType;

/**
 * <p>Title: JSONRequestHandler</p>
 * <p>Description: Marks a method as a json web-rpc endpoint</p> 
 * <p>Annotated methods must implement the signature defined in {@literal JSONDataService#processRequest(org.json.JSONObject, org.jboss.netty.channel.Channel)}.</p>
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.webrpc.wwebsocket.annotations.JSONRequestHandler</code></p>
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface JSONRequestHandler {
	/**
	 * The name of the request handler which maps to the <b><code>op name</code></b> of a {@link JSONRequest}
	 */
	public String name();
	/**
	 * A description of the operation
	 */
	public String description() default "A JSON Request Operation";	

	/**
	 * The request type
	 */
	public RequestType type() default RequestType.REQUEST;

}
