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
package com.heliosapm.webrpc.json;

/**
 * <p>Title: JSONException</p>
 * <p>Description: Generalized runtime exception for JSON serialization failures</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.webrpc.json.JSONException</code></p>
 */

public class JSONException extends RuntimeException {

	/**  */
	private static final long serialVersionUID = -856918010110469450L;

	/**
	 * Creates a new JSONException
	 */
	public JSONException() {

	}

	/**
	 * Creates a new JSONException
	 * @param message The exception message
	 */
	public JSONException(String message) {
		super(message);
	}

	/**
	 * Creates a new JSONException
	 * @param cause The underlying exception cause
	 */
	public JSONException(Throwable cause) {
		super(cause);
	}

	/**
	 * Creates a new JSONException
	 * @param message The exception message
	 * @param cause The underlying exception cause
	 */
	public JSONException(String message, Throwable cause) {
		super(message, cause);
	}

}

