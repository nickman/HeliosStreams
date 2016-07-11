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
package com.heliosapm.streams.admin.nodes;

/**
 * <p>Title: ContentType</p>
 * <p>Description: Enumerates the different content types served by the {@link NodeConfigurationServer}</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.admin.nodes.ContentType</code></p>
 */

public enum ContentType {
	/** App configuration properties */
	CONFIG(".properties", "text/x-java-properties"),
	/** App execution java archive */
	JAR(".jar", "application/java-archive"),
	/** App execution java archive meta-data */
	JARMETA(".json", "application/json");
	
	private ContentType(final String extension, final String mimeType) {
		this.extension = extension;
		this.mimeType = mimeType;
	}
	
	/** The extension of this content type */
	public final String extension;
	/** The mime type of this content type */
	public final String mimeType;
	
}
