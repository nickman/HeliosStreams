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
package com.heliosapm.streams.discovery;

/**
 * <p>Title: AdvertisedEndpointListener</p>
 * <p>Description: Defines a listener to be notified of new onlined AdvertisedEndpoints and disappeared offlined AdvertisedEndpoints</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.discovery.AdvertisedEndpointListener</code></p>
 */

public interface AdvertisedEndpointListener {
	/**
	 * Fired when an AdvertisedEndpoint comes on line
	 * @param endpoint the online AdvertisedEndpoint
	 */
	public void onOnlineAdvertisedEndpoint(final AdvertisedEndpoint endpoint);
	/**
	 * Fired when an AdvertisedEndpoint goes off line
	 * @param endpoint the offline AdvertisedEndpoint
	 */
	public void onOfflineAdvertisedEndpoint(final AdvertisedEndpoint endpoint);
}
