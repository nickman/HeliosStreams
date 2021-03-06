/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.heliosapm.streams.common.zoo;

/**
 * <p>Title: AdminURLListener</p>
 * <p>Description: Listens on AdminURL events in the admin finder</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.common.zoo.AdminURLListener</code></p>
 */

public interface AdminURLListener {
	
	/**
	 * Called when an adminURL is first discovered
	 * @param adminURL the adminURL
	 */
	public void onAdminURL(String adminURL);
	
	/**
	 * Called when an adminURL change is detected
	 * @param priorAdminURL The prior adminURL
	 * @param newAdminURL The new adminURL
	 */
	public void onAdminURLChanged(String priorAdminURL, String newAdminURL);
	
	/**
	 * Called when the current adminURL is removed
	 * @param priorAdminURL The prior adminURL
	 */
	public void onAdminURLRemoved(String priorAdminURL);
}
