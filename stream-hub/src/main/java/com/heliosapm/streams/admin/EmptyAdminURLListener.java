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
package com.heliosapm.streams.admin;

/**
 * <p>Title: EmptyAdminURLListener</p>
 * <p>Description: An empty concrete implementation of a {@link AdminURLListener} for selective extending</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.admin.EmptyAdminURLListener</code></p>
 */

public class EmptyAdminURLListener implements AdminURLListener {

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.admin.AdminURLListener#onAdminURL(java.lang.String)
	 */
	@Override
	public void onAdminURL(final String adminURL) {
		/* No Op */
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.admin.AdminURLListener#onAdminURLChanged(java.lang.String, java.lang.String)
	 */
	@Override
	public void onAdminURLChanged(final String priorAdminURL, final String newAdminURL) {
		/* No Op */
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.admin.AdminURLListener#onAdminURLRemoved(java.lang.String)
	 */
	@Override
	public void onAdminURLRemoved(final String priorAdminURL) {
		/* No Op */
	}

}
