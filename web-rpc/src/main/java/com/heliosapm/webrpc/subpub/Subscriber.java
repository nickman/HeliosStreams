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
package com.heliosapm.webrpc.subpub;

import java.util.Collection;



/**
 * <p>Title: Subscriber</p>
 * <p>Description: Defines a class that subscribes to TSDBEvents</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.webrpc.subpub.Subscriber</code></p>
 * @param <T> The types of events consumed by this subscriber
 */

public interface Subscriber<T> {
	/**
	 * Returns the selective bitmask for the types of events this subscriber is interested in
	 * @return the TSDBEvent selective bitmask
	 * @see  org.helios.tsdb.plugins.event.TSDBEventType#getMask(TSDBEventType...)
	 */
	public int getEventBitMask();
	
	/**
	 * Delivers a TSDBEvent to the subscriber
	 * @param events The events to deliver
	 */
	public void accept(Collection<T> events);
	
	/**
	 * Registers a listener that should be notified of subscriber events
	 * @param listener The listener to register
	 */
	public void registerListener(SubscriberEventListener listener);
	
	/**
	 * Unregisters a listener
	 * @param listener The listener to unregister
	 */
	public void removeListener(SubscriberEventListener listener);
	
	/**
	 * Returns a unique identifier for this subscriber
	 * @return the unique identifier
	 */
	public String getSubscriberId();
	
}
