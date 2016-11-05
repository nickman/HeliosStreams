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
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashSet;




/**
 * <p>Title: AbstractSubscriber</p>
 * <p>Description: Base class for subscriber implementations</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.webrpc.subpub.AbstractSubscriber</code></p>
 * @param <T> The types of events consumed by this subscriber
 */
public abstract class AbstractSubscriber<T> implements Subscriber<T> {
	/** The bitmask of the event types this subscriber is interested in */
	protected final int eventBitMask;
	/** The unique subscriber id */
	protected final String id;
	/** The subscriber logger */
	protected final Logger log;

	
	/** A set of registered listeners  */
	protected final Set<SubscriberEventListener> listeners = new NonBlockingHashSet<SubscriberEventListener>();
	
	
	/**
	 * Creates a new AbstractSubscriber
	 * @param id The subscriber id
	 * @param types the event types this subscriber is interested in
	 */
	protected AbstractSubscriber(final String id, final TSDBEventType...types) {
		this.id = id;
		eventBitMask = TSDBEventType.getMask(types);
		log = LogManager.getLogger(getClass().getName() + "." + id);
	}
	
	@Override
	public abstract void accept(Collection<T> events);
	
	/**
	 * {@inheritDoc}
	 * @see org.helios.tsdb.plugins.remoting.subpub.Subscriber#getSubscriberId()
	 */
	@Override
	public String getSubscriberId() {
		return id;
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see org.helios.tsdb.plugins.remoting.subpub.Subscriber#getEventBitMask()
	 */
	@Override
	public int getEventBitMask() {
		return eventBitMask;
	}



	/**
	 * {@inheritDoc}
	 * @see org.helios.tsdb.plugins.remoting.subpub.Subscriber#registerListener(org.helios.tsdb.plugins.remoting.subpub.SubscriberEventListener)
	 */
	@Override
	public void registerListener(SubscriberEventListener listener) {
		if(listener!=null) {
			listeners.add(listener);
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.helios.tsdb.plugins.remoting.subpub.Subscriber#removeListener(org.helios.tsdb.plugins.remoting.subpub.SubscriberEventListener)
	 */
	@Override
	public void removeListener(SubscriberEventListener listener) {
		if(listener!=null) {
			listeners.remove(listener);
		}		
	}
	
	
	/**
	 * Fires a disconnect event against all registered listeners
	 */
	protected void fireDisconnected() {
		for(SubscriberEventListener l: listeners) {
			l.onDisconnect(this);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof AbstractSubscriber))
			return false;
		AbstractSubscriber other = (AbstractSubscriber) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Subscriber [");
		builder.append("id:").append(id).append(", ")
		.append("type:").append(getClass().getSimpleName());		
		builder.append("]");
		return builder.toString();
	}


}
