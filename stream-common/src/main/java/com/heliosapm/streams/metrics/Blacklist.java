/**
 * Helios, OpenSource Monitoring
 * Brought to you by the Helios Development Group
 *
 * Copyright 2016, Helios Development Group and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org. 
 *
 */
package com.heliosapm.streams.metrics;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanNotificationInfo;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;

import org.cliffc.high_scale_lib.NonBlockingHashMap;

import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.jmx.SharedNotificationExecutor;

import jsr166e.AccumulatingLongAdder;
import jsr166e.LongAdder;

/**
 * <p>Title: Blacklist</p>
 * <p>Description: Tracks invalid metric names and a count of the instances of each</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.Blacklist</code></p>
 */

public class Blacklist extends NotificationBroadcasterSupport implements BlacklistMBean {
	/** The singleton instance */
	private static volatile Blacklist instance = null;
	/** The singleton instance ctor lock */
	private static final Object lock = new Object();
	/** Placeholder for blacklist adds */
	private static final LongAdder PLACEHOLDER = new LongAdder();

	private static final MBeanNotificationInfo[] INFOS = new MBeanNotificationInfo[] {
			new MBeanNotificationInfo(new String[]{NOTIF_NEW_BLACKLIST}, Notification.class.getName(), "Notification emitted when a new metric key is blacklisted")
	};
	
	/** A set of blacklisted metric keys */
	protected final NonBlockingHashMap<String, LongAdder> blackListed = new NonBlockingHashMap<String, LongAdder>(256);
	/** The total number of blacklisted metrics seen */
	protected final LongAdder blackListInstanceCount = new LongAdder();
	/** Notif serial number */
	protected final AtomicLong notifSerial = new AtomicLong(0);
	
	
	/**
	 * Acquires and returns the singleton instance
	 * @return the singleton instance
	 */
	public static Blacklist getInstance() {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					instance = new Blacklist();
				}
			}
		}
		return instance;
	}
	
	/**
	 * Adds the passed metric key to the blacklist
	 * @param key The metric key to blacklist
	 */
	@Override
	public void blackList(final String key) {
		LongAdder la = blackListed.putIfAbsent(key, PLACEHOLDER);
		if(la==null || la==PLACEHOLDER) {
			la = new AccumulatingLongAdder(blackListInstanceCount);
			blackListed.replace(key, la);		
			final Notification n = new Notification(NOTIF_NEW_BLACKLIST, OBJECT_NAME, notifSerial.incrementAndGet(), System.currentTimeMillis(), "Blacklisted metric key:" + key);
			n.setUserData(key);
			sendNotification(n);
		}
		la.increment();
	}
	
	/**
	 * Determines if the passed metric key has been blacklisted 
	 * and increments the incident count if it has
	 * @param key The metric key to test
	 * @return true if the metric key has been blacklisted, false otherwise
	 */
	@Override
	public boolean isBlackListed(final String key) {
		final LongAdder la = blackListed.get(key);
		if(la==null) return false;
		la.increment();
		return true;		
	}
	
	/**
	 * Returns the number of blacklisted metric keys
	 * @return the number of blacklisted metric keys
	 */
	@Override
	public int getBlacklistedCount() {
		return blackListed.size();
	}
	
	/**
	 * Returns the total number of observed blacklisted metric instances
	 * @return the total number of observed blacklisted metric instances
	 */
	@Override
	public long getBlacklistInstances() {
		return blackListInstanceCount.longValue();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.BlacklistMBean#blacklisted()
	 */
	@Override
	public Set<String> blacklisted() {
		return new HashSet<String>(blackListed.keySet());
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.metrics.BlacklistMBean#blacklistedCounts()
	 */
	@Override
	public Map<String, Long> blacklistedCounts() {
		final HashMap<String, Long> map = new HashMap<String, Long>(blackListed.size());
		for(Map.Entry<String, LongAdder> entry: blackListed.entrySet()) {
			map.put(entry.getKey(), entry.getValue().longValue());
		}
		return map;
	}
	
	/**
	 * Resets the blacklist state and counters
	 */
	@Override
	public void reset() {
		blackListed.clear();
		blackListInstanceCount.reset();
		
	}

	private Blacklist() {
		super(SharedNotificationExecutor.getInstance(), INFOS);
		JMXHelper.registerMBean(this, OBJECT_NAME);
	}

}
