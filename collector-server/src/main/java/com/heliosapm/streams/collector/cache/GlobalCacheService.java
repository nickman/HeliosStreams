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
package com.heliosapm.streams.collector.cache;

import java.io.Closeable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanNotificationInfo;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;

import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.jmx.JMXManagedScheduler;
import com.heliosapm.utils.jmx.SharedNotificationExecutor;

import groovy.lang.Closure;
import jsr166e.LongAdder;


/**
 * <p>Title: GlobalCacheService</p>
 * <p>Description: Namespaced cache service for scripts to store/lookup arbitrary values</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.cache.GlobalCacheService</code></p>
 */

public class GlobalCacheService extends NotificationBroadcasterSupport implements GlobalCacheServiceMBean {
	/** The singleton instance */
	private static volatile GlobalCacheService instance = null;
	/** The singleton instance ctor lock */
	private static final Object lock = new Object();
	
	/** The notification infos for the broadcaster */
	private static final MBeanNotificationInfo[] NOTIF_INFOS = new MBeanNotificationInfo[] {
		new MBeanNotificationInfo(new String[]{NOTIF_ADD_EVENT}, Notification.class.getName(), "Cache item added notification"),
		new MBeanNotificationInfo(new String[]{NOTIF_REMOVE_EVENT}, Notification.class.getName(), "Cache item removed notification"),
		new MBeanNotificationInfo(new String[]{NOTIF_REPLACE_EVENT}, Notification.class.getName(), "Cache item replaced notification"),
		new MBeanNotificationInfo(new String[]{NOTIF_EXPIRE_EVENT}, Notification.class.getName(), "Cache item expired notification")
	};
	
	/** The cache event scheduler */
	private JMXManagedScheduler scheduler = new JMXManagedScheduler(schedulerObjectName, "CacheEventScheduler", true); 
	
	/** A map of cache event listeners keyed by the interest key */
	private final Map<String, Set<CacheEventListener>> listenersByKey = new ConcurrentHashMap<String, Set<CacheEventListener>>();
	/** A set of cache event listeners for any interest key */
	private final Set<CacheEventListener> listenersByAnyKey = new CopyOnWriteArraySet<CacheEventListener>();
	/** A map of cache values keyed by the interest key */
	private final Map<String, CacheValue<Object>> cacheValuesByKey = new ConcurrentHashMap<String, CacheValue<Object>>();	
	/** Shared notification thread pool */
	private final SharedNotificationExecutor notificationExecutor =  SharedNotificationExecutor.getInstance();
	/** The count of expiries */
	private final LongAdder expiryCount = new LongAdder();
	/** The count of cache hits */
	private final LongAdder hitCount = new LongAdder();
	/** The count of cache misses */
	private final LongAdder missCount = new LongAdder();
	/** The JMX notification serial */
	private final AtomicLong notifSerial = new AtomicLong();
	
	
	
	/**
	 * Acquires the global cache singleton instance
	 * @return the global cache singleton instance
	 */
	public static GlobalCacheService getInstance() {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					instance = new GlobalCacheService();
				}
			}
		}
		return instance;
	}
	
	/**
	 * Sends a JMX notification
	 * @param type The notification type
	 * @param message The message
	 */
	protected void sendNotif(final String type, final String message) {
		final Notification notif = new Notification(type, objectName, notifSerial.incrementAndGet(), System.currentTimeMillis(), message);
		sendNotification(notif);
	}
	
	
	/**
	 * Fires a value added event to all registered listeners
	 * @param key The cache item key
	 * @param value The new cache item value
	 */
	protected void fireValueAdded(final String key, final Object value) {
		sendNotif(NOTIF_ADD_EVENT, "Cache Item Added [" + key + "/" + value + "]");
		for(final CacheEventListener listener: listenersByAnyKey) {
			notificationExecutor.execute(new Runnable(){
				public void run() {
					listener.onValueAdded(key, value);
				}
			});
		}
		final Set<CacheEventListener> keyListeners = listenersByKey.get(key);
		if(keyListeners!=null && !keyListeners.isEmpty()) {
			for(final CacheEventListener listener: keyListeners) {
				notificationExecutor.execute(new Runnable(){
					@Override
					public void run() {
						listener.onValueAdded(key, value);
					}
				});
			}			
		}
	}
	
	/**
	 * Fires a value removed event to all registered listeners
	 * @param key The cache item key
	 * @param value The removed cache item value
	 */
	protected void fireValueRemoved(final String key, final Object value) {
		sendNotif(NOTIF_REMOVE_EVENT, "Cache Item Removed [" + key + "]");
		for(final CacheEventListener listener: listenersByAnyKey) {
			notificationExecutor.execute(new Runnable(){
				public void run() {
					listener.onValueRemoved(key, value);
				}
			});
		}
		final Set<CacheEventListener> keyListeners = listenersByKey.get(key);
		if(keyListeners!=null && !keyListeners.isEmpty()) {
			for(final CacheEventListener listener: keyListeners) {
				notificationExecutor.execute(new Runnable(){
					public void run() {
						listener.onValueRemoved(key, value);
					}
				});
			}			
		}
	}
	
	/**
	 * Fires a value replaced event to all registered listeners
	 * @param key The cache item key
	 * @param oldValue The prior cache item value
	 * @param newValue The new cache item value
	 */
	protected void fireValueReplaced(final String key, final Object oldValue, final Object newValue) {
		sendNotif(NOTIF_REMOVE_EVENT, "Cache Item Replaced [" + key + "]");
		for(final CacheEventListener listener: listenersByAnyKey) {
			notificationExecutor.execute(new Runnable(){
				public void run() {
					listener.onValueReplaced(key, oldValue, newValue);
				}
			});
		}
		final Set<CacheEventListener> keyListeners = listenersByKey.get(key);
		if(keyListeners!=null && !keyListeners.isEmpty()) {
			for(final CacheEventListener listener: keyListeners) {
				notificationExecutor.execute(new Runnable(){
					public void run() {
						listener.onValueReplaced(key, oldValue, newValue);
					}
				});
			}			
		}
	}	
	
	/**
	 * Registers a cache event listener
	 * @param listener The listener to register
	 * @param keys The keys the listener is interested in. If none specified, will callback on any key
	 */
	public void addCacheEventListener(final CacheEventListener listener, final String...keys) {
		if(listener==null) throw new IllegalArgumentException("The passed listener was null");
		if(keys==null || keys.length==0) {
			listenersByAnyKey.add(listener);
		} else {
			for(String key : keys) {
				if(key==null || key.trim().isEmpty()) continue;
				final String _key = key.trim();
				Set<CacheEventListener> keyListeners = listenersByKey.get(_key);
				if(keyListeners==null) {
					synchronized(listenersByKey) {
						keyListeners = listenersByKey.get(_key);
						if(keyListeners==null) {
							keyListeners = new CopyOnWriteArraySet<CacheEventListener>();
							listenersByKey.put(_key, keyListeners);
						}
					}
				}
				keyListeners.add(listener);
			}
		}
	}
	
	/**
	 * Removes a registered cache event listener
	 * @param listener The listener to remove
	 * @param keys The keys the listener is interested in. If none specified, will remove all found instances of the listener
	 */
	public void removeCacheEventListener(final CacheEventListener listener, final String...keys) {
		if(listener==null) throw new IllegalArgumentException("The passed listener was null");
		if(keys==null || keys.length==0) {
			listenersByAnyKey.remove(listener);			
			final Iterator<Map.Entry<String, Set<CacheEventListener>>> keyIter = listenersByKey.entrySet().iterator();
			while(keyIter.hasNext()) {
				final Map.Entry<String, Set<CacheEventListener>> entry = keyIter.next();
				final Set<CacheEventListener> set = entry.getValue();
				if(set!=null) set.remove(listener);
				synchronized(listenersByKey) {
					if(set==null || set.isEmpty()) keyIter.remove();
				}
			}
		} else {
			for(final String key: keys) {
				final Set<CacheEventListener> set = listenersByKey.get(key);
				if(set!=null) set.remove(listener);
				synchronized(listenersByKey) {
					if(set==null || set.isEmpty()) listenersByKey.remove(key);
				}				
			}
		}
	}
	
	/**
	 * Attempts to retrieve the value for the passed cache key. If the value is found, it is returned.
	 * If the value is not found, the passed listener is registered to get a callback when a value is bound
	 * to the supplied cache key. If the value is not supplied within the timeout period, the timeout callback will be invoked.
	 * <b>Timeout not implemented yet</b>.
	 * @param key The cache key to look up
	 * @param type The expected type of the cache value
	 * @param listener The listener to notify if the value is bound yet
	 * @param timeout The timeout period on waiting for the value to be bound
	 * @param unit the unit of the timeout
	 * @return the cached value or null if not bound yet
	 */
	public <T> T getOrNotify(final String key, final Class<T> type, final CacheEventListener listener, final long timeout, final TimeUnit unit) {
		if(key==null || key.trim().isEmpty()) throw new IllegalArgumentException("The passed key was null or empty");
		final String _key = key.trim();
		final T t = get(_key);
		if(t!=null) {
			if(type!=null) {
				if(!type.isInstance(t)) throw new RuntimeException("Requested type for key [" + _key + "] was [" + type.getName() + "] but value of type [" + t.getClass().getName() + "]");
			}
			return t;
		}
		if(listener==null) throw new IllegalArgumentException("The passed listener was null");
		if(unit==null) throw new IllegalArgumentException("The passed TimeUnit was null");
		addCacheEventListener(listener, _key);
		return null;
	}
	
	/**
	 * Retrieves a value from cache
	 * @param key The key to retrieve by
	 * @param createIfNotFound A closure that will create the value if not found in cache
	 * @return The value or null if the key was not bound
	 */
	@SuppressWarnings("unchecked")
	public <T> T get(final String key, final Closure<T> createIfNotFound) {
		if(key==null || key.trim().isEmpty()) throw new IllegalArgumentException("The passed listener was null or empty");
		CacheValue<T> cv = (CacheValue<T>) cacheValuesByKey.get(key.trim());
		if(cv==null && createIfNotFound!=null) {
			synchronized(cacheValuesByKey) {
				cv = (CacheValue<T>) cacheValuesByKey.get(key.trim());
				if(cv==null) {
					final PutBuilder<T> pb = new PutBuilder<T>(key.trim());
					cv = (CacheValue<T>) cacheValuesByKey.get(key.trim());
					final T t = createIfNotFound.call(pb);
					pb.value(t);
				}
			}
		}
		
		if(cv==null) {
			missCount.increment();
			return null;
		}
		hitCount.increment();
		return cv.value;
	}
	
	/**
	 * Retrieves a value from cache
	 * @param key The key to retrieve by
	 * @param expiryPeriod The expiry period for the newly created cache value if created
	 * @param unit The expiry period unit
	 * @param createIfNotFound A closure that will create the value if not found in cache
	 * @return The value or null if the key was not bound and the closure returned null
	 */
	@SuppressWarnings("unchecked")
	public <T> T get(final String key, final long expiryPeriod , final TimeUnit unit, final Closure<T> createIfNotFound) {
		if(key==null || key.trim().isEmpty()) throw new IllegalArgumentException("The passed listener was null or empty");		
		CacheValue<T> cv = (CacheValue<T>) cacheValuesByKey.get(key.trim());
		if(cv==null && createIfNotFound!=null) {
			synchronized(cacheValuesByKey) {
				cv = (CacheValue<T>) cacheValuesByKey.get(key.trim());
				if(cv==null) {
					
					cv = new PutBuilder<T>(key.trim())
						.value(createIfNotFound.call())
						.expireIn(expiryPeriod, unit)
						.putForValue();
				}
			}
		}
		
		if(cv==null) {
			missCount.increment();
			return null;
		}
		hitCount.increment();
		return cv.value;		
	}

	/**
	 * Retrieves a value from cache
	 * @param key The key to retrieve by
	 * @param expiryPeriod The expiry period in ms. for the newly created cache value if created
	 * @param createIfNotFound A closure that will create the value if not found in cache
	 * @return The value or null if the key was not bound and the closure returned null
	 */
	public <T> T get(final String key, final long expiryPeriod , final Closure<T> createIfNotFound) {
		return get(key, expiryPeriod, TimeUnit.MILLISECONDS, createIfNotFound);
	}
	
	/**
	 * Retrieves a value from cache
	 * @param key The key to retrieve by
	 * @return The value or null if the key was not bound
	 */
	public <T> T get(final String key) {
		if(key==null || key.trim().isEmpty()) throw new IllegalArgumentException("The passed listener was null or empty");
		@SuppressWarnings("unchecked")
		CacheValue<T> cv = (CacheValue<T>) cacheValuesByKey.get(key.trim());
		if(cv==null) {
			missCount.increment();
			return null;
		}
		hitCount.increment();
		return cv.value;
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see org.helios.nash.cache.GlobalCacheServiceMBean#getListenerCount()
	 */
	@Override
	public int getListenerCount() {
		final Set<CacheEventListener> set = new HashSet<CacheEventListener>(listenersByAnyKey);
		for(Set<CacheEventListener> sl: listenersByKey.values()) set.addAll(sl);		
		return set.size();
	}
	
	
	/**
	 * Puts a value into the cache
	 * @param key The key to bind the value under
	 * @param value The value to bind
	 * @param expiryPeriod The expiry period for this cache item. Ignored if less than 1.
	 * @param unit The unit of the expiry period. Ignored if expiry period is less than 1. Defaults to {@link TimeUnit#MILLISECONDS} if null.
	 * @param onRemove An optional closure to be called when bound cache entry is removed or replaced
	 * @return the unbound value that was replaced or null
	 */
	@SuppressWarnings("unchecked")
	public <T> T put(final String key, final T value, final long expiryPeriod, final TimeUnit unit, final Closure<Void> onRemove) {
		if(key==null || key.trim().isEmpty()) throw new IllegalArgumentException("The passed key was null or empty");
		if(value==null) throw new IllegalArgumentException("The passed value with key [" + key + "] was null");
		final CacheValue<T> cv = new CacheValue<T>(key,value, expiryPeriod, unit, onRemove);		
		final CacheValue<T> oldValue = (CacheValue<T>) cacheValuesByKey.put(key.trim(), (CacheValue<Object>) cv);
		cv.boundTime = System.currentTimeMillis();
		if(oldValue!=null) {
			oldValue.close();
			fireValueReplaced(key.trim(), oldValue.value, value);
			return oldValue.value;
		} else {
			fireValueAdded(key.trim(), value);
			return null;
		}
		
	}
	
	/**
	 * Puts a value into the cache
	 * @param key The key to bind the value under
	 * @param value The value to bind
	 * @param expiryPeriod The expiry period for this cache item. Ignored if less than 1.
	 * @param unit The unit of the expiry period. Ignored if expiry period is less than 1. Defaults to {@link TimeUnit#MILLISECONDS} if null.
	 * @return the unbound value that was replaced or null
	 */

	public <T> T put(final String key, final T value, final long expiryPeriod, final TimeUnit unit) {
		return put(key, value, expiryPeriod, unit, null);
	}
	
	/**
	 * Puts a value into the cache
	 * @param key The key to bind the value under
	 * @param value The value to bind
	 * @param expiryPeriod The expiry period for this cache item in ms.. Ignored if less than 1.
	 * @return the unbound value that was replaced or null
	 */

	public <T> T put(final String key, final T value, final long expiryPeriod) {
		return put(key, value, expiryPeriod, TimeUnit.MILLISECONDS, null);
	}
	
	
	/**
	 * Puts a value into the cache
	 * @param key The key to bind the value under
	 * @param value The value to bind
	 * @param onRemove An optional closure to be called when bound cache entry is removed or replaced
	 * @return the unbound value that was replaced or null
	 */
	public <T> T put(final String key, final T value, final Closure<Void> onRemove) {		
		return put(key, value, -1L, TimeUnit.MILLISECONDS, onRemove);
	}
	
	/**
	 * Puts a value into the cache
	 * @param key The key to bind the value under
	 * @param value The value to bind
	 * @param expiryPeriod The expiry period for this cache item. Ignored if less than 1.
	 * @param onRemove An optional closure to be called when bound cache entry is removed or replaced
	 * @return the unbound value that was replaced or null
	 */
	public <T> T put(final String key, final T value, final long expiryPeriod, final Closure<Void> onRemove) {		
		return put(key, value, expiryPeriod, TimeUnit.MILLISECONDS, onRemove);
	}

	
	/**
	 * Puts a value into the cache
	 * @param key The key to bind the value under
	 * @param value The value to bind
	 * @return the unbound value that was replaced or null
	 */
	public <T> T put(final String key, final T value) {		
		return put(key, value, -1L, null, null);
	}
	
	public <T> PutBuilder<T> putBuilder(final String key, final T value) {
		return new PutBuilder<T>(key, value);
	}
	
	public class PutBuilder<T> {
		private final String key;
		private T value = null; 
		private long expiryPeriod = -1L;
		private TimeUnit unit = TimeUnit.MILLISECONDS; 
		private Closure<Void> onRemove = null;
		
		private PutBuilder(final String key, final T value) {
			this.key = key;
			this.value = value;
		}
		
		private PutBuilder(final String key) {
			this.key = key;
		}
		
		
		public PutBuilder<T> expireIn(final long expiryPeriod, final TimeUnit unit) {
			this.expiryPeriod = expiryPeriod;
			this.unit = unit;
			return this;
		}
		
		public PutBuilder<T> expireIn(final long expiryPeriod) {
			this.expiryPeriod = expiryPeriod;
			return this;
		}
		
		public PutBuilder<T> onRemove(final Closure<Void> onRemove) {
			this.onRemove = onRemove;
			return this;
		}
		
		private PutBuilder<T> value(final T v) {
			value = v;
			return this;
		}
		
		@SuppressWarnings("unchecked")
		public CacheValue<T> put() {
			if(value==null) {
				throw new RuntimeException("No value set on PutBuilder with key [" + key + "]. Programmer error");
			}
			final CacheValue<T> cv = new CacheValue<T>(key, value, expiryPeriod, unit, onRemove);
			final CacheValue<Object> prior = cacheValuesByKey.put(key, (CacheValue<Object>) cv);
			cv.boundTime = System.currentTimeMillis();
			if(prior!=null) {
				prior.close();
				fireValueReplaced(key.trim(), prior.value, value);
				return (CacheValue<T>)prior;
			} else {
				fireValueAdded(key.trim(), value);
			}
			return null;
		}

		@SuppressWarnings("unchecked")
		public CacheValue<T> putForValue() {
			if(value==null) {
				throw new RuntimeException("No value set on PutBuilder with key [" + key + "]. Programmer error");
			}
			final CacheValue<T> cv = new CacheValue<T>(key, value, expiryPeriod, unit, onRemove);
			final CacheValue<Object> prior = cacheValuesByKey.put(key, (CacheValue<Object>) cv);
			cv.boundTime = System.currentTimeMillis();
			if(prior!=null) {
				prior.close();
				fireValueReplaced(key.trim(), prior.value, value);				
			} else {
				fireValueAdded(key.trim(), value);
			}
			return cv;
		}
		
		
	}
	
//	public <T> void put(final String key, final T value, final long expiryPeriod, final TimeUnit unit) {
	
	private static final HashMap<String, String> EMPTY_MAP = new HashMap<String, String>(0);
	
	/**
	 * {@inheritDoc}
	 * @see org.helios.nash.cache.GlobalCacheServiceMBean#printCacheValues()
	 */
	@Override
	public HashMap<String, String> printCacheValues() {
		if(cacheValuesByKey.isEmpty()) return EMPTY_MAP; 
		final HashMap<String, String> map = new HashMap<String, String>(cacheValuesByKey.size());
		for(Map.Entry<String, CacheValue<Object>> entry: cacheValuesByKey.entrySet()) {
			map.put(entry.getKey(), entry.getValue().value.getClass().getName());
		}
		return map;
	}
	
	
	
	
	/**
	 * Removes the cache value keyed by the passed key
	 * @param key The key of the item to remove
	 * @return The removed value, or null if a value was not bound
	 */
	public <T> T remove(final String key) {
		if(key==null || key.trim().isEmpty()) throw new IllegalArgumentException("The passed key was null or empty");
		@SuppressWarnings("unchecked")
		final CacheValue<T> oldValue = (CacheValue<T>) cacheValuesByKey.remove(key.trim());		
		if(oldValue==null) return null;
		oldValue.close();
		fireValueRemoved(key.trim(), oldValue.value);		
		return oldValue.value;		
	}
	
	private GlobalCacheService() {		
		super(SharedNotificationExecutor.getInstance(), NOTIF_INFOS);
		JMXHelper.registerMBean(this, objectName);
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.helios.nash.cache.GlobalCacheServiceMBean#getSize()
	 */
	@Override
	public int getSize() {
		return cacheValuesByKey.size();
	}
	

	/**
	 * {@inheritDoc}
	 * @see org.helios.nash.cache.GlobalCacheServiceMBean#getExpiryCount()
	 */
	@Override
	public long getExpiryCount() {
		return expiryCount.longValue();
	}
	
	public <T> CacheValue<T> newCV(final String k, final T v) {
		return new CacheValue<T>(k, v, -1, null, null);
	}

	/**
	 * <p>Title: CacheValue</p>
	 * <p>Description: Container for cache values</p> 
	 * <p>Company: Helios Development Group LLC</p>
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>org.helios.nash.cache.GlobalCacheService.CacheValue</code></p>
	 */
	private class CacheValue<T> implements Closeable {
		/** The time the value was bound */
		long boundTime;
		/** The cached value */
		T value;
		/** The expiration handle */
		ScheduledFuture<?> expiryHandle = null;
		final long expireIn;
		final TimeUnit unit;
		final Closure<Void> onRemove;
		final String key;
		
		/**
		 * Creates a new CacheValue
		 * @param k The key
		 * @param v The value
		 * @param expireIn The time to expire this entry in
		 * @param unit The expire time unit
		 */
		@SuppressWarnings("unused")
		CacheValue(final String k, final T v, final long expireIn, final TimeUnit unit) {
			this(k, v, expireIn, unit, null);
		}
		
		/**
		 * Creates a new CacheValue
		 * @param k The key
		 * @param v The value
		 * @param onRemove A closure to call when this value is removed
		 */
		@SuppressWarnings("unused")
		CacheValue(final String k, final T v, final Closure<Void> onRemove) {
			this(k, v, -1L, null, onRemove);
		}
		
		/**
		 * Creates a new CacheValue
		 * @param k The key
		 * @param v The value
		 */
		@SuppressWarnings("unused")
		CacheValue(final String k, final T v) {
			this(k, v, -1L, null, null);
		}
		
		
		CacheValue(final String k, final T v, final long expireIn, final TimeUnit unit, final Closure<Void> onRemove) {
			this.value = v;
			this.key = k;
			boundTime = System.currentTimeMillis();	
			this.expireIn = expireIn;
			this.unit = unit==null ? TimeUnit.MILLISECONDS : unit;			
			this.onRemove = onRemove;
			if(this.expireIn>0) {
				expiryHandle = scheduler.schedule(new Runnable(){
					public void run() {
						remove(key);						
					}					
				}, this.expireIn, this.unit);				
			}
		}
		
		
		public void close() {
			if(expiryHandle!=null) try { 
				expiryHandle.cancel(true); 
			} catch (Exception x) {
				/* No Op */
			} finally {
				expiryHandle = null;
			}
			if(onRemove!=null) {
				onRemove.call(value);
			}
			if(value!=null && (value instanceof Closeable)) {
				try { ((Closeable)value).close(); } catch (Exception x) {/* No Op */}
			}
		}
		
	}
}
