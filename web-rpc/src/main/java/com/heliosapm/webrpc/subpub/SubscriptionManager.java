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

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.MXBean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.cliffc.high_scale_lib.NonBlockingHashSet;

import com.heliosapm.webrpc.SingletonEnvironment;
import com.heliosapm.webrpc.annotations.JSONRequestHandler;
import com.heliosapm.webrpc.annotations.JSONRequestService;
import com.heliosapm.webrpc.jsonservice.JSONRequest;
import com.heliosapm.webrpc.jsonservice.JSONRequestHandlerInvokerFactory;
import com.heliosapm.webrpc.jsonservice.JSONRequestRouter;
import com.heliosapm.webrpc.jsonservice.JSONSubscriber;
import com.heliosapm.webrpc.jsonservice.RequestType;
import com.heliosapm.webrpc.serialization.Datapoint;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.uid.UniqueId;
import reactor.core.Reactor;
import reactor.core.composable.Promise;
import reactor.core.composable.spec.Promises;
import reactor.event.Event;
import reactor.event.selector.Selectors;
import reactor.function.Consumer;

/**
 * <p>Title: SubscriptionManager</p>
 * <p>Description: Manages subscriptions on behalf of remote subscribers</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>org.helios.tsdb.plugins.remoting.subpub.SubscriptionManager</code></p>
 */
@JSONRequestService(name="pubsub", description="Manages subscriptions on behalf of remote subscribers")
@MXBean
public class SubscriptionManager implements SubscriptionManagerMXBean, SubscriberEventListener {
	
	/** All of the current subscriptions keyed by the pattern */
	protected final NonBlockingHashMap<String, Subscription> allSubscriptions = new NonBlockingHashMap<String, Subscription>(1024); 
	
	/** A map of sets of subscriptions keyed by the subscriber to those subscriptions */
	protected final NonBlockingHashMap<String, Map<Subscriber<Datapoint>, Set<Subscription>>> subscriberSubscriptions = new NonBlockingHashMap<String, Map<Subscriber<Datapoint>, Set<Subscription>>>(128);
	/** The event reactor */
	protected final Reactor reactor;
	
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());


	
	/** The default multiplier factor where a subscription's bloom filter will be the size of the initial load
	 * multiplied by this value */  // FIXME: this should be configurable
	public static final float DEFAULT_BLOOM_FILTER_SPACE_FACTOR = 2.1f;
	
	/**
	 * Creates a new SubscriptionManager
	 * @param config The extracted configuration
	 */
	public SubscriptionManager(final Properties config) {
		reactor = SingletonEnvironment.getInstance().getDefaultReactor();
		reactor.on(Selectors.object("subscription-terminated"), new Consumer<Event<Subscription>>(){
			@Override
			public void accept(Event<Subscription> t) {
				final Subscription termSub = t.getData();
				log.info("Terminated Subscription: [{}]", termSub);				
				allSubscriptions.remove(termSub.pattern);				
			}
		});
	}


	
	
	
	/**
	 * {@inheritDoc}
	 * @see org.helios.tsdb.plugins.remoting.subpub.SubscriptionManagerMXBean#getSubscriptions()
	 */
	@Override
	public SubscriptionMBean[] getSubscriptions() {
		try {
			final Set<SubscriptionMBean> copy = new HashSet<SubscriptionMBean>(allSubscriptions.values());		
			return copy.toArray(new SubscriptionMBean[copy.size()]);
		} catch (Exception ex) {
			log.error("Failed to convert Subscriptions to CompositeData", ex);
			return new SubscriptionMBean[0];
		}
	}
	
	/**
	 * Subscribes the calling client to a subscription of events matching the passed expression
	 * @param request The JSON request
	 * <p>Invoker:<b><code>sendRemoteRequest('ws://localhost:4243/ws', {svc:'pubsub', op:'sub', {x:'sys*:dc=dc*,host=WebServer1|WebServer5'}});</code></b>
	 */
	@JSONRequestHandler(name="sub", type=RequestType.SUBSCRIBE, description="Creates a new subscription on behalf of the calling client")
	public void subscribe(final JSONRequest request) {
		final String expression = request.getRequest().get("x").textValue();
		final String ID = JSONSubscriber.getId(request);
		Map<Subscriber<Datapoint>, Set<Subscription>> subMap = subscriberSubscriptions.get(ID);
		if(subMap==null) {
			synchronized(subscriberSubscriptions) {
				subMap = subscriberSubscriptions.get(ID);
				if(subMap==null) {
					Subscriber<Datapoint> subscriber = new JSONSubscriber<Datapoint>(request, TSDBEventType.DPOINT_DOUBLE, TSDBEventType.DPOINT_LONG);		
					Set<Subscription> set = new NonBlockingHashSet<Subscription>();
					subMap = Collections.unmodifiableMap(Collections.singletonMap(subscriber, set));
					subscriberSubscriptions.put(ID, subMap);
					subscriber.registerListener(this);
				}
			}
		}
		final Subscriber<Datapoint> finalSub = subMap.keySet().iterator().next();
		final Set<Subscription> subSet = subMap.values().iterator().next();
		subscribe(expression).onSuccess(new Consumer<Subscription>(){
			@Override
			public void accept(Subscription t) {				
				subSet.add(t);
				t.addSubscriber(finalSub);
				// FIXME:  Client side.
				request.subConfirm("" + t.getSubscriptionId()).setContent("ok").send();
			}
		}).onError(new Consumer<Throwable>(){
			@Override
			public void accept(Throwable t) {
				request.error("Failed to initiate subscription for [" + expression + "]", t);
			}
		});
	}
	

	/**
	 * {@inheritDoc}
	 * @see org.helios.tsdb.plugins.remoting.subpub.SubscriberEventListener#onDisconnect(org.helios.tsdb.plugins.remoting.subpub.Subscriber)
	 */
	@Override
	public void onDisconnect(final Subscriber subscriber) {
		log.info("Subscriber Disconnected: [{}]", subscriber);
		final  Map<Subscriber<Datapoint>, Set<Subscription>> endSubs = subscriberSubscriptions.remove(subscriber.getSubscriberId());
		if(endSubs!=null) {
			reactor.notify("subscriber-terminated", Event.wrap(subscriber));
		}		
	}	
	
	
	
	private <T> reactor.core.composable.Deferred<T, Promise<T>> getDeferred() {
		return Promises.<T>defer()				
				  .get();		
	}
	
	
	/**
	 * Acquires an existing or creates a new subscription for the passed pattern
	 * @param pattern The subscription pattern
	 * @return a promise of the created subscription
	 */
	protected Promise<Subscription> subscribe(final CharSequence pattern) {		
		log.info("Initiating Subscription for pattern [{}]", pattern);
		if(pattern==null) throw new IllegalArgumentException("The passed pattern was null");
		final String _pattern = pattern.toString().trim();
		if(_pattern.isEmpty()) throw new IllegalArgumentException("The passed pattern was empty");
		final reactor.core.composable.Deferred<Subscription, Promise<Subscription>> def = getDeferred();
		final reactor.core.composable.Deferred<Long, Promise<Long>> defCount = getDeferred();
		final Promise<Subscription> promise = def.compose();
//		reactor.getDispatcher().execute(new Runnable() {			
//			public void run() {
//				Subscription subx = allSubscriptions.get(_pattern);
//				if(subx==null) {
//					synchronized(allSubscriptions) {
//						subx = allSubscriptions.get(_pattern);						
//						if(subx==null) {							
//							try {
//								final QueryContext q = new QueryContext().setContinuous(true).setMaxSize(10240);
//								final long startTime = System.currentTimeMillis();								
//								final Set<byte[]> matchingTsuids = new HashSet<byte[]>(128);
//								log.info("Executing search for TSMetas with pattern [{}]", pattern);
//								metricSvc.evaluate(q, pattern.toString()).consume(new Consumer<List<TSMeta>>() {
//									final AtomicBoolean done = new AtomicBoolean(false);
//									@Override
//									public void accept(List<TSMeta> t) {
//										int cnt = 0;
//										for(TSMeta tsm: t) {
//											if(matchingTsuids.add(UniqueId.stringToUid(tsm.getTSUID()))) {
//												cnt++;
//											}											
//										}
//										log.info("Accepted [{}] TSMeta TSUIDs", cnt);
//										t.clear();
//										if(!q.shouldContinue() && done.compareAndSet(false, true)) {
//											final long elapsedTime = System.currentTimeMillis()-startTime;
//											log.info("Retrieved [{}] TSMetas in [{}] ms. to prime subscription [{}]", matchingTsuids.size(), elapsedTime, pattern);
//											final int bloomFactor = Math.round(DEFAULT_BLOOM_FILTER_SPACE_FACTOR * matchingTsuids.size());
//											final Subscription subx1 = new Subscription(reactor, metricSvc, _pattern, bloomFactor, TSDBEventType.DPOINT_DOUBLE, TSDBEventType.DPOINT_LONG);
//											int indexCnt = 0;
//											for(final Iterator<byte[]> biter = matchingTsuids.iterator(); biter.hasNext();) {
//												subx1._internalIndex(biter.next());
//												indexCnt++;
//												biter.remove();
//											}
//											log.info("Created and initialized [{}] items in Subscription BloomFilter for [{}]", indexCnt, pattern);
//											allSubscriptions.put(pattern.toString(), subx1);
//											def.accept(subx1);
//										}
//									}
//								});
//							} catch (Exception ex) {
//								def.accept(new Exception("Timed out on populating subscription for expression [" + pattern + "]"));
//							}
//						}  else {// subx not null
//							def.accept(subx);
//							return;							
//						}
//					} // synchr
//				} else {// subx not null
//					def.accept(subx);
//					return;					
//				}
//			} // end of run
//		}); // end of runnable
		return promise;
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.helios.tsdb.plugins.remoting.subpub.SubscriptionManagerMXBean#getSubscriptionCount()
	 */
	@Override
	public int getSubscriptionCount() {
		return allSubscriptions.size();
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.helios.tsdb.plugins.remoting.subpub.SubscriptionManagerMXBean#getSubscriberCount()
	 */
	@Override
	public int getSubscriberCount() {
		return subscriberSubscriptions.size();
	}

	


}




