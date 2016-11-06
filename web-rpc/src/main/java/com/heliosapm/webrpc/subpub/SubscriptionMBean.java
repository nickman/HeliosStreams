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

/**
 * <p>Title: SubscriptionMBean</p>
 * <p>Description: MBean interface for Subscriptions</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.webrpc.subpub.SubscriptionMBean</code></p>
 */

public interface SubscriptionMBean {
	/**
	 * Indicates if the passed message is a member of this subscription
	 * @param message The message to determine the membership of
	 * @return false if the message is definitely NOT a member of this subscription, true if it is a member.
	 */
	public boolean test(String message);
	
	/**
	 * Returns the total number of matched incoming messages
	 * @return the total number of matched incoming messages
	 */
	public long getMatches();
	
	/**
	 * Returns the the total number of dropped incoming messages
	 * @return the the total number of dropped incoming messages
	 */
	public long getDrops();
	
	
	/**
	 * Returns the current number of retained (inserted) patterns
	 * @return the current number of retained (inserted) patterns
	 */
	public int getSize();
	
	/**
	 * Returns the current number of subscribers
	 * @return the current number of subscribers
	 */
	public int getSubscriberCount();
	
	/**
	 * Returns the subscription Id
	 * @return the subscription Id
	 */	
	public long getSubscriptionId();
	
	/**
	 * Returns the probability that the subscription will erroneously return true for a message that has not actually been put in the BloomFilter.
	 * @return the error probability
	 */
	public double getErrorProbability();

	/**
	 * Returns the delta between the expected and actual error probability
	 * @return the delta between the expected and actual error probability
	 */
	public double getRelativeProbability();
	
	/**
	 * Returns a message describing the subscription and state
	 * @return a message describing the subscription and state
	 */
	public String getDescription();
	

	/**
	 * Returns the initial expected insertion count for the bloom filter
	 * @return the bloom filter initial expected insertion count
	 */
	public int getCapacity();



	
}
