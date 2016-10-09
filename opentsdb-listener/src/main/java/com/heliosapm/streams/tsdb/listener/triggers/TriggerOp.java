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
package com.heliosapm.streams.tsdb.listener.triggers;


import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * <p>Title: TriggerOp</p>
 * <p>Description: Functional and bitmapped enum for h2 trigger types</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tsdb.listener.triggers.TriggerOp</code></p>
 */

public enum TriggerOp {
	/** The INSERT trigger operation */
	INSERT(1),

	/** The UPDATE trigger operation */
	UPDATE(2),

	/** The DELETE trigger operation */
	DELETE(4),

	/** The SELECT trigger operation */
	SELECT(8);
	
	

	/**
	 * Returns a string containing the pipe delimited names of the TriggersOps that are enabled in the passed mask
	 * @param mask The mask to render
	 * @return a string containing the pipe delimited names of the TriggersOps that are enabled in the passed mask
	 */
	public static String getNamesFor(int mask) {
		if(mask<0) throw new IllegalArgumentException("Invalid mask value [" + mask + "]", new Throwable());
		StringBuilder b = new StringBuilder();		
		for(TriggerOp to: values()) {
			if(to.isEnabled(mask)) {
				b.append(to.name()).append("|");
			}
		}
		if(b.length()<1) b.append("NONE");
		else b.deleteCharAt(b.length()-1);
		return b.toString();
	}
	
	private TriggerOp(int code) {
		this.code = code;
		mask = code;
	}
	
	/** The code for this op */
	private final int code;
	/** The mask for this op */
	private final int mask;


	/**
	 * Returns the code for this trigger op
	 * @return the code for this trigger op
	 */
	public int getCode() {
		return code;
	}
	
	/**
	 * Returns the mask for this state
	 * @return the mask for this state
	 */
	public int getMask() {
		return mask;
	}
	
	/**
	 * Determines if the passed mask is enabled for this TriggerOp
	 * @param mask the mask to test
	 * @return true if the passed mask is enabled for this TriggerOp, false otherwise
	 */
	public boolean isEnabled(int mask) {		
		return (mask | this.mask) == mask;
	}
	
	/**
	 * Enables the passed mask for this TriggerOp and returns it
	 * @param mask The mask to modify
	 * @return the modified mask
	 */
	public int enable(int mask) {
		return (mask | this.mask);
	}
	
	
	/**
	 * Returns an array of TriggerOps that enabled in the passed mask
	 * @param mask The masks to get the TriggerOps for 
	 * @return an array of TriggerOps that are enabled in the passed mask
	 */
	public static TriggerOp[] getEnabledStates(int mask) {
		Set<TriggerOp> enabled = new HashSet<TriggerOp>();
		for(TriggerOp t: values()) {
			if(t.isEnabled(mask)) enabled.add(t);
		}
		return enabled.toArray(new TriggerOp[enabled.size()]);
	}
	
	/**
	 * Returns a compound name representing all the TriggerOps that enabled in the passed mask
	 * @param mask The masks to get the TriggerOps for 
	 * @return a compound name representing all the TriggerOps that are enabled in the passed mask
	 */
	public static String getEnabledStatesName(int mask) {
		return Arrays.toString(getEnabledStates(mask)).replace("[", "").replace("]", "").replace(" ", "").replace(',', '|');
	}


	/**
	 * @param mask
	 * @return
	 */
	public int disable(int mask) {
		return mask & ~code;
	}
	
}
