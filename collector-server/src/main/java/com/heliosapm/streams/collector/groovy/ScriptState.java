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
package com.heliosapm.streams.collector.groovy;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;

import javax.management.AttributeChangeNotification;
import javax.management.MBeanNotificationInfo;

/**
 * <p>Title: ScriptState</p>
 * <p>Description: Enumerates the possible states of a {@link ManagedScript}</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.groovy.ScriptState</code></p>
 */

public enum ScriptState {
	/** The initial state of a newly created script */
	INIT("The script was just initialized"),
	/** Script execution is paused */
	PAUSED("The script is scheduled but paused"),
	/** Script is in a scheduled steady state */
	SCHEDULED("The script is in scheduled steady state"),
	/** Script is in collecting */
	EXECUTING("The script is executing"),
	/** Script is compiled but has no schedule */
	PASSIVE("The script is compiled but has no schedule"),
	/** Script is waiting for dependency injection */
	WAITING("The script is waiting on dependencies"),
	/** Script is steady state but throwing some errors */
	ERRORS("The script is steady state but throwing errors"),
	/** Script is steady state but cannot connect to a resource */
	DISCONNECT("The script is steady state but cannot connect to a resource"),
	/** Script is being destroyed */
	DESTROY("The script is being destroyed");
	
	
	private ScriptState(final String description) {
		this.description = description;
		this.notifType = "collector.script.state." + name().toLowerCase();
		
	}
	
	/** A map of script state jmx notification infos keyed by the associated script state */
	public static final Map<ScriptState, MBeanNotificationInfo> NOTIF_INFOS;
	private static final ScriptState[] values = values();
	
	/** The script state description */
	public final String description;
	/** The jmx notification type */
	public final String notifType;
	/** States that this state can transition to */
	private static final EnumMap<ScriptState, EnumSet<ScriptState>> transitionTo = new EnumMap<ScriptState, EnumSet<ScriptState>>(ScriptState.class);
	
	static {
		Map<ScriptState, MBeanNotificationInfo> tmpNotifs = new EnumMap<ScriptState, MBeanNotificationInfo>(ScriptState.class);
		for(ScriptState st: values) {
			st.transitionTo(values);
			tmpNotifs.put(st, new MBeanNotificationInfo(new String[]{st.notifType}, AttributeChangeNotification.class.getName(), st.description));
		}
		NOTIF_INFOS = Collections.unmodifiableMap(tmpNotifs);
	}
	
	private ScriptState transitionTo(final ScriptState...states) {
		final EnumSet<ScriptState> set = EnumSet.noneOf(ScriptState.class);
		set.remove(this);
		transitionTo.put(this, set);
		return this;
	}
	
	public boolean canTransitionTo(final ScriptState state) {
		return transitionTo.get(state).contains(state);
	}
	
	public static void main(String[] args) {
		for(ScriptState s: ScriptState.values()) {
			System.out.println(s.name() + ".transitionTo(values);");
		}
	}
	
	
}
