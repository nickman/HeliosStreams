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
package com.heliosapm.streams.tracing;

import com.google.common.base.Predicate;
import com.heliosapm.streams.tracing.DefaultTracerImpl.PredicateTrace;

/**
 * <p>Title: SuppressPredicates</p>
 * <p>Description: Some predefines suppression predicates</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tracing.SuppressPredicates</code></p>
 */

public class SuppressPredicates {

	/** Suppression predicate that suppresses tracing if the value is zero or less */
	public static final Predicate<PredicateTrace> IGNORE_ZERO_OR_LESS = new Predicate<PredicateTrace>() {
		@Override
		public boolean apply(final PredicateTrace input) {
			if(input==null) return true;			
			return input.getValue().doubleValue() <= 0D;
		}
	};
	
	/** Suppression predicate that suppresses tracing if the value is less than zero */
	public static final Predicate<PredicateTrace> IGNORE_NEGATIVE = new Predicate<PredicateTrace>() {
		@Override
		public boolean apply(final PredicateTrace input) {
			if(input==null) return true;			
			return input.getValue().doubleValue() < 0D;
		}
	};
	
	private SuppressPredicates() {}

}
