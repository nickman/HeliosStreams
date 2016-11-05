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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.management.ObjectName;

import com.heliosapm.utils.jmx.JMXHelper;

import reactor.event.selector.HeaderResolver;
import reactor.event.selector.Selector;

/**
 * <p>Title: TSMetaPatternSelector</p>
 * <p>Description: A reactor selector for TSMeta patterns</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>org.helios.tsdb.plugins.remoting.subpub.TSMetaPatternSelector</code></p>
 * <p>At last benchmark:  Avg: 5893.4605769231 ns.</p>
 */

public class TSMetaPatternSelector implements Selector {
	/** The selector key */
	protected final String key;
	/** Indicates if there are any wildcards in the key */
	protected final boolean patternObjectName;
	/** Indicates if there are any wildcards in the key's domain */
	protected final boolean patternDomain;
	/** The key's domain matching regex */
	protected final Pattern domainPattern;
	/** A list of key pair matching regexes ordered most specific to least specific  */
	protected final List<RankedPattern> rankedPatterns;
	/** A thread local to assign a one time list of ranked patterns to each callng thread */
	private final ThreadLocal<List<RankedPattern>> rpHolder;
	
	/**
	 * Creates a new TSMetaPatternSelector
	 * @param pattern The pattern to select on
	 */
	public TSMetaPatternSelector(CharSequence pattern) {
		if(pattern==null) throw new IllegalArgumentException("The passed pattern was null");
		key = pattern.toString().trim();
		final ObjectName objectName = JMXHelper.objectName(key);
		patternObjectName = objectName.isPattern();
		patternDomain = objectName.isDomainPattern();
		domainPattern = patternize(objectName.getDomain());		
		List<RankedPattern> rp = new ArrayList<RankedPattern>();
		for(Map.Entry<String, String> keyProp : objectName.getKeyPropertyList().entrySet()) {
			String p = keyProp.getKey() + "=" + keyProp.getValue();
			int rank = rank(p);
			rp.add(new RankedPattern(patternize(p), rank));
		}		
		Collections.sort(rp);
		rankedPatterns = Collections.unmodifiableList(new ArrayList<RankedPattern>(rp));
		
		rpHolder = new ThreadLocal<List<RankedPattern>>() {
			final int rpSize = rankedPatterns.size();
			@Override
			protected List<RankedPattern> initialValue() {			
				return new ArrayList<RankedPattern>(rpSize);
			}
			@Override
			public List<RankedPattern> get() {
				final List<RankedPattern> k = super.get();
				k.clear();
				k.addAll(rankedPatterns);
				return k;
			}
		};		
	}
	
	public static void main(String[] args) {
		System.out.println(new TSMetaPatternSelector("sys*:dc=dc*,host=WebServer1|WebServer5"));
	}

	
	protected int rank(final String segment) {
		int rank = 0;
		if(segment.indexOf("*")!=-1) rank++;
		if(segment.indexOf("|")!=-1) rank++;
		return rank==2 ? 3 : rank;
	}
	
	public String toString() {
		final StringBuilder b = new StringBuilder("TSMetaPatternSelector [");
		b.append("\n\tPattern:").append(key);
		b.append("\n\tRanked Matchers:");
		for(RankedPattern r: rankedPatterns) {
			b.append("\n\t\t").append(r.toString());
		}		
		b.append("\n]");
		return b.toString();
	}

	/**
	 * Creates a regex pattern to match the passed segment
	 * @param segment The segment to create a pattern for
	 * @return the compiled pattern
	 */
	protected Pattern patternize(final String segment) {
	    String p = segment;
		if(segment.indexOf("|") != -1) {
			final int index = segment.indexOf("=");
			if(index != -1) {
				p = new StringBuilder(segment.substring(0, index)).append("=(").append(segment.substring(index+1)).append(")").toString();
			} else {
				p = new StringBuilder("(").append(segment).append(")").toString();
			}
		}
	    return Pattern.compile(p.replace("*", ".*?"));
	}
	
	
	
	/**
	 * {@inheritDoc}
	 * @see reactor.event.selector.Selector#getObject()
	 */
	@Override
	public Object getObject() {
		return key;
	}

	/**
	 * {@inheritDoc}
	 * @see reactor.event.selector.Selector#matches(java.lang.Object)
	 */
	@Override
	public boolean matches(final Object key) {
		if(key==null) return false;
		final ObjectName on;
		try {
			on = JMXHelper.objectName(key.toString());
		} catch (Exception ex) {
			return false;
		}
		if(!domainPattern.matcher(on.getDomain()).matches()) return false;
		
		final List<RankedPattern> rps = rpHolder.get();
		final HashMap<String, String> pairs = new HashMap<String, String>(on.getKeyPropertyList());
		
		Iterator<RankedPattern> iter = rps.iterator();
		Iterator<Map.Entry<String, String>> pairIter = pairs.entrySet().iterator();
		
		while(pairIter.hasNext()) {
			Map.Entry<String, String> pairEntry = pairIter.next();
			final String pair = pairEntry.getKey() + "=" + pairEntry.getValue();
			while(iter.hasNext()) {
				RankedPattern rp = iter.next();
				if(rp.rank==0) {
					if(rp.pattern().equals(pair)) {
						iter.remove();
						pairIter.remove();
						break;						
					}
				} else {
					if(rp.matcher(pair).matches()) {
						iter.remove();
						pairIter.remove();
						break;						
					}
				}				
			}
			iter = rps.iterator();
		}
		return patternObjectName ? rps.isEmpty() : (rps.isEmpty() && pairs.isEmpty());
	}

	/**
	 * {@inheritDoc}
	 * @see reactor.event.selector.Selector#getHeaderResolver()
	 */
	@Override
	public HeaderResolver getHeaderResolver() {
		return null;
	}
	
	private static class RankedPattern implements Comparable<RankedPattern> {
		final int rank;
		final Pattern regex;
		
		RankedPattern(final Pattern regex, final int rank) {
			this.regex = regex;
			this.rank = rank;
		}
		
		final Matcher matcher(final CharSequence input) {
			return regex.matcher(input);
		}

		@Override
		public int compareTo(RankedPattern r) {
			if(r.rank==rank) return 0;
			return rank < r.rank ? -1 : 1;
		}
		
		public String pattern() {
			return regex.pattern();
		}
		
		public String toString() {
			return new StringBuilder("RankedPattern [pattern:").append(regex.pattern()).append(", rank:").append(rank).append("]").toString();
		}

		/**
		 * {@inheritDoc}
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + rank;
			result = prime * result + regex.pattern().hashCode();
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
			if (getClass() != obj.getClass())
				return false;
			RankedPattern other = (RankedPattern) obj;
			if (rank != other.rank)
				return false;
			if (regex == null) {
				if (other.regex != null)
					return false;
			} else if (!regex.equals(other.regex))
				return false;
			return true;
		}
		
		
	}

}
