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

import java.util.Comparator;

/**
 * <p>Title: TagKeySorter</p>
 * <p>Description: Sorts tags, putting <b><code>app</code></b> and <b><code>host</code></b> first</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tracing.TagKeySorter</code></p>
 */

public class TagKeySorter implements Comparator<String> {
	/** A shareable instance */
	public static final TagKeySorter INSTANCE = new TagKeySorter(); 
//	/** The tag keys this comparator will sort first */
//	private final Set<String> orderedTagKeys;
//	
//	/** The config key name for the tags that will sort low (in low to high order) */
//	public static String CONFIG_TAG_KEYS = "tagkey.sort";
//	/** The default tags that will sort low (in low to high order) */
//	public static String[] DEFAULT_TAG_KEYS = {"app", "host"};
	
	/**
	 * Creates a new TagKeySorter
	 */
	protected TagKeySorter() {
//		final String[] tagKeys = ConfigurationHelper.getArraySystemThenEnvProperty(CONFIG_TAG_KEYS, DEFAULT_TAG_KEYS);
//		if(tagKeys==null || tagKeys.length==0) throw new IllegalArgumentException("The passed tag key array was null or zero length");
//		final LinkedHashSet<String> cleanedKeys = new LinkedHashSet<String>(tagKeys.length);
//		for(String key: tagKeys) {
//			if(key==null || key.trim().isEmpty()) continue;
//			cleanedKeys.add(key.trim().toLowerCase());
//		}
//		if(cleanedKeys.isEmpty()) throw new IllegalArgumentException("The passed tag key array had no tags");
//		orderedTagKeys = Collections.unmodifiableSet(cleanedKeys);
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	 */
	@Override
	public int compare(final String o1, final String o2) {
		if(o1.equals(o2)) return 0;
		if(o1.equalsIgnoreCase(o2)) return -1;
		final int d = doCompare(o1, o2);
		if(d!=Integer.MAX_VALUE) return d;
		return o1.toLowerCase().compareTo(o2.toLowerCase());
	}
	
	/**
	 * Performs the tag key specific compare
	 * @param var1 The first tag
	 * @param var2 The second tag
     * @return a negative integer, zero, or a positive integer as the
     *         first argument is less than, equal to, or greater than the
     *         second.
	 */
    @SuppressWarnings("static-method")
	protected int doCompare(final String var1, final String var2) {
        return "app".equalsIgnoreCase(var1)?-1:("app".equalsIgnoreCase(var2)?1:("host".equalsIgnoreCase(var1)?-1:("host".equalsIgnoreCase(var2)?1:2147483647)));
    }
	
	
//	/**
//	 * {@inheritDoc}
//	 * @see java.lang.Object#toString()
//	 */
//	@Override
//	public String toString() {
//		return "TagKeySorter" + orderedTagKeys.toString();
//	}

}

