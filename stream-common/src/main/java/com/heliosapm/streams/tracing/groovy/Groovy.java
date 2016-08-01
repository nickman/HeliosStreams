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
package com.heliosapm.streams.tracing.groovy;

/**
 * <p>Title: Groovy</p>
 * <p>Description: Utility class to determine if groovy is available on the classpath</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tracing.groovy.Groovy</code></p>
 */

public class Groovy {

	/**
	 * Indicates if Groovy is available in the classpath
	 * @return true if Groovy is available in the classpath, false otherwise
	 */
	public static boolean isGroovyAvailable() {
		try {
			Class.forName("groovy.lang.GroovySystem");
			return true;
		} catch (Exception ex) {
			return false;
		}
	}
	
	private Groovy() {}

}

