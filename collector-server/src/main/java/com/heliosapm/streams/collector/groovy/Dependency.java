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

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.LOCAL_VARIABLE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

import groovy.transform.AnnotationCollector;
import groovy.transform.Field;

/**
 * <p>Title: Dependency</p>
 * <p>Description: Annotation to create a dependency on a cache value in a groovy script</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.groovy.Dependency</code></p>
 */
@Documented
@Retention(RUNTIME)
@Target({ FIELD, LOCAL_VARIABLE, ANNOTATION_TYPE })

@AnnotationCollector({VolatileField.class})
public @interface Dependency {
	/**
	 * The cache key of the value to inject
	 */
	public String cacheKey();
	
	/**
	 * The expected type of the injected value
	 */
	public Class<?> type() default Object.class;
	
	/**
	 * The timeout on waiting for the cache to callback with the value
	 * if it was not present at the initial lookup. 
	 * A zero or less timeout means no timeout.
	 */
	public long timeout() default 0L;
	
	/**
	 * The unit of the timeout
	 */
	public TimeUnit unit() default TimeUnit.SECONDS;
}
