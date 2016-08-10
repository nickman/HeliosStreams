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
package com.heliosapm.streams.opentsdb;

import java.util.Properties;

import com.heliosapm.utils.lang.StringHelper;

/**
 * <p>Title: InterceptorInstaller</p>
 * <p>Description: Splits the supplied interceptor class names for producers and consumers</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.opentsdb.InterceptorInstaller</code></p>
 */

public class InterceptorInstaller {

	/**
	 * Filters out any property configured class names that are not interceptors appropriate for the given kafka client type
	 * @param producer If true, the kafka client type is a producer, otherwise is a consumer
	 * @param props The properties to filter
	 * @return the filtered properties
	 */
	public static Properties filter(final boolean producer, final Properties props) {
		try {
			final String interceptorProps = props.getProperty("interceptor.classes");
			if(interceptorProps==null || interceptorProps.trim().isEmpty()) return props;
			final String[] classNames = StringHelper.splitString(interceptorProps, ',', true);
			if(classNames.length==0) {
				props.remove("interceptor.classes");
				return props;			
			}
			final Class<?> baseInterceptor = Class.forName(producer ? "org.apache.kafka.clients.producer.ProducerInterceptor" : "org.apache.kafka.clients.consumer.ConsumerInterceptor");
			final StringBuilder b = new StringBuilder();			
			for(String className: classNames) {
				Class<?> inter = null; 
				try {
					inter = Class.forName(className);
				} catch (Exception x) {
					continue;
				}
				if(baseInterceptor.isAssignableFrom(inter)) {
					b.append(className).append(",");
				}
			}
			if(b.length() > 0) {
				final String pvalue = b.deleteCharAt(b.length()-1).toString();
				props.setProperty("interceptor.classes", pvalue);
			} else {
				props.remove("interceptor.classes");
			}
			return props;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		
	}
	
}
