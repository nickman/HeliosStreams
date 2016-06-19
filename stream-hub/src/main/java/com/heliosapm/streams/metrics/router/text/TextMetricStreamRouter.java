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
package com.heliosapm.streams.metrics.router.text;

import java.util.Properties;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * <p>Title: TextMetricStreamRouter</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrics.router.text.TextMetricStreamRouter</code></p>
 */

public class TextMetricStreamRouter {
	/** Instance logger */
	protected Logger logger = LogManager.getLogger(getClass());
	
	protected final Properties config;
	protected final StreamsConfig streamsConfig;
	 
	/**
	 * Creates a new TextMetricStreamRouter
	 */
	public TextMetricStreamRouter(final Properties config) {
		this.config = config;
		streamsConfig = new StreamsConfig(this.config);
		
	}

}
