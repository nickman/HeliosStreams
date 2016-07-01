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
package com.heliosapm.streams.admin.nodes;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * <p>Title: NodeConfigurationServer</p>
 * <p>Description: The endpoint that responds to nodes requesting marching orders</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.admin.nodes.NodeConfigurationServer</code></p>
 */
@RestController
@RequestMapping(value="/nodeconfig")
public class NodeConfigurationServer {
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	
	
	/**
	 * Retrieves the configuration for the passed host and app
	 * @param host The requesting host
	 * @param appname The requested app for which configuration should be delivered
	 * @return a properties file in string format
	 */
	@SuppressWarnings("static-method")
	@RequestMapping(value="/nodeconfig/{host}/{appname}", method=RequestMethod.GET)
	public String getConfiguration(@PathVariable final String host, @PathVariable final String appname) {
		return null;
	}

}
