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
package com.heliosapm.streams.streamnode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.heliosapm.streams.common.zoo.AdminFinder;

/**
 * <p>Title: StreamNode</p>
 * <p>Description: Bootstraps a worker stream node instance</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.streamnode.StreamNode</code></p>
 */

public class StreamNode {
	/** Instance loger */
	protected final Logger log = LogManager.getLogger(getClass());
	
	/** the command line args */
	protected final String[] clArgs;
	/** The admin url */
	protected String adminUrl = null;
	
	/**
	 * Creates a new StreamNode
	 * @param args the command line args
	 */
	public StreamNode(final String[] args) {
		clArgs = args;
	}
	
	/**
	 * Starts the node
	 */
	protected void start() {
		log.info(">>>>> Starting StreamNode...");
		AdminFinder af = AdminFinder.getInstance(clArgs);
		log.info("Waiting for Admin URL");
		adminUrl = af.getAdminURL(true);
		log.info("Acquired Admin URL: [{}]", adminUrl);
		// @ComponentScan(basePackages = "${scan.packages}")
		// @SpringBootApplication
		
	}
	
	
	/**
	 * Boots a stream node instance
	 * @param args as follows:
	 */
	public static void main(final String[] args) {
		final StreamNode streamNode = new StreamNode(args);
		streamNode.start();

	}

}
