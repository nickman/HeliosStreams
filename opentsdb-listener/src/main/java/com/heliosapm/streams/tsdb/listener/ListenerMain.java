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
package com.heliosapm.streams.tsdb.listener;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.impl.StoreFileListener;

/**
 * <p>Title: ListenerMain</p>
 * <p>Description: </p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tsdb.listener.ListenerMain</code></p>
 */

public class ListenerMain implements Closeable, StoreFileListener, Runnable {
	/** Instance logger */
	protected final Logger log = LogManager.getLogger();
//	/** The in/out queue */
//	protected final MessageQueue<AbstractMarshallable> q;
	
	protected final String basePath = System.getProperty("java.io.tmpdir") + "/getting-started";
	protected final ChronicleQueue queue = ChronicleQueueBuilder.single(basePath).build();
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		final ListenerMain listener = new ListenerMain();
		listener.run();
	}
	
	
	public void run() {
		
	}


	/**
	 * {@inheritDoc}
	 * @see net.openhft.chronicle.queue.impl.StoreFileListener#onReleased(int, java.io.File)
	 */
	@Override
	public void onReleased(int cycle, File file) {
		// TODO Auto-generated method stub
		
	}


	/**
	 * {@inheritDoc}
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
	

}
