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
package com.heliosapm.webrpc.server;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.concurrent.Executor;

import javax.management.ObjectName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.jmx.JMXManagedThreadPool;
import com.heliosapm.webrpc.websocket.WebSocketServiceHandler;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketServerCompressionHandler;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

/**
 * <p>Title: RPCServer</p>
 * <p>Description: The RPC server singleton</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.webrpc.server.RPCServer</code></p>
 */

public class RPCServer extends ChannelInitializer<SocketChannel> implements Closeable {
	/** The singleton instance */
	private static volatile RPCServer instance = null;
	/** The singleton instance ctor lock */
	private static final Object lock = new Object();
	
	/** The configuration key for the listening port */
	public static final String CONF_PORT = "webrpc.port";
	/** The default listening port */
	public static final int DEFAULT_PORT = 8081;
	
	/** The configuration key for the worker thread count */
	public static final String CONF_WORKERS = "webrpc.workers";
	/** The default worker thread count */
	public static final int DEFAULT_WORKERS = Runtime.getRuntime().availableProcessors() * 2;
	
	
	/** The configuration key for the binding interface */
	public static final String CONF_BIND = "webrpc.bind";
	/** The default binding interface */
	public static final String DEFAULT_BIND = "0.0.0.0";
	
	private static final String WEBSOCKET_PATH = "/ws";
	
	
	/** Instance logger */
	protected static final Logger log = LogManager.getLogger(RPCServer.class);
	/** The configured socket we're listening on */
	protected final InetSocketAddress socketAddress;
	/** The server bootstrap */
	protected final ServerBootstrap bootstrap;
	/** The boss event loop group */
	protected final EventLoopGroup bossGroup;
	/** The worker event loop group */
	protected final EventLoopGroup workerGroup;
	/** The boss group executor's JMX ObjectName */
	protected final ObjectName workerExecutorObjectName = JMXHelper.objectName(new StringBuilder(getClass().getPackage().getName()).append(":service=Executor,type=WorkerGroup"));
	/** The worker group executor */
	protected final JMXManagedThreadPool workerExecutor;
	/** The server channel */
	protected final Channel serverChannel;
	
	protected final WebSocketServiceHandler webSockServiceHandler = new WebSocketServiceHandler();
	
	
	
	/**
	 * Initializes the RPCServer singleton instance if not already initialized and returns it
	 * @param properties The optional configuration properties
	 * @return the RPCServer singleton instance
	 */
	public static RPCServer init(final Properties properties) {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					instance = new RPCServer(properties);
				}
			}
		}
		return instance;
	}
	
	/**
	 * Acquires the RPCServer singleton instance
	 * @return the RPCServer singleton instance
	 */
	public static RPCServer getInstance() {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					throw new IllegalStateException("The RPCServer has not been initialized");
				}
			}
		}
		return instance;
	}
	
	
	
	private RPCServer(final Properties properties) {
		log.info(">>>>> RPCServer starting....");
		final int port = ConfigurationHelper.getIntSystemThenEnvProperty(CONF_PORT, DEFAULT_PORT, properties);
		final int workerThreads = ConfigurationHelper.getIntSystemThenEnvProperty(CONF_WORKERS, DEFAULT_WORKERS, properties);
		final String iface = ConfigurationHelper.getSystemThenEnvProperty(CONF_BIND, DEFAULT_BIND, properties);
		socketAddress = new InetSocketAddress(iface, port);		
		bossGroup = new NioEventLoopGroup(1);
		workerExecutor = new JMXManagedThreadPool(workerExecutorObjectName, "WorkerPool", workerThreads, workerThreads * 2, 1, 60000, 100, 99, true);
		workerGroup = new NioEventLoopGroup(workerThreads, (Executor)workerExecutor);
		bootstrap = new ServerBootstrap();		
		bootstrap.group(bossGroup, workerGroup)
			.channel(NioServerSocketChannel.class)
			.handler(new LoggingHandler(LogLevel.INFO))
			.childHandler(this);
		serverChannel = bootstrap.bind(socketAddress).syncUninterruptibly().channel();
		
		
		log.info("<<<<< Started RPCServer on [{}]", socketAddress);
	}
	
	/**
	 * <p>Closes this RPCServer </p>
	 * {@inheritDoc}
	 * @see java.io.Closeable#close()
	 */
	public void close() throws IOException {
		log.info(">>>>> RPCServer closing....");
		try { 
			bossGroup.shutdownGracefully().addListener(f -> {
				log.info("BossEventGroup stopped");
			}); 
		} catch (Exception x) {/* No Op */}
		try { 
			workerGroup.shutdownGracefully().addListener(f -> {
				workerExecutor.shutdown();
				log.info("WorkerEventGroup stopped");
			});
		} catch (Exception x) {/* No Op */}
		instance = null;
		log.info("<<<<< Closed RPCServer.");
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		JMXHelper.fireUpJMXMPServer(1239);
		log.info("Starting RPCServer");
		final RPCServer server = init(null);
		log.info("Done");
		StdInCommandHandler.getInstance().registerCommand("stop", new Runnable(){
			public void run() {
				try { server.close(); } catch (Exception x) {/* No Op */}
				System.exit(0);
			}
		}).run();
	}
	
	@Override
	protected void initChannel(final SocketChannel ch) throws Exception {
		final ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast(new HttpServerCodec());
        pipeline.addLast(new HttpObjectAggregator(65536));
        pipeline.addLast(new WebSocketServerCompressionHandler());
        pipeline.addLast(new WebSocketServerProtocolHandler(WEBSOCKET_PATH, null, true));
        pipeline.addLast(webSockServiceHandler);
        		
	}

}
