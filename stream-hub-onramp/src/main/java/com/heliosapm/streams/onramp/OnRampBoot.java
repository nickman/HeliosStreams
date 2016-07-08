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
package com.heliosapm.streams.onramp;

import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.StandardEnvironment;

import com.heliosapm.streams.buffers.BufferManager;
import com.heliosapm.utils.concurrency.ExtendedThreadManager;
import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.url.URLHelper;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * <p>Title: OnRampBoot</p>
 * <p>Description: The core channel factory and thread pool base</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.onramp.OnRampBoot</code></p>
 */
@SpringBootApplication
@EnableAutoConfiguration 
@ComponentScan(basePackages={"com.heliosapm.streams.onramp.internal"})
public class OnRampBoot {
	/** Indicates if we're on linux in which case, async will use epoll */
	public static final boolean IS_LINUX = System.getProperty("os.name").toLowerCase().contains("linux");
	/** The number of core available to this JVM */
	public static final int CORES = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();

	static {
		initPoolParam();
	}
	

	/** The instance logger */
	protected final Logger log = LogManager.getLogger(OnRampBoot.class);
	/** The port to listen on */
	protected final int port;
	/** The nic interface to bind to */
	protected final String bindInterface;
	/** The socket address that the listener will be bound to */
	protected final InetSocketAddress bindSocket;	
	/** Indicates if we're using asynchronous net io */
	protected final boolean async;
	/** Indicates if epoll has been disabled even if we're on linux and using asynchronous net io */
	protected final boolean disableEpoll;
	
	/** The netty server bootstrap */
	protected final ServerBootstrap serverBootstrap = new ServerBootstrap();
	/** The configured number of worker threads */
	protected final int workerThreads;
	
	/** The channel type this server will create */
	protected final Class<? extends ServerChannel> channelType;
	
	/** The netty boss event loop group */
	protected final EventLoopGroup bossGroup;
	/** The netty boss event loop group's executor and thread factory */
	protected final Executor bossExecutorThreadFactory;
	
	
	/** The netty worker event loop group */
	protected final EventLoopGroup workerGroup;
	/** The netty worker event loop group's executor and thread factory */
	protected final Executor workerExecutorThreadFactory;
	
	/** The netty pipeline factory */
	protected final PipelineFactory pipelineFactory;
	
	/** The server channel created on socket bind */
	protected Channel serverChannel = null;
	
	/** The server's close future */
	protected ChannelFuture closeFuture = null;
	
	// =============================================
	// Channel Configs
	// =============================================
	/** The size of the server socket's backlog queue */
	protected final int backlog;
	/** Indicates if reuse address should be enabled */
	protected final boolean reuseAddress;
	/** The server's connect timeout in ms */
	protected final int connectTimeout;
	
	
	// =============================================
	// Child Channel Configs
	// =============================================
	/** Indicates if tcp no delay should be enabled */
	protected final boolean tcpNoDelay;
	/** Indicates if tcp keep alive should be enabled */
	protected final boolean keepAlive;
	/** The write spin count */
	protected final int writeSpins;
	/** The size of a channel's receive buffer in bytes */
	protected final int recvBuffer;
	/** The size of a channel's send buffer in bytes */
	protected final int sendBuffer;
	
	/** The server URI */
	public final URI serverURI;
	
	
	
	/** The boot reference */
	static OnRampBoot boot = null;
	/** The boot configs */
	static Properties bootConfig = null;
	/** The Spring Application */
	static SpringApplication springApp = null;
	/** The Spring Application Context */
	static ConfigurableApplicationContext appCtx = null;
	
	/**
	 * Main boot entry point
	 * @param args One of:<ul>
	 * 	<li><b>--admin=&lt;The admin server URL&gt;</li>
	 *  <li><b>--config=&lt;The URL of config properties&gt;</li>
	 * </ul>
	 */
	public static void main(final String[] args) {
		System.setProperty("java.net.preferIPv4Stack", "true");
//		System.setProperty("io.netty.leakDetectionLevel", "PARANOID");
//		System.setProperty("buffers.leakdetection", "true");
		System.setProperty("spring.output.ansi.enabled", "DETECT");
//		System.setProperty("buffers.pooled", "false");
//		System.setProperty("buffers.direct", "false");
		System.setProperty("spring.boot.admin.client.enabled", "true");
		System.setProperty("info.version", "0.0.3a");
		System.setProperty("spring.boot.admin.client.name", "OnRamp");
//		System.setProperty("spring.boot.admin.url", "http://pdk-pt-cltsdb-05/streamhubadmin");
		ExtendedThreadManager.install();
		
//		=============================================================================
		final Logger log = LogManager.getLogger(OnRampBoot.class);
		final String adminUrl = findArg("--admin", null, args);
		final String configUrl = findArg("--config", null, args);
		if(adminUrl==null && configUrl==null) {
			bootConfig = URLHelper.readProperties(OnRampBoot.class.getClassLoader().getResource("defaultConfig.properties"));
			//System.getProperties().putAll(bootConfig);
		}
		springApp = new SpringApplication(OnRampBoot.class);
		bootConfig.setProperty("spring.boot.admin.url", "http://localhost:7560/streamhubadmin");
		springApp.setDefaultProperties(bootConfig);
		springApp.addListeners(new ApplicationListener<ContextRefreshedEvent>(){
			@Override
			public void onApplicationEvent(final ContextRefreshedEvent event) {
				log.info("\n\t==================================================\n\tOnRamp Started\n\t==================================================\n");
			}
		});
		springApp.addListeners(new ApplicationListener<ContextClosedEvent>(){
			@Override
			public void onApplicationEvent(final ContextClosedEvent event) {
				log.info("\n\t==================================================\n\tOnRamp Stopped\n\t==================================================\n");											
			}
		});
//		springApp.setResourceLoader(resourceLoader);
//		springApp.setDefaultProperties(p);
		
//		springApp.setDefaultProperties(p);
//		StandardEnvironment environment = new StandardEnvironment();
		
		Thread springBootLaunchThread = new Thread("SpringBootLaunchThread") {
			public void run() {
				appCtx = springApp.run(args);
			}
		};
		springBootLaunchThread.setContextClassLoader(OnRampBoot.class.getClassLoader());
		springBootLaunchThread.setDaemon(true);
		springBootLaunchThread.start();
		
		log.info("Starting StdIn Handler");
		final Thread MAIN = Thread.currentThread();
		StdInCommandHandler.getInstance().registerCommand("shutdown", new Runnable(){
			public void run() {
				log.info("StdIn Handler Shutting Down AppCtx....");
//				Thread stopThread = new Thread("ShutdownThread") {
//					public void run() {
//						
//					}
//				};
				SpringApplication.exit(appCtx, new ExitCodeGenerator(){
					@Override
					public int getExitCode() {
						return 1;
					}});
				
				MAIN.interrupt();
				
			}
		}).runAsync(true).join();
		
//		
//		
//		final Properties p = new Properties();
//		boot = new OnRampBoot(p);
		
	}
	
	private final Thread shutdownHook = new Thread() {
		public void run() {
			
			if(workerGroup!=null) {
				serverChannel.close().syncUninterruptibly();
				log.info(">>>>> Shutting down OnRamp TCP Listener....");
				workerGroup.shutdownGracefully().syncUninterruptibly();
				log.info("<<<<< OnRamp TCP Listener Shutdown");
			}
		}
	};
	
	/**
	 * Creates a new OnRampBoot using the static bootConfig
	 */
	public OnRampBoot() {
		this(bootConfig);
	}
	
	
	/**
	 * Creates a new OnRampBoot
	 * @param appConfig  The application configuration
	 */
	public OnRampBoot(final Properties appConfig) {
		final String jmxmpUri = ConfigurationHelper.getSystemThenEnvProperty("jmx.jmxmp.uri", "jmxmp://0.0.0.0:1893", appConfig);
		JMXHelper.fireUpJMXMPServer(jmxmpUri);
		MessageForwarder.initialize(appConfig);
		port = ConfigurationHelper.getIntSystemThenEnvProperty("onramp.network.port", 8091, appConfig);
		bindInterface = ConfigurationHelper.getSystemThenEnvProperty("onramp.network.bind", "0.0.0.0", appConfig);
		bindSocket = new InetSocketAddress(bindInterface, port);
		workerThreads = ConfigurationHelper.getIntSystemThenEnvProperty("onramp.network.worker_threads", CORES * 2, appConfig);
		connectTimeout = ConfigurationHelper.getIntSystemThenEnvProperty("onramp.network.sotimeout", 0, appConfig);
		backlog = ConfigurationHelper.getIntSystemThenEnvProperty("onramp.network.backlog", 3072, appConfig);
		writeSpins = ConfigurationHelper.getIntSystemThenEnvProperty("onramp.network.writespins", 16, appConfig);
		recvBuffer = ConfigurationHelper.getIntSystemThenEnvProperty("onramp.network.recbuffer", 43690, appConfig);
		sendBuffer = ConfigurationHelper.getIntSystemThenEnvProperty("onramp.network.sendbuffer", 8192, appConfig);
		disableEpoll =  ConfigurationHelper.getBooleanSystemThenEnvProperty("onramp.network.epoll.disable", false, appConfig);
		async = ConfigurationHelper.getBooleanSystemThenEnvProperty("onramp.network.async_io", true, appConfig);
		tcpNoDelay = ConfigurationHelper.getBooleanSystemThenEnvProperty("onramp.network.tcp_no_delay", true, appConfig);
		keepAlive = ConfigurationHelper.getBooleanSystemThenEnvProperty("onramp.network.keep_alive", true, appConfig);
		reuseAddress = ConfigurationHelper.getBooleanSystemThenEnvProperty("onramp.network.reuse_address", true, appConfig);		
		pipelineFactory = new PipelineFactory(appConfig);
		serverBootstrap.handler(new LoggingHandler(getClass(), LogLevel.INFO));
		serverBootstrap.childHandler(pipelineFactory);
		// Set the child options
		serverBootstrap.childOption(ChannelOption.ALLOCATOR, BufferManager.getInstance().getAllocator());
		serverBootstrap.childOption(ChannelOption.TCP_NODELAY, tcpNoDelay);
		serverBootstrap.childOption(ChannelOption.SO_KEEPALIVE, keepAlive);
		serverBootstrap.childOption(ChannelOption.SO_RCVBUF, recvBuffer);
		serverBootstrap.childOption(ChannelOption.SO_SNDBUF, sendBuffer);
		serverBootstrap.childOption(ChannelOption.WRITE_SPIN_COUNT, writeSpins);
		// Set the server options
		serverBootstrap.option(ChannelOption.SO_BACKLOG, backlog);
		serverBootstrap.option(ChannelOption.SO_REUSEADDR, reuseAddress);
		serverBootstrap.option(ChannelOption.SO_RCVBUF, recvBuffer);
		serverBootstrap.option(ChannelOption.SO_TIMEOUT, connectTimeout);
		final StringBuilder uri = new StringBuilder("tcp");
		if(IS_LINUX && !disableEpoll) {
			bossExecutorThreadFactory = new ExecutorThreadFactory("EpollServerBoss", true);
			bossGroup = new EpollEventLoopGroup(1, (ThreadFactory)bossExecutorThreadFactory);
			workerExecutorThreadFactory = new ExecutorThreadFactory("EpollServerWorker", true);
			workerGroup = new EpollEventLoopGroup(workerThreads, (ThreadFactory)workerExecutorThreadFactory);
			channelType = EpollServerSocketChannel.class;
			uri.append("epoll");
		} else {
			bossExecutorThreadFactory = new ExecutorThreadFactory("NioServerBoss", true);
			bossGroup = new NioEventLoopGroup(1, bossExecutorThreadFactory);
			workerExecutorThreadFactory = new ExecutorThreadFactory("NioServerWorker", true);
			workerGroup = new NioEventLoopGroup(workerThreads, workerExecutorThreadFactory);
			channelType = NioServerSocketChannel.class;
			uri.append("nio");
		}
		
		uri.append("://").append(bindInterface).append(":").append(port);
		URI u = null;
		try {
			u = new URI(uri.toString());
		} catch (URISyntaxException e) {
			log.warn("Failed server URI const: [{}]. Programmer Error", uri, e);
		}
		serverURI = u;
		
		log.info(">>>>> Starting OnRamp TCP Listener on [{}]...", serverURI);
		final ChannelFuture cf = serverBootstrap
			.channel(channelType)
			.group(bossGroup, workerGroup)
			.bind(bindSocket)
			.awaitUninterruptibly()
			.addListener(new GenericFutureListener<Future<? super Void>>() {
				public void operationComplete(final Future<? super Void> f) throws Exception {
					log.info("<<<<< OnRamp TCP Listener on [{}] Started", serverURI);					
				};
			}).awaitUninterruptibly();
		serverChannel = cf.channel();
		closeFuture = serverChannel.closeFuture();
		Runtime.getRuntime().addShutdownHook(shutdownHook);
		
		
	}
		
	
	/**
	 * <p>Title: ExecutorThreadFactory</p>
	 * <p>Description: Combines an executor and thread factory</p> 
	 * <p>Company: Helios Development Group LLC</p>
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>net.opentsdb.tools.TSDTCPServer.ExecutorThreadFactory</code></p>
	 */
	public static class ExecutorThreadFactory implements Executor, ThreadFactory {
		final Executor executor;
		final ThreadFactory threadFactory;
		final String name;
		final AtomicInteger serial = new AtomicInteger();
		
		ExecutorThreadFactory(final String name, final boolean daemon) {
			this.name = name;
			threadFactory = new ThreadFactory() {
				@Override
				public Thread newThread(final Runnable r) {
					final Thread t = new Thread(r, name + "Thread#" + serial.incrementAndGet());
					t.setDaemon(daemon);
					return t;
				}
			};
			executor = Executors.newCachedThreadPool(threadFactory);
		}

		/**
		 * Executes the passed runnable in the executor
		 * @param command The runnable to execute
		 * @see java.util.concurrent.Executor#execute(java.lang.Runnable)
		 */
		@Override
		public void execute(final Runnable command) {
			executor.execute(command);
		}
		
		/**
		 * Creates a new thread
		 * {@inheritDoc}
		 * @see java.util.concurrent.ThreadFactory#newThread(java.lang.Runnable)
		 */
		@Override
		public Thread newThread(final Runnable r) {
			return threadFactory.newThread(r);
		}
	}
	
	
	
	private static void initPoolParam() {
		try {
			Class.forName("io.netty.buffer.PooledByteBufAllocator", true, OnRampBoot.class.getClassLoader());
		} catch (Exception ex) {
			throw new RuntimeException("Failed to initialize pool params", ex);
		}
	}
	
	/**
	 * Finds a command line arg value
	 * @param prefix The prefix
	 * @param defaultValue The default value if not found
	 * @param args The command line args to search
	 * @return the value
	 */
	private static String findArg(final String prefix, final String defaultValue, final String[] args) {
		for(String s: args) {
			if(s.startsWith(prefix)) {
				s = s.replace(prefix, "").trim();
				return s;
			}
		}
		return defaultValue;
	}
	
	

}
