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
package com.heliosapm.streams.metrichub.tsdbplugin;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.DirectChannelBufferFactory;
import org.jboss.netty.buffer.HeapChannelBufferFactory;
import org.jboss.netty.buffer.ReadOnlyChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.heliosapm.streams.metrichub.HubManager;
import com.heliosapm.streams.metrichub.MetricsMetaAPI;
import com.heliosapm.utils.buffer.BufferManager;
import com.stumbleupon.async.Deferred;

import io.netty.buffer.ByteBuf;
import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.tsd.HttpRpcPlugin;
import net.opentsdb.tsd.HttpRpcPluginQuery;

/**
 * <p>Title: MetricsAPIHttpPlugin</p>
 * <p>Description: Initilizes the MetricsAPI service and registers handlers to serve the metrics api UI content.</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrichub.tsdbplugin.MetricsAPIHttpPlugin</code></p>
 */

public class MetricsAPIHttpPlugin extends HttpRpcPlugin {
	/** Instance logger */
	private final Logger log = LogManager.getLogger(getClass());
	/** The metric manager for this plugin */
	protected final PluginMetricManager metricManager = new PluginMetricManager(getClass().getSimpleName());
	/** A reference to the metrics api service instance */
	protected MetricsMetaAPI metricsMetaAPI = null;
	/** The JSON based remoting interface to the metrics api service */
	protected JSONMetricsAPIService jsonMetricsService = null;
	/** The parent TSDB instance */
	protected TSDB tsdb = null;
	/* The location of the metric api UI content */
	protected File metricUiContentDir = null;
	
	/** The cache of content */
	protected final Cache<String, ChannelBuffer> contentCache = CacheBuilder.newBuilder()
		.concurrencyLevel(Runtime.getRuntime().availableProcessors())
		.expireAfterAccess(1, TimeUnit.MINUTES)
		.initialCapacity(1024)
		.recordStats()
		.build();
	
	/** A heap buffer factory */
	protected static final HeapChannelBufferFactory heapBuffFactory = new HeapChannelBufferFactory();
	/** A direct buffer factory */
	protected static final DirectChannelBufferFactory directBuffFactory = new DirectChannelBufferFactory();
	
	/** The content base to load resources from the classpath */
	public static final String CONTENT_BASE = "/metricapi-ui";
	/** The UTF8 character set */
	public static final Charset UTF8 = Charset.forName("UTF8");
	private static final byte[] NOT_FOUND_BYTES = "NOT FOUND".getBytes(UTF8); 
	/** The cache entry for not found content lookups */
	public static final ChannelBuffer NOTFOUND_PLACEHOLDER = new ReadOnlyChannelBuffer(heapBuffFactory.getBuffer(NOT_FOUND_BYTES, 0, NOT_FOUND_BYTES.length));
	/** This class's class loader */
	public static final ClassLoader CL = MetricsAPIHttpPlugin.class.getClassLoader();

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.HttpRpcPlugin#initialize(net.opentsdb.core.TSDB)
	 */
	@Override
	public void initialize(final TSDB tsdb) {
		log.info(">>>>> Initializing MetricsAPIHttpPlugin....");
		this.tsdb = tsdb;
		final String _staticContentDirName = this.tsdb.getConfig().getDirectoryName("tsd.http.staticroot");
		final File _staticContentDir = new File(_staticContentDirName);
		if(!_staticContentDir.isDirectory()) {
			throw new IllegalArgumentException("No static content directory found (defined by tsd.http.staticroot or --staticroot=)");
		}
		metricUiContentDir = new File(_staticContentDir, "metricapi-ui");
		if(!metricUiContentDir.isDirectory()) {
			if(!metricUiContentDir.exists()) {
				if(!metricUiContentDir.mkdir()) throw new IllegalArgumentException("Failed to create metricapi-ui directory [" + metricUiContentDir + "]");
			}
			throw new IllegalArgumentException("Cannot create metricapi-ui directory [" + metricUiContentDir + "]");
		}
		metricsMetaAPI = HubManager.getInstance().getMetricMetaService();
		jsonMetricsService = new JSONMetricsAPIService(metricsMetaAPI);
		log.info("<<<<< MetricsAPIHttpPlugin Initialized.");
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.HttpRpcPlugin#shutdown()
	 */
	@Override
	public Deferred<Object> shutdown() {

		return Deferred.fromResult(null);
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.HttpRpcPlugin#version()
	 */
	@Override
	public String version() {
		return "2.2";
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.HttpRpcPlugin#collectStats(net.opentsdb.stats.StatsCollector)
	 */
	@Override
	public void collectStats(final StatsCollector collector) {
		metricManager.collectStats(collector);

	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.HttpRpcPlugin#getPath()
	 */
	@Override
	public String getPath() {
		return "/metricsapi";
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.HttpRpcPlugin#execute(net.opentsdb.core.TSDB, net.opentsdb.tsd.HttpRpcPluginQuery)
	 */
	@Override
	public void execute(final TSDB tsdb, final HttpRpcPluginQuery query) throws IOException {
		log.info("HTTP Query: [{}]", query.getQueryBaseRoute());
		final String baseURI = query.request().getUri();
		query.notFound();
//		if(CONTENT_BASE.equals(baseURI)) {
//			sendFile(CONTENT_BASE + "/index.html", 0);
//			return;
//		}
//	    final String uri = baseURI.replace("metricapi-ui/", "");
//	    if ("/favicon.ico".equals(uri)) {
//	      sendFile(staticDir 
//	          + "/favicon.ico", 31536000 /*=1yr*/);
//	      return;
//	    }
//	    if (uri.length() < 3) {  // Must be at least 3 because of the "/s/".
//	      throw new RuntimeException("URI too short <code>" + uri + "</code>");
//	    }
//	    // Cheap security check to avoid directory traversal attacks.
//	    // TODO(tsuna): This is certainly not sufficient.
//	    if (uri.indexOf("..", 3) > 0) {
//	      throw new RuntimeException("Malformed URI <code>" + uri + "</code>");
//	    }
//	    final int questionmark = uri.indexOf('?', 3);
//	    final int pathend = questionmark > 0 ? questionmark : uri.length();
//	    sendFile(contentDir + "/" 
//	                 + uri.substring(1, pathend),  // Drop the "/s"
//	                   uri.contains("nocache") ? 0 : 31536000 /*=1yr*/);
		

	}
	
	
	protected ChannelBuffer getContent(final String key) {
		try {
			return contentCache.get(key, new Callable<ChannelBuffer>(){
				@Override
				public ChannelBuffer call() throws Exception {					
					return readResource(CL.getResourceAsStream(key));
				}
			});
		} catch (Exception ex) {
			throw new RuntimeException();
		}
	}
	
	protected ChannelBuffer readResource(final InputStream is) {
		ChannelBufferOutputStream os = null;
		try {
			if(is==null) return NOTFOUND_PLACEHOLDER;
			final ChannelBuffer buff = directBuffFactory.getBuffer(is.available());
			os = new ChannelBufferOutputStream(buff);
			final byte[] b = new byte[2048];
			int bytesRead = -1;
			while((bytesRead = is.read(b))!=-1) {
				os.write(b, 0, bytesRead);
			}
			os.flush();
			return new ReadOnlyChannelBuffer(buff);
		} catch (Exception ex) { 
			return NOTFOUND_PLACEHOLDER;
		} finally {
			if(is!=null) try { is.close(); } catch (Exception x) {/* No Op */}
			if(os!=null) try { os.close(); } catch (Exception x) {/* No Op */}
		}
	}
	
	

	  /**
	   * Send a file (with zero-copy) to the client with a 200 OK status.
	   * This method doesn't provide any security guarantee.  The caller is
	   * responsible for the argument they pass in.
	   * @param path The path to the file to send to the client.
	   * @param max_age The expiration time of this entity, in seconds.  This is
	   * not a timestamp, it's how old the resource is allowed to be in the client
	   * cache.  See RFC 2616 section 14.9 for more information.  Use 0 to disable
	   * caching.
	   */
	  public void sendFile(final HttpRpcPluginQuery query, 
			  			   final String path,
	                       final int max_age) throws IOException {
	    sendFile(query, HttpResponseStatus.OK, path, max_age);
	  }

	  /**
	   * Send a file to the client.
	   * This method doesn't provide any security guarantee.  The caller is
	   * responsible for the argument they pass in.
	   * @param status The status of the request (e.g. 200 OK or 404 Not Found).
	   * @param path The path to the file to send to the client.
	   * @param max_age The expiration time of this entity, in seconds.  This is
	   * not a timestamp, it's how old the resource is allowed to be in the client
	   * cache.  See RFC 2616 section 14.9 for more information.  Use 0 to disable
	   * caching.
	   */
	  public void sendFile(final HttpRpcPluginQuery query, 
			  			   final HttpResponseStatus status,
	                       final String path,
	                       final int max_age) throws IOException {
	    if (max_age < 0) {
	      throw new IllegalArgumentException("Negative max_age=" + max_age
	                                         + " for path=" + path);
	    }
	    final Channel chan = query.channel();
	    if (!chan.isConnected()) {
	      query.done();
	      return;
	    }
	    final Map<String, List<String>> querystring = query.getQueryString();
	    
//	    RandomAccessFile file;
//	    try {
//	      file = new RandomAccessFile(path, "r");
//	    } catch (FileNotFoundException e) {
//	      log.warn("File not found: " + e.getMessage());
//	      if (querystring != null) {
//	        querystring.remove("png");  // Avoid potential recursion.
//	      }
//	      this.sendReply(HttpResponseStatus.NOT_FOUND, serializer.formatNotFoundV1());
//	      return;
//	    }
//	    final long length = file.length();
//	    {
//	      final String mimetype = guessMimeTypeFromUri(path);
//	      response.headers().set(HttpHeaders.Names.CONTENT_TYPE,
//	                         mimetype == null ? "text/plain" : mimetype);
//	      final long mtime = new File(path).lastModified();
//	      if (mtime > 0) {
//	        response.headers().set(HttpHeaders.Names.AGE,
//	                           (System.currentTimeMillis() - mtime) / 1000);
//	      } else {
//	        logWarn("Found a file with mtime=" + mtime + ": " + path);
//	      }
//	      response.headers().set(HttpHeaders.Names.CACHE_CONTROL,
//	                         "max-age=" + max_age);
//	      HttpHeaders.setContentLength(response, length);
//	      chan.write(response);
//	    }
//	    final DefaultFileRegion region = new DefaultFileRegion(file.getChannel(),
//	                                                           0, length);
//	    final ChannelFuture future = chan.write(region);
//	    future.addListener(new ChannelFutureListener() {
//	      public void operationComplete(final ChannelFuture future) {
//	        region.releaseExternalResources();
//	        done();
//	      }
//	    });
//	    if (!HttpHeaders.isKeepAlive(request)) {
//	      future.addListener(ChannelFutureListener.CLOSE);
//	    }
	  }
	  
	  
	  protected void loadContent() {
		    final String codeSourcePath = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
		    final File file = new File(codeSourcePath);
		    final boolean inJar = file.getName().toLowerCase().endsWith(".jar");
		    if(inJar) {
		    	unloadFromJar(file);
		    }
		    // metricUiContentDir
	  }
	  
	  /**
	   * Unloads the UI content from classpath/dir
	   * TODO: merge this with {@link #unloadFromJar(File)}
	   */
	  protected void unloadFromDir() {
		  final long startTime = System.currentTimeMillis();
		  int filesLoaded = 0;
		  int fileFailures = 0;
		  int fileNewer = 0;
		  long bytesLoaded = 0;
		  final URL url = getClass().getClassLoader().getResource(CONTENT_BASE);
		  final File file = new File(url.getFile());
		  if(!file.isDirectory()) throw new IllegalArgumentException("Cold not find content in path [" + file + "]");
		  
		  log.info("Loading MetricsAPI UI Content from Classpath Directory: [{}]", file);  
		  final ByteBuf contentBuffer = BufferManager.getInstance().directBuffer(30000);
		  try {
			  
//			  Dir: /home/nwhitehead/hprojects/HeliosStreams/metric-hub/src/main/resources/metricapi-ui
//			  Dir: /home/nwhitehead/hprojects/HeliosStreams/metric-hub/src/main/resources/metricapi-ui/jqueryui
//			  Dir: /home/nwhitehead/hprojects/HeliosStreams/metric-hub/src/main/resources/metricapi-ui/jqueryui/css
//			  Dir: /home/nwhitehead/hprojects/HeliosStreams/metric-hub/src/main/resources/metricapi-ui/jqueryui/css/pepper-grinder
//			  Dir: /home/nwhitehead/hprojects/HeliosStreams/metric-hub/src/main/resources/metricapi-ui/jqueryui/css/pepper-grinder/images
//			  Dir: /home/nwhitehead/hprojects/HeliosStreams/metric-hub/src/main/resources/metricapi-ui/jqueryui/js			  

			  Files.walkFileTree(file.toPath(), new FileVisitor<Path>(){
				@Override
				public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) throws IOException {
					
					return FileVisitResult.CONTINUE;
				}

				@Override
				public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
					return null;
				}

				@Override
				public FileVisitResult visitFileFailed(final Path file, final IOException exc) throws IOException {
					return null;
				}

				@Override
				public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) throws IOException {
					// TODO Auto-generated method stub
					return null;
				}
				  
			  });
			  final Path path = file.toPath();
			  
			  final Enumeration<JarEntry> entries = jar.entries(); 
			  while(entries.hasMoreElements()) {
				  JarEntry entry = entries.nextElement();
				  final String name = entry.getName();
				  if (name.startsWith(CONTENT_BASE + "/")) { 
					  final int contentSize = (int)entry.getSize();
					  final long contentTime = entry.getTime();
					  if(entry.isDirectory()) {
						  new File(metricUiContentDir, name).mkdirs();
						  continue;
					  }
					  File contentFile = new File(metricUiContentDir, name.replace(CONTENT_BASE + "/", ""));
					  if( !contentFile.getParentFile().exists() ) {
						  contentFile.getParentFile().mkdirs();
					  }
					  if( contentFile.exists() ) {
						  if( contentFile.lastModified() >= contentTime ) {
							  log.debug("File in directory was newer [{}]", name);
							  fileNewer++;
							  continue;
						  }
						  contentFile.delete();
					  }
					  log.debug("Writing content file [{}]", contentFile );
					  contentFile.createNewFile();
					  if( !contentFile.canWrite() ) {
						  log.warn("Content file [{}] not writable", contentFile);
						  fileFailures++;
						  continue;
					  }
					  FileOutputStream fos = null;
					  InputStream jis = null;
					  try {
						  fos = new FileOutputStream(contentFile);
						  jis = jar.getInputStream(entry);
						  contentBuffer.writeBytes(jis, contentSize);
						  contentBuffer.readBytes(fos, contentSize);
						  fos.flush();
						  jis.close(); jis = null;
						  fos.close(); fos = null;
						  filesLoaded++;
						  bytesLoaded += contentSize;
						  log.debug("Wrote content file [{}] + with size [{}]", contentFile, contentSize );
					  } finally {
						  if( jis!=null ) try { jis.close(); } catch (Exception ex) {}
						  if( fos!=null ) try { fos.close(); } catch (Exception ex) {}
						  contentBuffer.clear();
					  }
				  }  // not content
			  } // end of while loop
			  final long elapsed = System.currentTimeMillis()-startTime;
			  StringBuilder b = new StringBuilder("\n\n\t===================================================\n\tMetricsAPI Content Directory:[").append(metricUiContentDir).append("]");
			  b.append("\n\tTotal Files Written:").append(filesLoaded);
			  b.append("\n\tTotal Bytes Written:").append(bytesLoaded);
			  b.append("\n\tFile Write Failures:").append(fileFailures);
			  b.append("\n\tExisting File Newer Than Content:").append(fileNewer);
			  b.append("\n\tElapsed (ms):").append(elapsed);
			  b.append("\n\t===================================================\n");
			  log.info(b.toString());
		  } catch (Exception ex) {
			  log.error("Failed to export MetricsAPI content", ex);       
		  } finally {
			  if( jar!=null ) try { jar.close(); } catch (Exception x) { /* No Op */}
			  try { contentBuffer.release(); } catch (Exception x) {/* No Op */}
		  }
	  }


	  /**
	   * Unloads the UI content from a jar
	   * @param file The jar file
	   * TODO: merge this with {@link #unloadFromDir(File)}
	   */
	  protected void unloadFromJar(final File file) {
		  log.info("Loading MetricsAPI UI Content from JAR: [{}]", file);
		  final long startTime = System.currentTimeMillis();
		  int filesLoaded = 0;
		  int fileFailures = 0;
		  int fileNewer = 0;
		  long bytesLoaded = 0;

		  JarFile jar = null;
		  final ByteBuf contentBuffer = BufferManager.getInstance().directBuffer(30000);
		  try {
			  jar = new JarFile(file);
			  final Enumeration<JarEntry> entries = jar.entries(); 
			  while(entries.hasMoreElements()) {
				  JarEntry entry = entries.nextElement();
				  final String name = entry.getName();
				  if (name.startsWith(CONTENT_BASE + "/")) { 
					  final int contentSize = (int)entry.getSize();
					  final long contentTime = entry.getTime();
					  if(entry.isDirectory()) {
						  new File(metricUiContentDir, name).mkdirs();
						  continue;
					  }
					  File contentFile = new File(metricUiContentDir, name.replace(CONTENT_BASE + "/", ""));
					  if( !contentFile.getParentFile().exists() ) {
						  contentFile.getParentFile().mkdirs();
					  }
					  if( contentFile.exists() ) {
						  if( contentFile.lastModified() >= contentTime ) {
							  log.debug("File in directory was newer [{}]", name);
							  fileNewer++;
							  continue;
						  }
						  contentFile.delete();
					  }
					  log.debug("Writing content file [{}]", contentFile );
					  contentFile.createNewFile();
					  if( !contentFile.canWrite() ) {
						  log.warn("Content file [{}] not writable", contentFile);
						  fileFailures++;
						  continue;
					  }
					  FileOutputStream fos = null;
					  InputStream jis = null;
					  try {
						  fos = new FileOutputStream(contentFile);
						  jis = jar.getInputStream(entry);
						  contentBuffer.writeBytes(jis, contentSize);
						  contentBuffer.readBytes(fos, contentSize);
						  fos.flush();
						  jis.close(); jis = null;
						  fos.close(); fos = null;
						  filesLoaded++;
						  bytesLoaded += contentSize;
						  log.debug("Wrote content file [{}] + with size [{}]", contentFile, contentSize );
					  } finally {
						  if( jis!=null ) try { jis.close(); } catch (Exception ex) {}
						  if( fos!=null ) try { fos.close(); } catch (Exception ex) {}
						  contentBuffer.clear();
					  }
				  }  // not content
			  } // end of while loop
			  final long elapsed = System.currentTimeMillis()-startTime;
			  StringBuilder b = new StringBuilder("\n\n\t===================================================\n\tMetricsAPI Content Directory:[").append(metricUiContentDir).append("]");
			  b.append("\n\tTotal Files Written:").append(filesLoaded);
			  b.append("\n\tTotal Bytes Written:").append(bytesLoaded);
			  b.append("\n\tFile Write Failures:").append(fileFailures);
			  b.append("\n\tExisting File Newer Than Content:").append(fileNewer);
			  b.append("\n\tElapsed (ms):").append(elapsed);
			  b.append("\n\t===================================================\n");
			  log.info(b.toString());
		  } catch (Exception ex) {
			  log.error("Failed to export MetricsAPI content", ex);       
		  } finally {
			  if( jar!=null ) try { jar.close(); } catch (Exception x) { /* No Op */}
			  try { contentBuffer.release(); } catch (Exception x) {/* No Op */}
		  }
	  }
	
}
