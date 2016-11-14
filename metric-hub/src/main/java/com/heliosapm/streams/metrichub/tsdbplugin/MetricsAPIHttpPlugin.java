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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.DefaultFileRegion;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import com.heliosapm.streams.metrichub.HubManager;
import com.heliosapm.streams.metrichub.MetricsMetaAPI;
import com.stumbleupon.async.Deferred;

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
	/** The metric api content directory */
	protected String contentDir = null;
	/** The native opentsdb content directory */
	protected String staticDir = null;
	

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.HttpRpcPlugin#initialize(net.opentsdb.core.TSDB)
	 */
	@Override
	public void initialize(final TSDB tsdb) {
		log.info(">>>>> Initializing MetricsAPIHttpPlugin....");
		this.tsdb = tsdb;
		staticDir = this.tsdb.getConfig().getDirectoryName("tsd.http.staticroot");
		contentDir = System.getProperty("metricui.staticroot", staticDir);
		
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
		if("/metricapi-ui".equals(baseURI)) {
			sendFile(contentDir + "/index.html", 0);
			return;
		}
	    final String uri = baseURI.replace("metricapi-ui/", "");
	    if ("/favicon.ico".equals(uri)) {
	      sendFile(staticDir 
	          + "/favicon.ico", 31536000 /*=1yr*/);
	      return;
	    }
	    if (uri.length() < 3) {  // Must be at least 3 because of the "/s/".
	      throw new RuntimeException("URI too short <code>" + uri + "</code>");
	    }
	    // Cheap security check to avoid directory traversal attacks.
	    // TODO(tsuna): This is certainly not sufficient.
	    if (uri.indexOf("..", 3) > 0) {
	      throw new RuntimeException("Malformed URI <code>" + uri + "</code>");
	    }
	    final int questionmark = uri.indexOf('?', 3);
	    final int pathend = questionmark > 0 ? questionmark : uri.length();
	    sendFile(contentDir + "/" 
	                 + uri.substring(1, pathend),  // Drop the "/s"
	                   uri.contains("nocache") ? 0 : 31536000 /*=1yr*/);
		

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
	  public void sendFile(final String path,
	                       final int max_age) throws IOException {
	    sendFile(HttpResponseStatus.OK, path, max_age);
	  }

	  /**
	   * Send a file (with zero-copy) to the client.
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
	    RandomAccessFile file;
	    try {
	      file = new RandomAccessFile(path, "r");
	    } catch (FileNotFoundException e) {
	      log.warn("File not found: " + e.getMessage());
	      if (querystring != null) {
	        querystring.remove("png");  // Avoid potential recursion.
	      }
	      this.sendReply(HttpResponseStatus.NOT_FOUND, serializer.formatNotFoundV1());
	      return;
	    }
	    final long length = file.length();
	    {
	      final String mimetype = guessMimeTypeFromUri(path);
	      response.headers().set(HttpHeaders.Names.CONTENT_TYPE,
	                         mimetype == null ? "text/plain" : mimetype);
	      final long mtime = new File(path).lastModified();
	      if (mtime > 0) {
	        response.headers().set(HttpHeaders.Names.AGE,
	                           (System.currentTimeMillis() - mtime) / 1000);
	      } else {
	        logWarn("Found a file with mtime=" + mtime + ": " + path);
	      }
	      response.headers().set(HttpHeaders.Names.CACHE_CONTROL,
	                         "max-age=" + max_age);
	      HttpHeaders.setContentLength(response, length);
	      chan.write(response);
	    }
	    final DefaultFileRegion region = new DefaultFileRegion(file.getChannel(),
	                                                           0, length);
	    final ChannelFuture future = chan.write(region);
	    future.addListener(new ChannelFutureListener() {
	      public void operationComplete(final ChannelFuture future) {
	        region.releaseExternalResources();
	        done();
	      }
	    });
	    if (!HttpHeaders.isKeepAlive(request)) {
	      future.addListener(ChannelFutureListener.CLOSE);
	    }
	  }
	
}
