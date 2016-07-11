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

import java.io.File;
import java.io.FileFilter;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedMetric;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedOperationParameter;
import org.springframework.jmx.export.annotation.ManagedOperationParameters;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.jmx.support.MetricType;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.codahale.metrics.CachedGauge;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.io.Files;
import com.heliosapm.utils.lang.StringHelper;
import com.heliosapm.utils.url.URLHelper;

/**
 * <p>Title: NodeConfigurationServer</p>
 * <p>Description: The endpoint that responds to worker nodes requesting marching orders</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.admin.nodes.NodeConfigurationServer</code></p>
 * FIXME: cache resources
 */
@RestController
@RequestMapping(value="/nodeconfig")
@Configuration
@Component
@ManagedResource(
		objectName="com.heliosapm.streams.admin:service=NodeConfigurationServer", 
		description="The endpoint that responds to worker nodes requesting marching orders"		
)

public class NodeConfigurationServer implements InitializingBean {
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(NodeConfigurationServer.class);
	
	
	
	/** The configuration directory name */
	@Value("${workers.nodes.config.dir}")
	protected String configDirName = null;
	/** The application jar directory name */
	@Value("${workers.nodes.app.dir}")
	protected String appDirName = null;
	
	/** The content cache spec */
	@Value("${workers.nodes.config.cachespec}")
	protected String cacheSpec = null;
	
	/** The absolute config directory */
	protected File configDir = null;
	/** The absolute app directory */
	protected File appDir = null;
	
	
	/** The config cache */
	protected LoadingCache<String, KeyedFileContent> configCache;
	/** The app jar cache */
	protected LoadingCache<String, Map<String, KeyedFileContent>> appJarCache;

	
	/** Timed gauge to cache the config cache stats */
	protected CachedGauge<CacheStats> configCacheStats = null;
	/** Timed gauge to cache the app jar cache stats */
	protected CachedGauge<CacheStats> appJarCacheStats = null;
	
	/** The shared json node factory */
	protected final JsonNodeFactory nodeFactory = JsonNodeFactory.instance;
	/** The shared json object mapper */
	protected final ObjectMapper jsonMapper = new ObjectMapper();
	
	/** The config cache loader */
	protected CacheLoader<String, KeyedFileContent> configCacheLoader = new CacheLoader<String, KeyedFileContent>() {
		@Override
		public KeyedFileContent load(final String key) throws Exception {
			return new KeyedFileContent(getConfigFileForKey(key));
		}		
	};
	/** The app jar cache loader */
	protected CacheLoader<String, Map<String, KeyedFileContent>> appJarCacheLoader = new CacheLoader<String, Map<String, KeyedFileContent>>() {
		@Override
		public Map<String, KeyedFileContent> load(final String key) throws Exception {
			return KeyedFileContent.forFiles(
					getAppJarForKey(key)
			);
		}		
	};
	
	
	/**
	 * Retrieves the configuration for the passed host and app
	 * @param host The requesting host
	 * @param appname The requested app for which configuration should be delivered
	 * @return a properties file in string format
	 */
	@RequestMapping(value="/{host}/{appname}.properties", method=RequestMethod.GET, produces={"text/x-java-properties"})	
	public String getConfigurationProperties(@PathVariable final String host, @PathVariable final String appname) {
		final String _host = host.toLowerCase().trim().split("\\.")[0];
		final String _appname = appname.toLowerCase().trim()  + ".properties";
		final String key = _host + "/" + _appname;
		log.info("Fetching config for [{}]", key);
		return getConfigContent(key);
	}
	
	/**
	 * Retrieves the resource requirements for the passed app
	 * @param appname The requested app for which configuration should be delivered
	 * @return a JSON document describing each resource required
	 */
	@RequestMapping(value="/{appname}", method=RequestMethod.GET, produces={"application/json"})	
	public String getRequiredResources(@PathVariable final String appname) {
		final String _appname = appname.toLowerCase().trim();
		log.info("Fetching resources for [{}]", _appname);
		return getAppJarsMeta(_appname);
	}
	
	/**
	 * Returns the app resource for the passed app and resource name
	 * @param appname The app name
	 * @param resourceName The resource name
	 * @return The resource bytes
	 */
	@RequestMapping(value="/resource/{appname}/{resourceName}", method=RequestMethod.GET, produces={"application/java-archive"})
	public byte[] getResource(@PathVariable final String appname, @PathVariable final String resourceName) {
		final String _appname = appname.toLowerCase().trim();
		log.info("Fetching resource for [{}/{}]", _appname, resourceName);
		final File d = new File(new File(appDir, _appname), resourceName);
		return URLHelper.getBytesFromURL(URLHelper.toURL(d));
	}
	

	
	/**
	 * Returns the content for the passed key, reloading if it has expired
	 * @param key the key to get content for
	 * @return the content
	 */
	protected String getConfigContent(final String key) {
		try {
			KeyedFileContent k = configCache.get(key);
			if(k.isExpired()) {
				configCache.invalidate(key);
				k = configCache.get(key);
				log.info("Reloaded [{}]", key);
			}
			return k.getTextContent();
		} catch (Exception ex) {
			throw new RuntimeException("Failed to get content for [" + key + "]");
		}
	}
	

	/**
	 * Returns the app jar meta json for the passed key, reloading if it has expired
	 * @param key the app name to get content for
	 * @return the app jar meta json
	 */
	protected String getAppJarsMeta(final String key) {
		try {
			Map<String, KeyedFileContent> kfs = appJarCache.get(key);
			for(KeyedFileContent k: kfs.values()) {
				if(k.isExpired()) {
					appJarCache.invalidate(key);
					kfs = appJarCache.get(key);
					log.info("Reloaded App Jar Cache for [{}]", key);
					break;
				}
			}
			final ArrayNode arrNode = nodeFactory.arrayNode();
			for(KeyedFileContent k: kfs.values()) {
				final ObjectNode on = nodeFactory.objectNode();
				on.put("resource", k.name());
				on.put("sha", k.getContent());
				arrNode.add(on);
			}
			try {
				return jsonMapper.writeValueAsString(arrNode);
			} catch (JsonProcessingException jpe) {
				throw new RuntimeException("Failed to render resource json for [" + key + "]", jpe);
			}
			
		} catch (ExecutionException ex) {
			throw new RuntimeException("Failed to get content for [" + key + "]");
		}
	}
	
	/**
	 * Finds the file for the passed host/app key
	 * @param contentType The content type
	 * @param key The key to find the file for
	 * @return the file
	 */
	protected File getContentFileForKey(final ContentType contentType, final String key) {
		final String[] segments = StringHelper.splitString(key, '/', true);
		final String _host = segments[0];
		final String _appname = segments[1];
		File hostDir = new File(configDir, _host);
		if(!hostDir.isDirectory()) {
			hostDir = new File(configDir, "default");
			if(!hostDir.isDirectory()) {
				final String msg = "Failed to find host directory for [" + _host + "] or default";
				log.error(msg);
				throw new RuntimeException(msg);
			}
		}
		File appFile = new File(hostDir, _appname);
		if(!appFile.isFile()) {
			appFile = new File(hostDir, "default.properties");
			if(!appFile.isFile()) {
				final String msg = "Failed to find app config in [" + hostDir + "] for [" + _appname + "] or default.properties";
				log.error(msg);
				throw new RuntimeException(msg);
			}
		}
		return appFile;
	}
	
	/**
	 * Finds the app jars for the passed app key
	 * @param key The app to find the files for
	 * @return the files
	 */
	protected File[] getAppJarForKey(final String key) {
		final File dir = new File(appDir, key);
		return dir.listFiles(new FileFilter(){
			@Override
			public boolean accept(final File f) {
				return f.isFile() && f.getName().endsWith(".jar");
			}
		});
	}
	
	/**
	 * Finds the config file for the passed host/app key
	 * @param key The key to find the file for
	 * @return the file
	 */
	protected File getConfigFileForKey(final String key) {		
		final String[] segments = StringHelper.splitString(key, '/', true);
		final String _host = segments[0];
		final String _appname = segments[1];
		File hostDir = new File(configDir, _host);
		if(!hostDir.isDirectory()) {
			hostDir = new File(configDir, "default");
			if(!hostDir.isDirectory()) {
				final String msg = "Failed to find host directory for [" + _host + "] or default";
				log.error(msg);
				throw new RuntimeException(msg);
			}
		}
		File appFile = new File(hostDir, _appname);
		if(!appFile.isFile()) {
			appFile = new File(hostDir, "default.properties");
			if(!appFile.isFile()) {
				final String msg = "Failed to find app config in [" + hostDir + "] for [" + _appname + "] or default.properties";
				log.error(msg);
				throw new RuntimeException(msg);
			}
		}
		return appFile;
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() throws Exception {		
		configDir = new File(configDirName).getAbsoluteFile();
		appDir = new File(appDirName).getAbsoluteFile();
		if(!configDir.isDirectory()) throw new IllegalArgumentException("The configuration directory [" + configDirName + "] is invalid");
		if(!appDir.isDirectory()) throw new IllegalArgumentException("The app directory [" + appDirName + "] is invalid");
		log.info("Configuration Directory: [{}]", configDir);
		if(!cacheSpec.contains("recordStats")) cacheSpec = cacheSpec + ",recordStats";
		
		configCache = CacheBuilder.from(cacheSpec).build(configCacheLoader);
		configCacheStats = new CachedGauge<CacheStats>(5, TimeUnit.SECONDS) {
			@Override
			protected CacheStats loadValue() {				
				return configCache.stats();
			}
		};
		appJarCache = CacheBuilder.from(cacheSpec).build(appJarCacheLoader);
		appJarCacheStats = new CachedGauge<CacheStats>(5, TimeUnit.SECONDS) {
			@Override
			protected CacheStats loadValue() {				
				return appJarCache.stats();
			}
		};
		
		reloadConfigCache();
		log.info("Loaded [{}] App Configurations", configCache.size());
		reloadAppJarCache();
		log.info("Loaded [{}] App Jar Sets", appJarCache.size());
	}
	
	/**
	 * Reloads the config cache
	 */
	protected void reloadConfigCache() {
		for(File f : Files.fileTreeTraverser().preOrderTraversal(configDir)) {
			if(!f.getName().endsWith(".properties"))  continue;
			final String key = new StringBuilder(f.getParentFile().getName()).append("/").append(f.getName()).toString();			
			configCache.put(key, new KeyedFileContent(f));
		}		
	}
	
	/**
	 * Reloads the app jar cache
	 */
	protected void reloadAppJarCache() {
		for(File app : appDir.listFiles()) {
			if(!app.isDirectory()) continue;
			final String appName = app.getName();
			final File[] jars = app.listFiles(new FileFilter(){
				@Override
				public boolean accept(final File fx) {				
					return fx.isFile() && fx.getName().endsWith(".jar");
				}
			});
			appJarCache.put(appName, KeyedFileContent.forFiles(jars));
		}		
	}
	
	/**
	 * Returns the average time spent loading new values.
	 * @return the average time spent loading new values.
	 */
	@ManagedMetric(category="NodeConfiguration", description="The average time spent loading new values", metricType=MetricType.GAUGE, unit="ns.")
	public double getAverageLoadPenalty() {
		return configCacheStats.getValue().averageLoadPenalty();
	}
	
	/**
	 * Returns the cache hit count
	 * @return the cache hit count
	 */
	@ManagedMetric(category="NodeConfiguration", description="The cache hit count", metricType=MetricType.COUNTER, unit="cache-hits")
	public long getHitCount() {
		return configCacheStats.getValue().hitCount();
	}
	
	/**
	 * Returns the cache miss count
	 * @return the cache miss count
	 */
	@ManagedMetric(category="NodeConfiguration", description="The cache miss count", metricType=MetricType.COUNTER, unit="cache-misses")
	public long getMissCount() {
		return configCacheStats.getValue().missCount();
	}
	
	/**
	 * Returns the cache size
	 * @return the cache size
	 */
	@ManagedMetric(category="NodeConfiguration", description="The cache size", metricType=MetricType.GAUGE, unit="cache-entries")
	public long getCacheSize() {
		return configCache.size();
	}
	
	/**
	 * Returns the cache load exception count
	 * @return the cache load exception count
	 */
	@ManagedMetric(category="NodeConfiguration", description="The cache load exception count", metricType=MetricType.COUNTER, unit="cache-load exceptions")
	public long getLoadExceptionCount() {
		return configCacheStats.getValue().loadExceptionCount();
	}
	
	/**
	 * Returns the cache request count
	 * @return the cache request count
	 */
	@ManagedMetric(category="NodeConfiguration", description="The cache request count", metricType=MetricType.COUNTER, unit="cache-requests")
	public long getRequestCount() {
		return configCacheStats.getValue().requestCount();
	}
	
	
	/**
	 * Returns the cache keys
	 * @return the cache keys
	 */
	@ManagedOperation(description="Returns the cache keys")
	public Set<String> cacheKeys() {
		return new HashSet<String>(configCache.asMap().keySet());		
	}
	
	/**
	 * Invalidates the whole cache
	 */
	@ManagedOperation(description="Invalidates the whole cache")
	public void invalidateCache() {
		configCache.invalidateAll();
	}
	
	/**
	 * Invalidates the cache configuration for the passed host/app
	 * @param host The host
	 * @param app The app
	 */
	@ManagedOperation(description="Invalidates the cache configuration for the passed host/app")
	@ManagedOperationParameters({
		@ManagedOperationParameter(name="host", description="The host to invalidate the cache entry for"),
		@ManagedOperationParameter(name="app", description="The app to invalidate the cache entry for")
	})
	public void invalidate(final String host, final String app) {
		final String key = host.trim().toLowerCase() + "/" + app.trim().toLowerCase();
		configCache.invalidate(key);
	}
	
	
	/**
	 * Reloads the cache from the config configDir
	 * @param clearFirst if true, the cache will be invalidated first
	 * @return the number of entries in the cache after this op completes
	 */
	@ManagedOperation(description="Reloads the cache from the config configDir. Returns the number of entries in the cache after this op completes.")
	@ManagedOperationParameters({
		@ManagedOperationParameter(name="clearFirst", description="If true, the cache will be invalidated first")
	})	
	public long reloadconfigCache(final boolean clearFirst) {
		if(clearFirst) configCache.invalidateAll();
		reloadConfigCache();
		log.info("Loaded [{}] KeyedFileContents", configCache.size());
		return configCache.size();
	}
	
	/**
	 * Returns the configuration directory
	 * @return the configuration directory
	 */
	@ManagedAttribute(description="The configuration directory")
	public String getConfigDirectory() {
		return configDir.getAbsolutePath();
	}
	
	/**
	 * Returns the application directory
	 * @return the application directory
	 */
	@ManagedAttribute(description="The application directory")
	public String getAppDirectory() {
		return appDir.getAbsolutePath();
	}
	
	
	static class KeyedFileContent {
		final long timestamp;
		final byte[] content;
		final File file;
		final byte[] sha;
		
		private static final Map<String, KeyedFileContent> EMPTY_MAP = Collections.unmodifiableMap(new HashMap<String, KeyedFileContent>(0));
		private static final Charset UTF8 = Charset.forName("UTF8");
		/**
		 * Creates a new KeyedFileContent
		 * @param f The file the text came from
		 */
		public KeyedFileContent(final File f) {
			this.file = f;
			this.timestamp = f.lastModified();
			this.content = URLHelper.getBytesFromURL(URLHelper.toURL(this.file));
			this.sha = URLHelper.hashSHA(f.getAbsolutePath());
		}
		
		public static Map<String, KeyedFileContent> forFiles(final File...files) {
			if(files==null || files.length==0) return EMPTY_MAP;
			final Map<String, KeyedFileContent> map = new LinkedHashMap<String, KeyedFileContent>(files.length);
			for(File f: files) {
				if(f==null || !f.exists() || !f.isFile()) continue;
				map.put(f.getName(), new KeyedFileContent(f));
			}
			return map;
		}
		
		public File file() {
			return file;
		}
		
		public String name() {
			return file.getName();
		}
		
		public boolean isExpired() {
			return file.lastModified() > timestamp;
		}
		
		public String getTextContent() {
			return new String(content, UTF8);
		}
		
		public byte[] getContent() {
			return content;
		}
		
		
		public byte[] getSHA() {
			return sha.clone();
		}

		/**
		 * {@inheritDoc}
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((file == null) ? 0 : file.hashCode());
			return result;
		}

		/**
		 * {@inheritDoc}
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			KeyedFileContent other = (KeyedFileContent) obj;
			if (file == null) {
				if (other.file != null)
					return false;
			} else if (!file.equals(other.file))
				return false;
			return true;
		}
	}

}
