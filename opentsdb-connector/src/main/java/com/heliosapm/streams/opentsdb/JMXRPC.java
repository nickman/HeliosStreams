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
package com.heliosapm.streams.opentsdb;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import org.cliffc.high_scale_lib.NonBlockingHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.heliosapm.utils.concurrency.ExtendedThreadManager;
import com.heliosapm.utils.jmx.JMXHelper;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.TimeoutException;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.tsd.RpcPlugin;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.Config;

/**
 * <p>Title: JMXRPC</p>
 * <p>Description: An OpenTSDB RPC plugin to expose some basic OpenTSDB management operations via JMX</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.opentsdb.JMXRPC</code></p>
 */

public class JMXRPC extends RpcPlugin implements JMXRPCMBean {
	private static final Logger LOG = LoggerFactory.getLogger(JMXRPC.class);
	
	
	/** The injected TSDB instance */
	protected TSDB tsdb = null;
	/** The injected TSDB instance's config */
	protected Config config = null;
	/** The JMXMP connector servers */
	protected Map<String, JMXConnectorServer> connectorServers = new HashMap<String, JMXConnectorServer>();
	/** The JMX ObjectNames of the registered connector servers */
	protected final NonBlockingHashSet<ObjectName> objectNames = new NonBlockingHashSet<ObjectName>(); 
	/** The default timeout for async calls in ms */
	protected long defaultTimeout = DEFAULT_ASYNC_TIMEOUT;
	/**
	 * Creates a new JMXRPC
	 */
	public JMXRPC() {
		
	}

	// javax.management.remote.jmxmp:service=JMXMPConnectorServer
	 
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RpcPlugin#initialize(net.opentsdb.core.TSDB)
	 */
	@Override
	public void initialize(final TSDB tsdb) {
		LOG.info("Initializing JMXRPC plugin....");
		ExtendedThreadManager.install();
		this.tsdb = tsdb;
		config = tsdb.getConfig();
		final Map<String, String> configMap = config.getMap();
		StringBuilder b= new StringBuilder();
		for(Map.Entry<String, String> entry: configMap.entrySet()) {
			if(entry.getKey().trim().toLowerCase().startsWith(CONFIG_JMX_URLS)) {
				final String url = configMap.get(entry.getValue().trim());
				final JMXServiceURL serviceURL = toJMXServiceURL(url);
				final JMXConnectorServer connectorServer = connector(serviceURL);
				connectorServers.put(url, connectorServer);
				b.append("\n\t").append(url);
			}
		}
		if(connectorServers.isEmpty()) {
			final JMXServiceURL serviceURL = toJMXServiceURL(DEFAULT_JMX_URLS);
			final JMXConnectorServer connectorServer = connector(serviceURL);			
			connectorServers.put(DEFAULT_JMX_URLS, connectorServer);
			b.append("\n\t").append(DEFAULT_JMX_URLS);
		}
		startJMXServers();
		JMXHelper.registerMBean(this, OBJECT_NAME);
		LOG.info("Initialized JMXRPC plugin. JMXConnectorServers installed and started at:{}", b.toString());
	}
	
	private void startJMXServers() {
		final AtomicBoolean started = new AtomicBoolean(false);
		final Thread t = new Thread("JMXConnectorServerStarterDaemon") {
			@Override
			public void run() {
				for(Map.Entry<String, JMXConnectorServer> entry: new HashMap<String, JMXConnectorServer>(connectorServers).entrySet()){
					try {
						entry.getValue().start();
						LOG.info("Started JMXConnectorServer: [{}]", entry.getValue().getAddress());
						try {
							final int port = entry.getValue().getAddress().getPort();
							final ObjectName on = JMXHelper.objectName("javax.management.remote.jmxmp:service=JMXMPConnectorServer,port=" + port);
							JMXHelper.registerMBean(entry.getValue(), on);
							objectNames.add(on);
						} catch (Exception ex) {
							LOG.warn("Failed to register MBean for JMXMP Connector [{}]", entry.getValue().getAddress());
						}
					} catch (Exception ex) {
						LOG.error("Failed to start JMXConnectorServer [{}]", entry.getKey(), ex);
						connectorServers.remove(entry.getKey());
					}
				}
				started.set(true);
			}
		};
		t.setDaemon(true);
		t.start();
		try {
			t.join(5000);
		} catch (InterruptedException e) {
			/* No Op */
		}
		if(!started.get()) {
			LOG.warn("Not all JMXConnectorServers Started yet");
		}
	}
	
	private JMXServiceURL toJMXServiceURL(final String url) {
		try {
			return new JMXServiceURL(url);
		} catch (Exception ex) {
			throw new IllegalArgumentException("Invalid JMXServiceURL: [" + url + "]", ex);
		}		
	}
	
	private JMXConnectorServer connector(final JMXServiceURL serviceURL) {
		try {
			return JMXConnectorServerFactory.newJMXConnectorServer(serviceURL, null, ManagementFactory.getPlatformMBeanServer());
		} catch (Exception ex) {
			throw new IllegalArgumentException("Unable to create connector server for JMXServiceURL: [" + serviceURL+ "]", ex);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RpcPlugin#shutdown()
	 */
	@Override
	public Deferred<Object> shutdown() {
		for(JMXConnectorServer server: connectorServers.values()) {
			try { server.stop(); } catch (Exception x) {/* No Op */}
		}
		connectorServers.clear();
		for(ObjectName on: objectNames) {
			try { JMXHelper.unregisterMBean(on); } catch (Exception x) {/* No Op */} 
		}
		return Deferred.fromResult(null);
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RpcPlugin#version()
	 */
	@Override
	public String version() {
		return "2.1.0";
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.RpcPlugin#collectStats(net.opentsdb.stats.StatsCollector)
	 */
	@Override
	public void collectStats(final StatsCollector collector) {
		// TODO Auto-generated method stub

	}

//	/**
//	 * @return
//	 * @see net.opentsdb.core.TSDB#getClient()
//	 */
//	public final HBaseClient getClient() {
//		tsdb.getClient().stats().atomicIncrements()
//		return tsdb.getClient();
//	}

	public Map<String, String> getConfig() {
		return new TreeMap<String, String>(config.getMap());
	}
	
	@Override
	public int get99thScanLatency() {		
		return tsdb.getScanLatencyHistogram().percentile(99);
	}
	
	@Override
	public int get95thScanLatency() {		
		return tsdb.getScanLatencyHistogram().percentile(95);
	}
	
	@Override
	public int get75thScanLatency() {		
		return tsdb.getScanLatencyHistogram().percentile(75);
	}
	
	@Override
	public int get50thScanLatency() {		
		return tsdb.getScanLatencyHistogram().percentile(50);
	}
	
	@Override
	public int get99thPutLatency() {		
		return tsdb.getPutLatencyHistogram().percentile(99);
	}
	
	@Override
	public int get95thPutLatency() {		
		return tsdb.getPutLatencyHistogram().percentile(95);
	}
	
	@Override
	public int get75thPutLatency() {		
		return tsdb.getPutLatencyHistogram().percentile(75);
	}
	
	@Override
	public int get50thPutLatency() {		
		return tsdb.getPutLatencyHistogram().percentile(50);
	}
	
	@Override
	public List<String> suggestMetrics(final String search, final int max_results) {
		return tsdb.suggestMetrics(search, max_results);
	}
	
	@Override
	public List<String> suggestMetrics(final String search) {
		return tsdb.suggestMetrics(search);
	}
	
	@Override
	public List<String> suggestTagNames(final String search, final int max_results) {
		return tsdb.suggestTagNames(search, max_results);
	}
	
	@Override
	public List<String> suggestTagNames(final String search) {
		return tsdb.suggestTagNames(search);
	}
	
	@Override
	public List<String> suggestTagValues(final String search, final int max_results) {
		return tsdb.suggestTagValues(search, max_results);
	}
	
	@Override
	public List<String> suggestTagValues(final String search) {
		return tsdb.suggestTagValues(search);
	}
	
	@Override
	public void flush()  throws Exception {
		tsdb.flush().joinUninterruptibly(30000);		
	}
	
	@Override
	public void shutdownTSD() throws Exception {
		tsdb.shutdown();	
	}
	
	private UniqueIdType decode(final String uniqueIdType) {
		if(uniqueIdType==null || uniqueIdType.trim().isEmpty()) throw new IllegalArgumentException("The passed UniqueIdType was null or empty");
		try {
			return UniqueIdType.valueOf(uniqueIdType.trim().toUpperCase());
		} catch (Exception ex) {
			throw new IllegalArgumentException("The passed UniqueIdType [" + uniqueIdType + "] was not a valid type. Valid types are " + Arrays.toString(UniqueIdType.values()));
		}
	}

	/**
	 * Attempts to find the name for a unique identifier given a type
	 * @param type The type name of UID
	 * @param uid The UID to search for
	 * @param timeout The timeout in ms.
	 * @return The name of the UID object if found
	 * @throws IllegalArgumentException if the type, uid or timeout is not valid
	 * @throws NoSuchUniqueId if the UID was not found
	 * @see net.opentsdb.core.TSDB#getUidName(net.opentsdb.uid.UniqueId.UniqueIdType, byte[])
	 */
	@Override
	public String getUidName(final String type, final byte[] uid, final long timeout) {		
		if(uid==null || uid.length==0) throw new IllegalArgumentException("The passed uid was null or zero length");
		if(timeout<0) throw new IllegalArgumentException("The passed timeout [" + timeout + "] was invalid");
		final UniqueIdType utype = decode(type);
		try {
			return tsdb.getUidName(utype, uid).joinUninterruptibly(timeout);
		} catch (NoSuchUniqueId nex) {
			throw nex;
		} catch (Exception e) {
			throw new RuntimeException("Failed to getUidName", e);
		}
	}

	/**
	 * Attempts to find the name for a unique identifier given a type using the default timeout
	 * @param type The type name of UID
	 * @param uid The UID to search for
	 * @return The name of the UID object if found
	 * @throws IllegalArgumentException if the type, uid or timeout is not valid
	 * @throws NoSuchUniqueId if the UID was not found
	 * @see net.opentsdb.core.TSDB#getUidName(net.opentsdb.uid.UniqueId.UniqueIdType, byte[])
	 */
	@Override
	public String getUidName(final String type, final byte[] uid) {		
		return getUidName(type, uid, defaultTimeout);
	}
	
	
	/**
	 * Updates a UIDMeta's display name with the default timeout
	 * @param type The type name of the UID
	 * @param uid The UIDMeta's UID
	 * @param displayName The new display name
	 */
	@Override
	public void updateUIDDisplayName(final String type, final String uid, final String displayName) {
		updateUIDDisplayName(type, uid, displayName, defaultTimeout);
	}
	
	/**
	 * Updates a UIDMeta's display name
	 * @param type The type name of the UID
	 * @param uid The UIDMeta's UID
	 * @param displayName The new display name
	 * @param timeout The timeout in ms.
	 */
	@Override
//	public void updateUIDDisplayName(final String type, final String uid, final String displayName, final long timeout) {
//		final UniqueIdType utype = decode(type);
//		if(uid==null || uid.trim().isEmpty()) throw new IllegalArgumentException("The passed uid was null or zero length");
//		if(displayName==null) throw new IllegalArgumentException("The passed displayName was null");
//		if(timeout<0) throw new IllegalArgumentException("The passed timeout [" + timeout + "] was invalid");
//		final Throwable[] terr = new Throwable[1];
//		final long startTime = System.currentTimeMillis();
//		try {			
//			final boolean b = UIDMeta.getUIDMeta(tsdb, utype, uid.trim()).addCallbacks(
//				new Callback<Boolean, UIDMeta>() {
//					@Override
//					public Boolean call(final UIDMeta uidMeta) throws Exception {
////						uidMeta.setDisplayName("");
//						uidMeta.setDisplayName(displayName);
//						try {
//							return uidMeta.syncToStorage(tsdb, false).joinUninterruptibly(timeout);
//						} catch (Exception ex) {
//							ex.printStackTrace(System.err);
//							throw ex;
//						}
//					}
//				},
//				new Callback<Boolean, Throwable>() {
//					@Override
//					public Boolean call(final Throwable t) throws Exception {
//						terr[0] = t;
//						return false;
//					}
//				}
//			).joinUninterruptibly(Math.max(startTime-System.currentTimeMillis()+timeout, 1));
//			if(!b) {
//				if(terr[0]==null) throw new RuntimeException("CAS Lock Failure");
//				throw terr[0];
//			}
//		} catch (NoSuchUniqueId nex) {
//			throw nex;			
//		} catch (Throwable e) {
//			throw new RuntimeException("Failed to update display name for " + utype.name() + ":" + uid + ":" + e);
//		}
//	}
	public void updateUIDDisplayName(final String type, final String uid, final String displayName, final long timeout) {
		final UniqueIdType utype = decode(type);
		if(uid==null || uid.trim().isEmpty()) throw new IllegalArgumentException("The passed uid was null or zero length");
		if(displayName==null) throw new IllegalArgumentException("The passed displayName was null");
		if(timeout<0) throw new IllegalArgumentException("The passed timeout [" + timeout + "] was invalid");
		final long startTime = System.currentTimeMillis();
		long timeLeft = timeout;
		int cnt = 0;
		while(timeLeft > 0 && (startTime+timeLeft) >= System.currentTimeMillis()) {
			if(doUidDisplayName(utype, uid, displayName, timeLeft)) return;
			cnt++;
			timeLeft = System.currentTimeMillis() - startTime;
		}
		throw new RuntimeException("Timed out after [" + cnt + "] attempts trying to update " + utype.name() + ":" + uid);
	}
	
	private boolean doUidDisplayName(final UniqueIdType utype, final String uid, final String displayName, final long timeout) {
		final long outerStart = System.currentTimeMillis();
		try {			
			
			final UIDMeta u = new UIDMeta(utype, uid);
			u.setDisplayName(displayName);
			try {
				final boolean complete = u.syncToStorage(tsdb, false).joinUninterruptibly(timeout);
				final long innerComplete = System.currentTimeMillis();
				LOG.info("Updated UID [{}] in [{}] ms.", u, innerComplete-outerStart);
				if(complete) {
					tsdb.indexUIDMeta(u);
				}
				return true;
			} catch (TimeoutException te) {
				return false;
			}
		} catch (NoSuchUniqueId nex) {
			throw nex;			
		} catch (Throwable e) {
			e.printStackTrace(System.err);
			return false;
//			throw new RuntimeException("Failed! to update display name for " + utype.name() + ":" + uid + ":" +  e);
		}
		
	}
	

	/**
	 * @param type
	 * @param name
	 * @return
	 * @see net.opentsdb.core.TSDB#getUID(net.opentsdb.uid.UniqueId.UniqueIdType, java.lang.String)
	 */
	public byte[] getUID(UniqueIdType type, String name) {
		return tsdb.getUID(type, name);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.opentsdb.jmx.JMXRPCMBean#getUidCacheHits()
	 */
	@Override
	public int getUidCacheHits() {
		return tsdb.uidCacheHits();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.opentsdb.jmx.JMXRPCMBean#getUidCacheMisses()
	 */
	@Override
	public int getUidCacheMisses() {
		return tsdb.uidCacheMisses();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.opentsdb.jmx.JMXRPCMBean#getUidCacheSize()
	 */
	@Override
	public int getUidCacheSize() {
		return tsdb.uidCacheSize();
	}

	/**
	 * @param metric
	 * @param timestamp
	 * @param value
	 * @param tags
	 * @return
	 * @see net.opentsdb.core.TSDB#addPoint(java.lang.String, long, double, java.util.Map)
	 */
	public Deferred<Object> addPoint(String metric, long timestamp,
			double value, Map<String, String> tags) {
		return tsdb.addPoint(metric, timestamp, value, tags);
	}

	/**
	 * 
	 * @see net.opentsdb.core.TSDB#dropCaches()
	 */
	@Override
	public void dropCaches() {
		tsdb.dropCaches();
	}

	/**
	 * @param type
	 * @param name
	 * @return
	 * @see net.opentsdb.core.TSDB#assignUid(java.lang.String, java.lang.String)
	 */
	public byte[] assignUid(String type, String name) {
		return tsdb.assignUid(type, name);
	}

	/**
	 * @param tsuid
	 * @see net.opentsdb.core.TSDB#deleteTSMeta(java.lang.String)
	 */
	public void deleteTSMeta(String tsuid) {
		tsdb.deleteTSMeta(tsuid);
	}

	/**
	 * @param meta
	 * @see net.opentsdb.core.TSDB#deleteUIDMeta(net.opentsdb.meta.UIDMeta)
	 */
	public void deleteUIDMeta(UIDMeta meta) {
		tsdb.deleteUIDMeta(meta);
	}

	/**
	 * @param note
	 * @see net.opentsdb.core.TSDB#deleteAnnotation(net.opentsdb.meta.Annotation)
	 */
	public void deleteAnnotation(Annotation note) {
		tsdb.deleteAnnotation(note);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.opentsdb.jmx.JMXRPCMBean#getDefaultTimeout()
	 */
	@Override
	public long getDefaultTimeout() {
		return defaultTimeout;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.opentsdb.jmx.JMXRPCMBean#setDefaultTimeout(long)
	 */
	@Override
	public void setDefaultTimeout(final long defaultTimeout) {
		if(defaultTimeout<0) throw new IllegalArgumentException("The passed timeout [" + defaultTimeout + "] was invalid");
		this.defaultTimeout = defaultTimeout;
	}

}
