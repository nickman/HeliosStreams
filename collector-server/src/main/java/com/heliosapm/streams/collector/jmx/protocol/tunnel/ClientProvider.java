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
package com.heliosapm.streams.collector.jmx.protocol.tunnel;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.management.Notification;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.remote.JMXConnectionNotification;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXConnectorProvider;
import javax.management.remote.JMXServiceURL;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.heliosapm.streams.collector.cache.CacheEventListener;
import com.heliosapm.streams.collector.cache.GlobalCacheService;
import com.heliosapm.streams.collector.ssh.SSHTunnelManager;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.jmx.protocol.UpdateableJMXConnector;

/**
 * <p>Title: ClientProvider</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.jmx.protocol.tunnel.ClientProvider</code></p>
 */

public class ClientProvider implements JMXConnectorProvider, NotificationListener, NotificationFilter {
	/** The protocol name */
	public static final String PROTOCOL_NAME = "tunnel";
	/** The env map key for the delegating JMX URL format */
	public static final String DELEGATE_PROTOCOL_FORMAT_KEY = "jmx.url.format";
	/** The env map key for the reconnect timeout */
	public static final String RECONNECT_TIMEOUT_KEY = "jmx.reconnect.timeout";
	
	/** The default delegating protocol format */
	public static final String DEFAULT_DELEGATE_PROTOCOL_FORMAT = "service:jmx:jmxmp://%s:%s"; 
	/** The default reconnect timeout in seconds */
	public static final long DEFAULT_RECONNECT_TIMEOUT = 300L; 
	
	/** Static class logger */
	private static final Logger LOG = LogManager.getLogger(ClientProvider.class.getName() + "-Tunnel"); 
	
	
    /**
     * {@inheritDoc}
     * @see javax.management.remote.JMXConnectorProvider#newJMXConnector(javax.management.remote.JMXServiceURL, java.util.Map)
     */
    @Override
	public JMXConnector newJMXConnector(final JMXServiceURL serviceURL, final Map<String, ?> env) throws IOException {
		if (!serviceURL.getProtocol().equals(PROTOCOL_NAME)) {
			throw new MalformedURLException("Protocol not [" + PROTOCOL_NAME + "]: " +
						    serviceURL.getProtocol());
		}
		final Map<String, ?> environment = env==null ? new HashMap<String, Object>() : env;
		final String remoteHost = serviceURL.getHost();
		final int remotePort = serviceURL.getPort();
		
		final int localPort = SSHTunnelManager.getInstance().getPortForward(remoteHost, remotePort);
		final String format = environment.containsKey(DELEGATE_PROTOCOL_FORMAT_KEY) ? environment.get(DELEGATE_PROTOCOL_FORMAT_KEY).toString() : DEFAULT_DELEGATE_PROTOCOL_FORMAT;
		
		final JMXServiceURL tunneledURL = JMXHelper.serviceUrl(format, "localhost", localPort);
		final JMXConnector connector = JMXConnectorFactory.newJMXConnector(tunneledURL, environment);
		
		final UpdateableJMXConnector ujmx = new UpdateableJMXConnector(connector, serviceURL, environment);
		connector.addConnectionNotificationListener(this, this, ujmx);
		return ujmx;
    }
    
	/**
	 * {@inheritDoc}
	 * @see javax.management.NotificationListener#handleNotification(javax.management.Notification, java.lang.Object)
	 */
	@Override
	public void handleNotification(final Notification notification, final Object handback) {
//		try { connector.get().close(); } catch (Exception x) {/* No Op */}
		if(handback!=null && (handback instanceof UpdateableJMXConnector)) {
			final UpdateableJMXConnector ujmx = (UpdateableJMXConnector)handback;
			final Map<String, ?> ujmxEnv = ujmx.getEnv(); 
			final JMXServiceURL serviceURL = ujmx.getAddress();
			final String remoteHost = serviceURL.getHost();
			final int remotePort = serviceURL.getPort();
			final String key = remoteHost + "-" + remotePort;
			final NotificationListener thisListener = this;
			final NotificationFilter thisFilter = this;
			final CacheEventListener cacheListener = new CacheEventListener() {

				@Override
				public void onValueAdded(final String key, final Object value) {
					if(value instanceof Integer) {
						final int localPort = (Integer)value;
						final String format = ujmx.getEnv().containsKey(DELEGATE_PROTOCOL_FORMAT_KEY) ? ujmxEnv.get(DELEGATE_PROTOCOL_FORMAT_KEY).toString() : DEFAULT_DELEGATE_PROTOCOL_FORMAT;
						
						final JMXServiceURL tunneledURL = JMXHelper.serviceUrl(format, "localhost", localPort);
						try {
							final JMXConnector connector = JMXConnectorFactory.newJMXConnector(tunneledURL, ujmxEnv);
							connector.addConnectionNotificationListener(thisListener, thisFilter, ujmx);
							connector.connect();
							ujmx.updateConnector(connector);
						} catch (Exception ex) {
							LOG.error("Failed to reconnect to [{}] via tunnel [{}]", serviceURL, localPort);
						}
					}
					
				}

				@Override
				public void onValueRemoved(final String key, final Object removedValue) {
					/* No Op */
				}

				@Override
				public void onValueReplaced(final String key, final Object oldValue, final Object newValue) {
					/* No Op */					
				}
				
			};
			
			long timeout = -1;
			try {
				timeout = (Long)ujmxEnv.get(RECONNECT_TIMEOUT_KEY);
			} catch (Exception ex) {
				timeout = DEFAULT_RECONNECT_TIMEOUT;
			}
			final Integer localPort = GlobalCacheService.getInstance().getOrNotify(key, Integer.class, cacheListener, timeout, TimeUnit.SECONDS);
			if(localPort!=null) {
				final String format = ujmx.getEnv().containsKey(DELEGATE_PROTOCOL_FORMAT_KEY) ? ujmxEnv.get(DELEGATE_PROTOCOL_FORMAT_KEY).toString() : DEFAULT_DELEGATE_PROTOCOL_FORMAT;
				
				final JMXServiceURL tunneledURL = JMXHelper.serviceUrl(format, "localhost", localPort);
				try {
					final JMXConnector connector = JMXConnectorFactory.newJMXConnector(tunneledURL, ujmxEnv);
					connector.addConnectionNotificationListener(this, this, ujmx);
					connector.connect();
					ujmx.updateConnector(connector);
				} catch (Exception ex) {
					LOG.error("Failed to reconnect to [{}] via tunnel [{}]", serviceURL, localPort);
				}
			}
		}
		
	}


	/**
	 * {@inheritDoc}
	 * @see javax.management.NotificationFilter#isNotificationEnabled(javax.management.Notification)
	 */
	@Override
	public boolean isNotificationEnabled(final Notification n) {
		if(n==null) return false;
		if(!JMXConnectionNotification.class.isInstance(n)) return false;
		final String type = n.getType();
		return (JMXConnectionNotification.FAILED.equals(type) || JMXConnectionNotification.CLOSED.equals(type));
	}



}
