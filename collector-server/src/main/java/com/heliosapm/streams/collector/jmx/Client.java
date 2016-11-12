package com.heliosapm.streams.collector.jmx;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;
import java.util.stream.IntStream.Builder;

import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.remote.JMXConnector;

import com.heliosapm.utils.io.StdInCommandHandler;
import com.heliosapm.utils.jmx.JMXHelper;
import com.heliosapm.utils.time.SystemClock;

public class Client {

	public static void main(String[] args) {
		Client c = new Client();
		c.go();
	}
	
	public static void log(Object msg) {
		System.out.println(msg);
		System.out.flush();
	}
	
	public void go() {
		log("JMXMP Client Test");
		JMXConnector connector = null;
		final NotificationListener listener = new NotificationListener() {
			@Override
			public void handleNotification(Notification n, Object handback) {
				final StringBuilder b = new StringBuilder("NOTIF");
				b.append("\n\tType:").append(n.getType());
				b.append("\n\tUserData:").append(n.getUserData());
				b.append("\n\tSeq:").append(n.getSequenceNumber());
				b.append("\n\tSource:").append(n.getSource());
				log(b);
			}
		};
		try {
			//connector = JMXHelper.connector(JMXHelper.serviceUrl("service:jmx:jmxmp://localhost:4245"), null);
			final JMXClient client = JMXClient.newInstance(this, "service:jmx:jmxmp://192.168.1.182:8006");
//			connector = JMXHelper.connector(JMXHelper.serviceUrl("service:jmx:jmxmp://192.168.1.182:8006"), null);
//			connector.addConnectionNotificationListener(listener, null, null);
//			connector.connect();
//			final JMXConnector fconnector = connector;
//			final MBeanServerConnection conn = connector.getMBeanServerConnection();
			final Client c = this;
			StdInCommandHandler.getInstance().registerCommand("close", new Runnable(){
				public void run() {
					try { client.close(); } catch (Exception x) {}
					System.exit(0);
				}
			}).runAsync(true);
			log("Starting...");
			for(int i = 0; i < 1000; i++) {
				Builder b = IntStream.builder();
				for(int x = 0; x < 20; x++) {
					b.add(x);
				}
				final int fi = i;
				b.build().parallel().forEach(q -> {
					try { 
						for(int a = 0; a < q; a++) {
							log("Starting " + fi + ":" + q);
							final JMXClient conn = JMXClient.newInstance(c, "service:jmx:jmxmp://192.168.1.182:8006");
							final Map<String, Object> allValues = new ConcurrentHashMap<String, Object>(1024);
							conn.queryNames(null, null)
								.parallelStream()
								.forEach(on -> {
									try {
										final MBeanInfo minfo = conn.getMBeanInfo(on);
										Arrays.stream(minfo.getAttributes()).parallel().forEach(ai -> {
											try {
												Object o = conn.getAttribute(on, ai.getName());
												allValues.put(on.toString() + "/" + ai.getName(), (o==null ? "" : o));
											} catch (javax.management.AttributeNotFoundException | UnsupportedOperationException | javax.management.RuntimeMBeanException uex) {
											} catch (Exception ex) {
												//ex.printStackTrace(System.err);
											}											
										});
									} catch (Exception ex) {
										ex.printStackTrace(System.err);
									}
								});
							log("[" + Thread.currentThread().getName() + "] Item Count:" + allValues.size());
						}
					} catch (Exception x) {
						x.printStackTrace(System.err);
					}
				});
				log("Completed Loop:" + i);
				SystemClock.sleep(1500);
			}
 			
			
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
		} finally {
			if(connector!=null) try { connector.close(); } catch (Exception x) {}
		}

		
	}

}
