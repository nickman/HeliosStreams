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
package com.heliospam.streams.tsdb.query;

import java.io.Closeable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.Response;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.heliosapm.utils.collections.FluentMap;


/**
 * <p>Title: QueryTool</p>
 * <p>Description: Basic TSDB Query Tool</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliospam.streams.tsdb.query.QueryTool</code></p>
 */

public class QueryTool implements Closeable {
	private static final Logger log = LogManager.getLogger(QueryTool.class);
	
	public static final String TS_FORMAT = "yyyy-MM-dd HH:mm:ss";
	
	private static final ObjectMapper OM = new ObjectMapper();
	
	protected AsyncHttpClient cli = null;
	protected final String tsdbUrl;
	protected static final ThreadLocal<SimpleDateFormat> SDF = new ThreadLocal<SimpleDateFormat>() {
		@Override
		protected SimpleDateFormat initialValue() {			
			return new SimpleDateFormat(TS_FORMAT);
		}
	};
	/**
	 * Creates a new QueryTool
	 */
	public QueryTool(final String tsdbUrl) {
		cli = new DefaultAsyncHttpClient();
		this.tsdbUrl = tsdbUrl;
	}
	
	public static long toSecs(final String time) {
		try {
			return TimeUnit.MILLISECONDS.toSeconds(SDF.get().parse(time).getTime());
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
	
	public static String fromSecs(final long time) {
		return SDF.get().format(new Date(TimeUnit.SECONDS.toMillis(time)));
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.io.Closeable#close()
	 */
	public void close() {
		 if(cli!=null) try { cli.close(); log.info("Client Closed"); } catch (Exception x) {/* No Op */}
	}
	
	public String getTSUID(final String metricName, final Map<String, String> tags) {
		final StringBuilder b = new StringBuilder(tsdbUrl)
			.append("/api/uid/tsmeta?m=")
			.append(metricName)
			.append(tags)
			.append("&create=false");
		log.info("Query: [{}]", b.toString());
		//cli.prepareGet("http://pdk-pt-cltsdb-01:4242/api/uid/tsmeta?m=ptms.ti&app=ptms").execute();
//		for(Map.Entry<String, String> tag: tags.entrySet()) {
//			b.append("&").append(tag.getKey()).append("=").append(tag.getValue());
//		}
		final Future<Response> f =  cli.prepareGet(b.toString()).execute();
		try {
			final Response resp = f.get();
			return resp.getResponseBody();
		} catch (Exception ex) {
			log.error("getTSUID failed with URL [{}]", b.toString(), ex);
			throw new RuntimeException(ex);
		}		
	}
	
	public String rawData(final String startTime, final String endTime, final String metricName, final Map<String, String> tags) {
		final StringBuilder b = new StringBuilder(tsdbUrl)
				.append("/api/query?")
				.append("show_tsuids=true")
//				.append("&show_summary=true")
				.append("&start=").append(toSecs(startTime))
				.append("&end=").append(toSecs(endTime))
				.append("&m=avg:1m-avg:").append(metricName)
//				.append("&m=none:1m-avg:").append(metricName)
				
				.append(tags);
			log.info("Query: [{}]", b.toString());
			final Future<Response> f =  cli.prepareGet(b.toString()).execute();
			final Map<String, Double> data = new LinkedHashMap<String, Double>(128);
			Response resp = null; 
			try {
				resp = f.get();
				final JsonNode node = OM.readTree(resp.getResponseBody());
				final ObjectNode dpsNode = (ObjectNode)node.get(0).get("dps");
				for(Iterator<String> iter = dpsNode.fieldNames(); iter.hasNext();) {
					final String key = iter.next();
					final long dpsTime = Long.parseLong(key);
					final Number d = dpsNode.get(key).numberValue();
					iter.remove();
					data.put(fromSecs(dpsTime), d.doubleValue());					
				}
				double total = 0D;
				for(Map.Entry<String, Double> entry: data.entrySet()) {
					dpsNode.put(entry.getKey(), entry.getValue());
					total += entry.getValue();
				}
				return OM.writeValueAsString(node) + "\n\tTOTAL:" + total;
			} catch (Exception ex) {
				log.error("rawData failed with URL [{}]", b.toString(), ex);
				System.err.println(resp.getResponseBody());
				throw new RuntimeException(ex);
			}		
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		log.info("QueryTool");
		QueryTool qt = null;
		try {
			//qt = new QueryTool("http://pdk-pt-cltsdb-01:4242");
			qt = new QueryTool("http://pdk-pt-cltsdb-05:4242");
			String s = qt.rawData("2016-09-17 11:00:00", "2016-09-17 12:00:00", "ptms.ibs", FluentMap.newMap(String.class, String.class)
//					.fput("app", "tradeimporter")
					.fput("app", "ptms")
//					.fput("host", "*")
//					.fput("env", "pt")
			);
			log.info("Response: [{}]", s);
			qt.close();
			qt = new QueryTool("http://pdk-pt-cltsdb-01:4242");
			s = qt.rawData("2016-09-17 11:00:00", "2016-09-17 12:00:00", "ptms.ibs", FluentMap.newMap(String.class, String.class)
					.fput("app", "ibs")
//					.fput("app", "ptms")
//					.fput("host", "*")
//					.fput("env", "pt")
			);
			log.info("Response: [{}]", s);
			
		} finally {
			try { qt.close(); } catch (Exception x) {/* No Op */}
		}

	}

}
