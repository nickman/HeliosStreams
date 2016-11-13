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
package com.heliosapm.streams.forwarder;

import java.nio.charset.Charset;
import java.util.List;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashSet;

import com.heliosapm.streams.json.JSONOps;
import com.heliosapm.streams.metrics.StreamedMetricValue;
import com.heliosapm.utils.buffer.BufferManager;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;

/**
 * <p>Title: HttpJsonOutboundHandler</p>
 * <p>Description: Converts received kafka consumer records to http posts</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.forwarder.HttpJsonOutboundHandler</code></p>
 */
@Sharable
public class HttpJsonOutboundHandler extends MessageToMessageEncoder<ConsumerRecords<String, StreamedMetricValue>> {
	
	/** The maximum number of records to post in each forwarding call */
	final int maxRecordsPerPost;
	/** The endpoint host to put in the HOST header of http post */
	final String host;
	/** The endpoint uri of the http post */
	final String postUri;
	
	/** Instance logger */
	protected final Logger log = LogManager.getLogger(getClass());

	
	/** The byte buff manager */
	final BufferManager buffManager = BufferManager.getInstance();
	
	/** The UTF8 character set */
	public static final Charset UTF8 = Charset.forName("UTF8");
	/** The bytes for a JSON array opener */
	public static final byte[] ARRAY_OPEN = "[".getBytes(UTF8);
	
	

	/**
	 * Creates a new HttpJsonOutboundHandler
	 * @param maxRecordsPerPost The maximum number of records to post in each forwarding call
	 * @param host The endpoint host to put in the HOST header of http post
	 * @param postUri The endpoint uri of the http post
	 */
	public HttpJsonOutboundHandler(final int maxRecordsPerPost, final String host, final String postUri) {
		this.maxRecordsPerPost = maxRecordsPerPost;
		this.host = host;
		this.postUri = postUri;
	}





	/**
	 * {@inheritDoc}
	 * @see io.netty.handler.codec.MessageToMessageEncoder#encode(io.netty.channel.ChannelHandlerContext, java.lang.Object, java.util.List)
	 */
	@Override
	protected void encode(final ChannelHandlerContext ctx, final ConsumerRecords<String, StreamedMetricValue> msg, final List<Object> out) throws Exception {
		final StreamedMetricValue[] smvs = StreamSupport.stream(msg.spliterator(), true)
			.map(new Function<ConsumerRecord<String, StreamedMetricValue>, StreamedMetricValue>() {
				@Override
				public StreamedMetricValue apply(ConsumerRecord<String, StreamedMetricValue> t) {
					return t.value();
				}
				
		}).toArray(s -> new StreamedMetricValue[s]);
		final int size = smvs.length;
		final ByteBuf buff = buffManager.buffer(size * 200);
		
		JSONOps.serializeAndGzip(smvs, buff);
		final int sz = buff.readableBytes();
		final HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, postUri, buff);
		request.headers().set(HttpHeaderNames.HOST, host);
		request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
		request.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
		request.headers().set(HttpHeaderNames.CONTENT_ENCODING, HttpHeaderValues.GZIP);
		request.headers().set(HttpHeaderNames.CONTENT_LENGTH,buff.readableBytes());
		out.add(request);
		
		ctx.executor().execute(new Runnable(){
			public void run() {
				final NonBlockingHashSet<String> hosts = new NonBlockingHashSet<String>();
				StreamSupport.stream(msg.spliterator(), true)
				.map(new Function<ConsumerRecord<String, StreamedMetricValue>, String>() {
					@Override
					public String apply(ConsumerRecord<String, StreamedMetricValue> t) {
						return t.value().getTags().get("host");
					}				
				}).forEach(h -> hosts.add(h));
				log.info("Hosts:{}, Size: {}", hosts, sz);
			}
		});
	}
	
	
}
