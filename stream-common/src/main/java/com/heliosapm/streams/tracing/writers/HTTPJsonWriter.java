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
package com.heliosapm.streams.tracing.writers;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import com.fasterxml.jackson.core.JsonGenerator;
import com.heliosapm.streams.json.JSONOps;
import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.tracing.writers.TelnetWriter.ResponseHandler;
import com.heliosapm.utils.config.ConfigurationHelper;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.stream.ChunkedWriteHandler;

/**
 * <p>Title: HTTPJsonWriter</p>
 * <p>Description: Tracing writer that sends metrics as HTTP/JSON to an OpenTSDB listener</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tracing.writers.HTTPJsonWriter</code></p>
 */

public class HTTPJsonWriter extends NetWriter<NioSocketChannel>  {
	/** The UTF8 character set */
	public static final Charset UTF8 = Charset.forName("UTF8");
	/** A string encoder */
	public static final StringEncoder STR_ENCODER = new StringEncoder(UTF8);
	/** The response handler */
	public static final ResponseHandler RESPONSE_HANDLER = new ResponseHandler();
	
	/** The config key for the enablement of gzip on submitted metrics */
	public static final String CONFIG_COMPRESSION = "metricwriter.httpjson.compression";
	/** The default enablement of gzip on submitted metrics */
	public static final boolean DEFAULT_COMPRESSION = false;
	/** The config key for the enablement of http chunking on submitted metrics */
	public static final String CONFIG_CHUNKING = "metricwriter.httpjson.chunking";
	/** The default enablement of chunking on submitted metrics */
	public static final boolean DEFAULT_CHUNKING = false;
	
	/** A streamed metric to string encoder */
	protected final StreamedMetricEncoder METRIC_ENCODER = new StreamedMetricEncoder();
	/** A streamed metric to string encoder */
	protected final MetricBufferHttpEncoder METRIC_HTTP_ENCODER = new MetricBufferHttpEncoder();
	
	/** Enable http chunking */
	protected boolean chunkingEnabled = DEFAULT_CHUNKING;
	/** Enable http compression */
	protected boolean compressionEnabled = DEFAULT_COMPRESSION;

	/** The telnet channel initializer */
	protected final ChannelInitializer<NioSocketChannel> CHANNEL_INIT = new ChannelInitializer<NioSocketChannel>() {
		@Override
		protected void initChannel(final NioSocketChannel ch) throws Exception {
			final ChannelPipeline p = ch.pipeline();
			if(compressionEnabled) p.addLast("deflater", new HttpContentCompressor());
			p.addLast("decoder", new HttpRequestDecoder());			
			p.addLast("encoder", new HttpResponseEncoder());
			if(chunkingEnabled) p.addLast("chunker", new ChunkedWriteHandler());	
			p.addLast("httpRequestConverter", METRIC_HTTP_ENCODER);
			p.addLast("metricEncoder", METRIC_ENCODER);
			p.addLast("responseHandler", RESPONSE_HANDLER);
		}
	};
	
	
	
//	/** A streamed metric to string encoder */
//	protected final StreamedMetricEncoder METRIC_ENCODER = new StreamedMetricEncoder();

	/**
	 * Creates a new HTTPJsonWriter
	 */
	public HTTPJsonWriter() {
		super(NioSocketChannel.class, false);		
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.writers.NetWriter#configure(java.util.Properties)
	 */
	@Override
	public void configure(final Properties config) {		
		super.configure(config);
		chunkingEnabled = ConfigurationHelper.getBooleanSystemThenEnvProperty(CONFIG_CHUNKING, DEFAULT_CHUNKING, config);
		compressionEnabled = ConfigurationHelper.getBooleanSystemThenEnvProperty(CONFIG_COMPRESSION, DEFAULT_COMPRESSION, config);
		this.config.put("chunkingEnabled", chunkingEnabled);
		this.config.put("compressionEnabled", compressionEnabled);
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.writers.NetWriter#getChannelInitializer()
	 */
	@Override
	protected ChannelInitializer<NioSocketChannel> getChannelInitializer() {
		return CHANNEL_INIT;
	}
	
	/**
	 * <p>Title: MetricBufferHttpEncoder</p>
	 * <p>Description: Converts a ByteBuf of JSON to an HTTP POST request</p> 
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>com.heliosapm.streams.tracing.writers.HTTPJsonWriter.MetricBufferHttpEncoder</code></p>
	 */
	@Sharable
	public class MetricBufferHttpEncoder extends MessageToMessageEncoder<ByteBuf> {

		/**
		 * {@inheritDoc}
		 * @see io.netty.handler.codec.MessageToMessageEncoder#encode(io.netty.channel.ChannelHandlerContext, java.lang.Object, java.util.List)
		 */
		@Override
		protected void encode(final ChannelHandlerContext ctx, final ByteBuf msg, final List<Object> out) throws Exception {
			out.add(
				new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, "/api/put", msg, true)
			);
		}

		
	}
	
	/**
	 * <p>Title: StreamedMetricEncoder</p>
	 * <p>Description: Encoder to encode streamed metrics into JSON</p> 
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>com.heliosapm.streams.tracing.writers.HTTPJsonWriter.StreamedMetricEncoder</code></p>
	 */
	@Sharable
	public class StreamedMetricEncoder extends MessageToByteEncoder<Object> {
		
		/**
		 * {@inheritDoc}
		 * @see io.netty.handler.codec.MessageToByteEncoder#encode(io.netty.channel.ChannelHandlerContext, java.lang.Object, io.netty.buffer.ByteBuf)
		 */
		@Override
		protected void encode(final ChannelHandlerContext ctx, final Object msg, final ByteBuf out) throws Exception {
			if(msg==null) return;
			int sent = 0;
			final OutputStream os = new ByteBufOutputStream(out);
			final JsonGenerator j = JSONOps.generatorFor(os);
			j.writeStartArray();
			if(msg instanceof ByteBuf) {
				final ByteBuf buff = (ByteBuf)msg;
				final InputStream is = new ByteBufInputStream(buff);
				for(StreamedMetric sm: StreamedMetric.streamedMetrics(is, true, false)) {
					j.writeObject(sm);
					sent++;
				}
			} else if(msg instanceof StreamedMetric) {
				j.writeObject(msg);
				sent++;
			} else if(msg instanceof StreamedMetric[]) {
				final StreamedMetric[] values = (StreamedMetric[])msg;
				for(StreamedMetric sm: values) {
					j.writeObject(sm);
					sent++;
				}				
			}  else if(msg instanceof Collection) {
				final Collection<Object> objects = (Collection<Object>)msg;
				if(!objects.isEmpty()) {
					for(Object o: objects) {
						if(o != null && (o instanceof StreamedMetric)) {
							j.writeObject(o);
							sent++;
						}
					}
				}
			} else {
				log.warn("Unknown type submitted: [{}]", msg.getClass().getName());
			}
			sentMetrics.add(sent);
			j.writeEndArray();
			j.flush();
			os.flush();
			os.close();
			ctx.channel().flush();
		}
	}
	

}
