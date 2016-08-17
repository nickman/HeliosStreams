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

import com.fasterxml.jackson.core.JsonGenerator;
import com.heliosapm.streams.json.JSONOps;
import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.tracing.writers.TelnetWriter.ResponseHandler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

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
	/** A streamed metric to string encoder */
	protected final StreamedMetricEncoder METRIC_ENCODER = new StreamedMetricEncoder();

	/** The telnet channel initializer */
	protected final ChannelInitializer<NioSocketChannel> CHANNEL_INIT = new ChannelInitializer<NioSocketChannel>() {
		@Override
		protected void initChannel(final NioSocketChannel ch) throws Exception {
			final ChannelPipeline p = ch.pipeline();
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
	 * @see com.heliosapm.streams.tracing.writers.NetWriter#getChannelInitializer()
	 */
	@Override
	protected ChannelInitializer<NioSocketChannel> getChannelInitializer() {
		return CHANNEL_INIT;
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
			ctx.channel().flush();
		}
	}
	

}
