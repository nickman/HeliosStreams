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
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.lang.StringHelper;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.compression.ZlibCodecFactory;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

/**
 * <p>Title: TelnetWriter</p>
 * <p>Description: Tracing writer that sends plain text <b><code>put</code></b> commands to an OpenTSDB listener</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tracing.writers.TelnetWriter</code></p>
 */

public class TelnetWriter extends NetWriter<NioSocketChannel> {
	
	/** The JVM's end of line separator */
	public static final String EOL = System.getProperty("line.separator");
	/** The UTF8 character set */
	public static final Charset UTF8 = Charset.forName("UTF8");
	/** A string encoder */
	public static final StringEncoder STR_ENCODER = new StringEncoder(UTF8);
	/** The response handler */
	public static final ResponseHandler RESPONSE_HANDLER = new ResponseHandler();
	
	/** The config key for the enablement of gzip on submitted metrics */
	public static final String CONFIG_COMPRESSION = "metricwriter.telnet.compression";
	/** The default enablement of gzip on submitted metrics */
	public static final boolean DEFAULT_COMPRESSION = false;

	/** Enable text compression */
	protected boolean compressionEnabled = DEFAULT_COMPRESSION;

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.tracing.writers.NetWriter#configure(java.util.Properties)
	 */
	@Override
	public void configure(final Properties config) {		
		super.configure(config);
		compressionEnabled = ConfigurationHelper.getBooleanSystemThenEnvProperty(CONFIG_COMPRESSION, DEFAULT_COMPRESSION, config);
		this.config.put("compressionEnabled", compressionEnabled);
	}
	
	
	/** A streamed metric to string encoder */
	protected final StreamedMetricEncoder METRIC_ENCODER = new StreamedMetricEncoder();
	/** The telnet channel initializer */
	protected final ChannelInitializer<NioSocketChannel> CHANNEL_INIT = new ChannelInitializer<NioSocketChannel>() {
		@Override
		protected void initChannel(final NioSocketChannel ch) throws Exception {
			final ChannelPipeline p = ch.pipeline();
			if(compressionEnabled) p.addLast("gzipdeflater", ZlibCodecFactory.newZlibEncoder(ZlibWrapper.GZIP));
			p.addLast("stringEncoder", STR_ENCODER);
			p.addLast("metricEncoder", METRIC_ENCODER);
			p.addLast("stringDecoder", new StringDecoder(UTF8));
			p.addLast("responseHandler", RESPONSE_HANDLER);
		}
	};
	
	/**
	 * <p>Title: ResponseHandler</p>
	 * <p>Description: Handles responses from the telnet endpoint</p> 
	 * <p>Company: Helios Development Group LLC</p>
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>com.heliosapm.streams.tracing.writers.TelnetWriter.ResponseHandler</code></p>
	 */
	@Sharable
	protected static class ResponseHandler extends SimpleChannelInboundHandler<String> {
		/** Instance logger */
		protected final Logger log = LogManager.getLogger(getClass());

		/**
		 * {@inheritDoc}
		 * @see io.netty.channel.SimpleChannelInboundHandler#channelRead0(io.netty.channel.ChannelHandlerContext, java.lang.Object)
		 */
		@Override
		protected void channelRead0(final ChannelHandlerContext ctx, final String msg) throws Exception {
			if(msg!=null && !msg.trim().isEmpty()) {
				final String[] messages = StringHelper.splitString(msg, EOL.charAt(0), true);
				if(messages.length!=0) {
					final StringBuilder b = new StringBuilder("\n\t================================\n\tTelnet Responses:");
					for(String s: messages) {
						b.append("\n\t[").append(s.trim()).append("]");
					}
					b.append("\n\t================================\n");
					log.info(b.toString());
				}
				
			}
		}
		
	}
	
	/**
	 * Creates a new TelnetWriter
	 */
	public TelnetWriter() {
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
	 * <p>Description: Encoder to encode streamed metrics into strings</p> 
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>com.heliosapm.streams.tracing.writers.TelnetWriter.StreamedMetricEncoder</code></p>
	 */
	@Sharable
	public class StreamedMetricEncoder extends MessageToMessageEncoder<Object> {
		
		/**
		 * {@inheritDoc}
		 * @see io.netty.handler.codec.MessageToMessageEncoder#encode(io.netty.channel.ChannelHandlerContext, java.lang.Object, java.util.List)
		 */
		@Override
		protected void encode(final ChannelHandlerContext ctx, final Object msg, final List<Object> out) throws Exception {
			if(msg==null) return;
			int sent = 0;
			if(msg instanceof ByteBuf) {
				final ByteBuf buff = (ByteBuf)msg;
				final StringBuilder b = new StringBuilder();
				final InputStream is = new ByteBufInputStream(buff);
				for(StreamedMetric sm: StreamedMetric.streamedMetrics(is, true, false)) {
					b.append(sm.toOpenTSDBString()).append(EOL);
					sent++;
				}
				out.add(b);
			} else if(msg instanceof StreamedMetric) {
				out.add(((StreamedMetric)msg).toOpenTSDBString());
				sent++;
			} else if(msg instanceof StreamedMetric[]) {
				final StreamedMetric[] values = (StreamedMetric[])msg;
				final StringBuilder b = new StringBuilder(values.length * 128);
				for(StreamedMetric sm: values) {
					b.append(sm.toOpenTSDBString()).append(EOL);
					sent++;
				}
				out.add(b);				
			}  else if(msg instanceof Collection) {
				final Collection<Object> objects = (Collection<Object>)msg;
				if(!objects.isEmpty()) {
					final StringBuilder b = new StringBuilder(objects.size() * 128);
					for(Object o: objects) {
						if(o != null && (o instanceof StreamedMetric)) {
							b.append(((StreamedMetric)o).toOpenTSDBString()).append(EOL);
							sent++;
						}
					}
					if(b.length()>0) {
						out.add(b);
					}
				}
			} else {
				log.warn("Unknown type submitted: [{}]", msg.getClass().getName());
			}
			sentMetrics.add(sent);
			ctx.channel().flush();
			if(log.isDebugEnabled() && !out.isEmpty()) {
				for(Object s: out) {
					log.debug("Out Metric:\n[{}]", s);
				}
			}
		}
	}

}
