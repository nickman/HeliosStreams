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

import java.nio.charset.Charset;

import com.heliosapm.streams.tracing.writers.TelnetWriter.ResponseHandler;
import com.heliosapm.streams.tracing.writers.TelnetWriter.StreamedMetricEncoder;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringEncoder;

/**
 * <p>Title: HTTPJsonWriter</p>
 * <p>Description: Tracing writer that sends metrics as HTTP/JSON to an OpenTSDB listener</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.tracing.writers.HTTPJsonWriter</code></p>
 */

public class HTTPJsonWriter extends NetWriter<NioSocketChannel> {
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
	
	
//	/** A streamed metric to string encoder */
//	protected final StreamedMetricEncoder METRIC_ENCODER = new StreamedMetricEncoder();

	public HTTPJsonWriter(Class<NioSocketChannel> channelType, boolean confirmsMetrics) {
		super(NioSocketChannel.class, false);		
	}

	@Override
	protected ChannelInitializer<NioSocketChannel> getChannelInitializer() {
		// TODO Auto-generated method stub
		return null;
	}

}
