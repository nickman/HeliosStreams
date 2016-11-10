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
package com.heliosapm.streams.onramp;

import java.nio.charset.Charset;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.heliosapm.streams.metrics.StreamedMetricValue;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * <p>Title: StringMetricHandler</p>
 * <p>Description: Decodes the passed (post-CR split) buffer, converts to a string amd submits the metric.</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.onramp.StringMetricHandler</code></p>
 */

public class StringMetricHandler extends SimpleChannelInboundHandler<ByteBuf> {
	/** The instance logger */
	protected static final Logger log = LogManager.getLogger(StringMetricHandler.class);
	/** The UTF8 character set */
	public static final Charset UTF8 = Charset.forName("UTF8");
	/** The message forwarder */
	protected final MessageForwarder mf = MessageForwarder.getInstance();
	
	


	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.SimpleChannelInboundHandler#channelRead0(io.netty.channel.ChannelHandlerContext, java.lang.Object)
	 */
	@Override
	protected void channelRead0(final ChannelHandlerContext ctx, final ByteBuf msg) throws Exception {
		final String v = msg.toString(UTF8).toLowerCase();
		try {			
			final StreamedMetricValue smv;
			if(v.indexOf("put ")==0) {
				// it's a put command, telnet style:  put <metric-name> <timestamp> <value> <tag1=value1>...<tagn=valuen>
				smv = StreamedMetricValue.fromOpenTSDBString(v);
			} else {
				// It's a stringed StreamedMetricValue: [<value-type>,]<timestamp>, [<value>,] <metric-name>, <host>, <app> [,<tagkey1>=<tagvalue1>,<tagkeyn>=<tagvaluen>]
				smv = StreamedMetricValue.fromString(v).forValue();
			}
			log.debug("Ingested UDP Metric: [{}]", smv);
			mf.send(smv);
		} catch (Exception ex) {
			log.error("Failed to handle metric text line: [{}]", v, ex);
		}		
	}
	
}
