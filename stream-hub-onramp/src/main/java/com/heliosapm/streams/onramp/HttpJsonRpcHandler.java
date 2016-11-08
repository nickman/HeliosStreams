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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.heliosapm.streams.json.JSONOps;
import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.StreamedMetricValue;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.FullHttpRequest;

/**
 * <p>Title: HttpJsonRpcHandler</p>
 * <p>Description: Decodes the JSON in the incoming http request and forwards the events upstream</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.onramp.HttpJsonRpcHandler</code></p>
 */

public class HttpJsonRpcHandler extends MessageToMessageDecoder<FullHttpRequest> {
	/** The instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** The topic name to send metrics to */
	protected final String metricTopicName;
	/** The topic name to send meta-data to */
	protected final String metaTopicName;
	
	/** The UTF8 character set */
	public static final Charset UTF8 = Charset.forName("UTF8");
	
	/** The message forwarder */
	protected MessageForwarder mf = MessageForwarder.getInstance();
	
	/** Type reference for common string/string maps */
	public static TypeReference<HashMap<String, String>> TR_HASH_MAP = new TypeReference<HashMap<String, String>>() {};

	
	/**
	 * Creates a new HttpJsonRpcHandler
	 * @param metricTopicName The topic name to send metrics to
	 * @param metaTopicName The topic name to send meta-data to
	 */
	public HttpJsonRpcHandler(final String metricTopicName, final String metaTopicName) {
		this.metricTopicName = metricTopicName;
		this.metaTopicName = metaTopicName;
	}

	/**
	 * {@inheritDoc}
	 * @see io.netty.handler.codec.MessageToMessageDecoder#decode(io.netty.channel.ChannelHandlerContext, java.lang.Object, java.util.List)
	 */
	@Override
	protected void decode(final ChannelHandlerContext ctx, final FullHttpRequest msg, final List<Object> out) throws Exception {
		final ByteBuf buff = msg.content();
		if(buff.readableBytes()<2) {
			log.info("Request from [{}] had no content: {}", ctx.channel().remoteAddress(), buff);
			// send response
			return;
		}
		final JsonNode rootNode = JSONOps.parseToNode(buff);
		if(rootNode.isArray()) {
			for(JsonNode node: rootNode) {
				if(node.has("metric")) {
					forwardMetric((ObjectNode)node);
				}
			}
		}
	}
	
	protected void forwardMetric(final ObjectNode metricNode) {
		final String metric = metricNode.get("metric").textValue();
		final String strValue = metricNode.get("value").textValue();
		final long timestamp = metricNode.get("timestamp").longValue();
		final Map<String, String> tags = JSONOps.parseToObject(metricNode.get("tags"), TR_HASH_MAP);
		StreamedMetricValue smv = new StreamedMetricValue(
			strValue.indexOf('.')==-1 ? Long.parseLong(strValue.trim()) : Double.parseDouble(strValue.trim()),
			metric,
			tags
		).updateTimestamp(timestamp);
		//mf.send(new ProducerRecord<String, StreamedMetric>(metricTopicName, smv));
		log.info("Processing Metric: [{}]", smv);
	}

}
