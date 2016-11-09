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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.heliosapm.streams.json.JSONOps;
import com.heliosapm.streams.metrics.StreamedMetric;
import com.heliosapm.streams.metrics.StreamedMetricValue;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

/**
 * <p>Title: HttpJsonRpcHandler</p>
 * <p>Description: Decodes the JSON in the incoming http request and forwards the events upstream</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.onramp.HttpJsonRpcHandler</code></p>
 */
@Sharable
public class HttpJsonRpcHandler extends MessageToMessageDecoder<FullHttpRequest> {
	/** The instance logger */
	protected final Logger log = LogManager.getLogger(getClass());
	/** The topic name to send metrics to */
	protected final String metricTopicName;
	/** The topic name to send meta-data to */
	protected final String metaTopicName;
	/** The max size of batches to forward to the endpoint */
	protected final int batchSize;
	
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
	 * @param batchSize The max size of batches to forward to the endpoint
	 */
	public HttpJsonRpcHandler(final String metricTopicName, final String metaTopicName, final int batchSize) {
		this.metricTopicName = metricTopicName;
		this.metaTopicName = metaTopicName;
		this.batchSize = batchSize;
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
		int nodes = 0;
		final ArrayList<ObjectNode> metricNodes = new ArrayList<ObjectNode>(256); 
		final JsonNode rootNode = JSONOps.parseToNode(buff);
		if(rootNode.isArray()) {
			for(JsonNode node: rootNode) {
				if(node.has("metric")) {
					metricNodes.add((ObjectNode)node);
					nodes++;
					if(nodes==batchSize) {
						try {			
							nodes = 0;
							forwardMetrics(metricNodes);							
						} finally {
							metricNodes.clear();
						}
					}
				}
			}
			if(!metricNodes.isEmpty()) try {
				nodes += metricNodes.size();
				forwardMetrics(metricNodes);							
			} finally {
				metricNodes.clear();
			}			
		} else {
			if(rootNode.has("metric")) {				
				forwardMetrics(Collections.singletonList((ObjectNode)rootNode));
			}
		}
		
		ctx.channel().pipeline().writeAndFlush(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT));
	}
	
	/**
	 * {@inheritDoc}
	 * @see io.netty.channel.ChannelInboundHandlerAdapter#exceptionCaught(io.netty.channel.ChannelHandlerContext, java.lang.Throwable)
	 */
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {		
		log.error("Pipeline Error", cause);
	}
	
	protected void forwardMetrics(final List<ObjectNode> metricNodes) {
		final HashSet<StreamedMetric> smetrics = new HashSet<StreamedMetric>(metricNodes.size()); 
		
		for(ObjectNode metricNode: metricNodes) {
			try {
				final String metric = metricNode.get("metric").textValue();
				final String strValue = metricNode.get("value").toString();
				final long timestamp = metricNode.get("timestamp").longValue();
				final Map<String, String> tags = JSONOps.parseToObject(metricNode.get("tags"), TR_HASH_MAP);
				smetrics.add(new StreamedMetricValue(
					strValue.indexOf('.')==-1 ? Long.parseLong(strValue.trim()) : Double.parseDouble(strValue.trim()),
					metric,
					tags
				).updateTimestamp(timestamp));				
			} catch (Exception ex) {
				log.error("Failed to unmarshal metric node [{}]", metricNode, ex);
			}
		}
		mf.sendnr(metricTopicName, smetrics);
		//mf.send(new ProducerRecord<String, StreamedMetric>(metricTopicName, smv));
		log.info("Processed [{}] Metrics", smetrics.size());
	}

}


/** MetaData Examples:
==========================
[INFO ] 2016-11-08 18:53:43.588 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.dsackofosent","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.588 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.rcvcoalesce","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.588 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.estabresets","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.588 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.msec_total","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.588 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.msec_write","Tags":{"dev":"sdb","host":"tpmint"},"Name":"desc","Value":"Total number of ms spent by all writes."}]
[INFO ] 2016-11-08 18:53:43.588 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.part.msec_weighted_total","Tags":{"dev":"dm-1","host":"tpmint"},"Name":"desc","Value":"Measure of recent I/O completion time and backlog."}]
[INFO ] 2016-11-08 18:53:43.588 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.packets","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.588 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.interrupts","Tags":{"cpu":"2","host":"tpmint","type":"MCE"},"Name":"desc","Value":"Machine check exceptions."}]
[INFO ] 2016-11-08 18:53:43.588 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.renofailures","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.588 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.part.msec_weighted_total","Name":"unit","Value":"milliseconds"}]
[INFO ] 2016-11-08 18:53:43.589 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"os.net.errs","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.589 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.slab","Name":"unit","Value":"kbytes"}]
[INFO ] 2016-11-08 18:53:43.589 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.numa_interleave","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.589 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.compact_isolated","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.589 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.interrupts","Tags":{"cpu":"0","host":"tpmint","type":"RES"},"Name":"desc","Value":"Rescheduling interrupts."}]
[INFO ] 2016-11-08 18:53:43.589 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"os.net.ifspeed","Name":"unit","Value":"Mbit"}]
[INFO ] 2016-11-08 18:53:43.589 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.pginodesteal","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.589 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.loadavg_total_threads","Name":"unit","Value":"processes"}]
[INFO ] 2016-11-08 18:53:43.589 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.sackshifted","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.589 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.icmp.outaddrmaskreps","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.589 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.cpu.percpu","Tags":{"cpu":"0","host":"tpmint","type":"idle"},"Name":"desc","Value":"Twiddling thumbs."}]
[INFO ] 2016-11-08 18:53:43.589 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.icmp.inmsgs","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.590 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.cpu.percpu","Tags":{"cpu":"3","host":"tpmint","type":"guest"},"Name":"desc","Value":"Running a guest vm."}]
[INFO ] 2016-11-08 18:53:43.590 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collect.post.batchsize_median","Name":"rate","Value":"gauge"}]
[INFO ] 2016-11-08 18:53:43.590 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.directmap4k","Name":"unit","Value":"kbytes"}]
[INFO ] 2016-11-08 18:53:43.590 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.renoreorder","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.590 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.part.write_requests","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.590 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.pgpg","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.590 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.pgsteal_kswapd_movable","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.590 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.sockets.tcp_orphaned","Name":"rate","Value":"gauge"}]
[INFO ] 2016-11-08 18:53:43.590 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.sockets.udplite_in_use","Name":"rate","Value":"gauge"}]
[INFO ] 2016-11-08 18:53:43.590 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.ip.inect0pkts","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.590 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.msec_weighted_total","Name":"unit","Value":"milliseconds"}]
[INFO ] 2016-11-08 18:53:43.590 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.numa_local","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.590 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.fastopenactive","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.590 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.udp.incsumerrors","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.590 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.part.msec_total","Name":"unit","Value":"milliseconds"}]
[INFO ] 2016-11-08 18:53:43.590 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.interrupts","Tags":{"cpu":"0","host":"tpmint","type":"CAL"},"Name":"desc","Value":"Funcation call interupts."}]
[INFO ] 2016-11-08 18:53:43.591 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.read_requests","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.591 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.buffers","Name":"unit","Value":"kbytes"}]
[INFO ] 2016-11-08 18:53:43.591 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.part.write_merged","Tags":{"dev":"sdb1","host":"tpmint"},"Name":"desc","Value":" Adjacent write requests merged in a single req."}]
[INFO ] 2016-11-08 18:53:43.591 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.msec_total","Tags":{"dev":"dm-0","host":"tpmint"},"Name":"desc","Value":"Amount of time during which ios_in_progress >= 1."}]
[INFO ] 2016-11-08 18:53:43.591 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.icmp.outaddrmasks","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.591 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.icmpmsg.intype3","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.591 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.sreclaimable","Name":"rate","Value":"gauge"}]
[INFO ] 2016-11-08 18:53:43.591 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.hugepagesize","Name":"rate","Value":"gauge"}]
[INFO ] 2016-11-08 18:53:43.591 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collector.error","Tags":{"collector":"bosun.org/cmd/scollector/collectors.c_snmp_keepalived_vrrp_instances","host":"tpmint","os":"linux"},"Name":"desc","Value":"Status of collector run. 1=Error, 0=Success."}]
[INFO ] 2016-11-08 18:53:43.591 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.dirty","Name":"rate","Value":"gauge"}]
[INFO ] 2016-11-08 18:53:43.591 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.interrupts","Tags":{"cpu":"2","host":"tpmint","type":"TLB"},"Name":"desc","Value":"TLB (translation lookaside buffer) shootdowns."}]
[INFO ] 2016-11-08 18:53:43.591 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.retransfail","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.591 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collect.queued","Name":"rate","Value":"gauge"}]
[INFO ] 2016-11-08 18:53:43.591 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.part.msec_write","Tags":{"dev":"sda5","host":"tpmint"},"Name":"desc","Value":"Total number of ms spent by all writes."}]
[INFO ] 2016-11-08 18:53:43.592 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.anonhugepages","Name":"unit","Value":"kbytes"}]
[INFO ] 2016-11-08 18:53:43.592 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.pgalloc_normal","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.592 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.hphits","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.592 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collect.post.error","Name":"desc","Value":"Counter of errors received when sending a batch to the server."}]
[INFO ] 2016-11-08 18:53:43.592 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Tags":{"host":"tpmint","iface":"Interface2"},"Name":"speed","Value":4294967295000000}]
[INFO ] 2016-11-08 18:53:43.592 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collector.duration","Tags":{"collector":"bosun.org/cmd/scollector/collectors.c_nodestats_cfstats_linux","host":"tpmint","os":"linux"},"Name":"desc","Value":"Duration in seconds for each collector run."}]
[INFO ] 2016-11-08 18:53:43.592 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.version","Name":"desc","Value":"Scollector version number, which indicates when scollector was built."}]
[INFO ] 2016-11-08 18:53:43.592 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.nr_alloc_batch","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.592 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.icmp.outerrors","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.592 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.memfree","Name":"unit","Value":"kbytes"}]
[INFO ] 2016-11-08 18:53:43.592 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.cpu.percpu","Tags":{"cpu":"2","host":"tpmint","type":"user"},"Name":"desc","Value":"Normal processes executing in user mode."}]
[INFO ] 2016-11-08 18:53:43.592 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.retranssegs","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.592 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collect.post.restore","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.592 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collector.duration","Name":"unit","Value":"seconds"}]
[INFO ] 2016-11-08 18:53:43.592 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.sunreclaim","Name":"rate","Value":"gauge"}]
[INFO ] 2016-11-08 18:53:43.593 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.directmap4k","Name":"rate","Value":"gauge"}]
[INFO ] 2016-11-08 18:53:43.593 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.pgalloc_dma","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.593 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"os.disk.fs.space_total","Name":"unit","Value":"bytes"}]
[INFO ] 2016-11-08 18:53:43.593 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.version","Name":"unit","Value":""}]
[INFO ] 2016-11-08 18:53:43.593 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.cpu.percpu","Tags":{"cpu":"3","host":"tpmint","type":"nice"},"Name":"desc","Value":"Niced processes executing in user mode."}]
[INFO ] 2016-11-08 18:53:43.593 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collect.post.duration_median","Name":"desc","Value":"How many milliseconds it took to send HTTP POST requests to the server."}]
[INFO ] 2016-11-08 18:53:43.593 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collect.goroutines","Name":"rate","Value":"gauge"}]
[INFO ] 2016-11-08 18:53:43.593 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collector.error","Tags":{"collector":"bosun.org/cmd/scollector/collectors.c_netbackup_jobs","host":"tpmint","os":"linux"},"Name":"desc","Value":"Status of collector run. 1=Error, 0=Success."}]
[INFO ] 2016-11-08 18:53:43.593 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.part.time_per_write","Name":"unit","Value":"milliseconds"}]
[INFO ] 2016-11-08 18:53:43.593 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"os.net.ifspeed","Name":"rate","Value":"gauge"}]
[INFO ] 2016-11-08 18:53:43.593 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collector.duration","Tags":{"collector":"bosun.org/cmd/scollector/collectors.c_memcached_stats","host":"tpmint","os":"linux"},"Name":"desc","Value":"Duration in seconds for each collector run."}]
[INFO ] 2016-11-08 18:53:43.593 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.sackreneging","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.593 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collector.error","Tags":{"collector":"bosun.org/cmd/scollector/collectors.c_memcached_stats","host":"tpmint","os":"linux"},"Name":"desc","Value":"Status of collector run. 1=Error, 0=Success."}]
[INFO ] 2016-11-08 18:53:43.593 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.fs.space_used","Tags":{"host":"tpmint","mount":"/media/nwhitehead/SOLID"},"Name":"desc","Value":"The space_used property indicates in bytes how much space is used on the disk."}]
[INFO ] 2016-11-08 18:53:43.593 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.cpu.percpu","Tags":{"cpu":"2","host":"tpmint","type":"softirq"},"Name":"desc","Value":"Servicing soft irqs."}]
[INFO ] 2016-11-08 18:53:43.593 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.writeback","Name":"rate","Value":"gauge"}]
[INFO ] 2016-11-08 18:53:43.594 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.renorecoveryfail","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.594 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.ip.reasmreqds","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.594 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.part.msec_read","Tags":{"dev":"sda1","host":"tpmint"},"Name":"desc","Value":"Total number of ms spent by all reads."}]
[INFO ] 2016-11-08 18:53:43.594 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.interrupts","Tags":{"cpu":"3","host":"tpmint","type":"NMI"},"Name":"desc","Value":"Non-maskable interrupts."}]
[INFO ] 2016-11-08 18:53:43.594 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Tags":{"host":"tpmint","iface":"Interface2"},"Name":"mac","Value":"3c:97:0e:bb:26:8a"}]
[INFO ] 2016-11-08 18:53:43.594 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.part.msec_write","Tags":{"dev":"sdb1","host":"tpmint"},"Name":"desc","Value":"Total number of ms spent by all writes."}]
[INFO ] 2016-11-08 18:53:43.594 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.nr_writeback_temp","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.594 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collect.post.batchsize_95","Name":"desc","Value":"Number of datapoints included in each batch."}]
[INFO ] 2016-11-08 18:53:43.594 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collector.error","Tags":{"collector":"bosun.org/cmd/scollector/collectors.c_omreport_fans","host":"tpmint","os":"linux"},"Name":"desc","Value":"Status of collector run. 1=Error, 0=Success."}]
[INFO ] 2016-11-08 18:53:43.594 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.cpu.percpu","Tags":{"cpu":"2","host":"tpmint","type":"idle"},"Name":"desc","Value":"Twiddling thumbs."}]
[INFO ] 2016-11-08 18:53:43.594 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.md5notfound","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.594 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Tags":{"host":"tpmint","iface":"Interface2"},"Name":"name","Value":"eth0"}]
[INFO ] 2016-11-08 18:53:43.594 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.committed_as","Name":"unit","Value":"kbytes"}]
[INFO ] 2016-11-08 18:53:43.594 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.pgfault","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.594 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.cpu.percpu","Tags":{"cpu":"2","host":"tpmint","type":"irq"},"Name":"desc","Value":"Servicing interrupts."}]
[INFO ] 2016-11-08 18:53:43.594 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.outofwindowicmps","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.595 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.msec_read","Tags":{"dev":"sda","host":"tpmint"},"Name":"desc","Value":"Total number of ms spent by all reads."}]
[INFO ] 2016-11-08 18:53:43.595 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.msec_total","Name":"unit","Value":"milliseconds"}]
[INFO ] 2016-11-08 18:53:43.595 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.numa_foreign","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.595 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.icmp.outparmprobs","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.595 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"os.mem.total","Name":"rate","Value":"gauge"}]
[INFO ] 2016-11-08 18:53:43.595 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.nr_slab_unreclaimable","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.595 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.sackshiftfallback","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.595 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collect.post.total_duration","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.595 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.anonpages","Name":"unit","Value":"kbytes"}]
[INFO ] 2016-11-08 18:53:43.595 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.numa_miss","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.595 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"os.disk.fs.space_total","Name":"rate","Value":"gauge"}]
[INFO ] 2016-11-08 18:53:43.595 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.abortonlinger","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.595 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.ip.indelivers","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.595 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.fs.open","Name":"rate","Value":"gauge"}]
[INFO ] 2016-11-08 18:53:43.595 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.hugepagesize","Name":"unit","Value":"kbytes"}]
[INFO ] 2016-11-08 18:53:43.595 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.nr_written","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.596 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.cpu","Tags":{"host":"tpmint","type":"iowait"},"Name":"desc","Value":"Waiting for I/O to complete."}]
[INFO ] 2016-11-08 18:53:43.596 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collector.error","Tags":{"collector":"bosun.org/cmd/scollector/collectors.c_meta_linux_ifaces","host":"tpmint","os":"linux"},"Name":"desc","Value":"Status of collector run. 1=Error, 0=Success."}]
[INFO ] 2016-11-08 18:53:43.596 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.interrupts","Tags":{"cpu":"3","host":"tpmint","type":"SPU"},"Name":"desc","Value":"Spurious interrupts."}]
[INFO ] 2016-11-08 18:53:43.596 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.udp.indatagrams","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.596 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collect.queued","Name":"unit","Value":"items"}]
[INFO ] 2016-11-08 18:53:43.596 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collector.duration","Tags":{"collector":"bosun.org/cmd/scollector/collectors.yum_update_stats_linux","host":"tpmint","os":"linux"},"Name":"desc","Value":"Duration in seconds for each collector run."}]
[INFO ] 2016-11-08 18:53:43.596 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collector.error","Tags":{"collector":"bosun.org/cmd/scollector/collectors.c_meta_linux_serial","host":"tpmint","os":"linux"},"Name":"desc","Value":"Status of collector run. 1=Error, 0=Success."}]
[INFO ] 2016-11-08 18:53:43.596 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"os.disk.fs.space_free","Tags":{"disk":"/media/nwhitehead/SOLID","host":"tpmint"},"Name":"desc","Value":"The space_free property indicates in bytes how much free space is available on the disk."}]
[INFO ] 2016-11-08 18:53:43.596 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.lostretransmit","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.596 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.fs.open","Tags":{"host":"tpmint"},"Name":"desc","Value":"The number of files presently open."}]
[INFO ] 2016-11-08 18:53:43.596 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.part.time_per_read","Name":"rate","Value":"rate"}]
[INFO ] 2016-11-08 18:53:43.596 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.compact_fail","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.596 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.interrupts","Tags":{"cpu":"2","host":"tpmint","type":"IWI"},"Name":"desc","Value":"IRQ work interrupts."}]
[INFO ] 2016-11-08 18:53:43.596 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.insegs","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.596 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.write_merged","Tags":{"dev":"sda","host":"tpmint"},"Name":"desc","Value":" Adjacent write requests merged in a single req."}]
[INFO ] 2016-11-08 18:53:43.597 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.swaptotal","Name":"unit","Value":"kbytes"}]
[INFO ] 2016-11-08 18:53:43.597 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.icmp.inredirects","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.597 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collect.post.batchsize_median","Name":"unit","Value":""}]
[INFO ] 2016-11-08 18:53:43.597 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.msec_total","Tags":{"dev":"sda","host":"tpmint"},"Name":"desc","Value":"Amount of time during which ios_in_progress >= 1."}]
[INFO ] 2016-11-08 18:53:43.597 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.part.write_requests","Tags":{"dev":"sda5","host":"tpmint"},"Name":"desc","Value":"Total number of writes completed successfully."}]
[INFO ] 2016-11-08 18:53:43.597 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.bounce","Name":"unit","Value":"kbytes"}]
[INFO ] 2016-11-08 18:53:43.597 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.fs.space_used","Name":"rate","Value":"gauge"}]
[INFO ] 2016-11-08 18:53:43.597 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collector.duration","Tags":{"collector":"bosun.org/cmd/scollector/collectors.c_redis","host":"tpmint","os":"linux"},"Name":"desc","Value":"Duration in seconds for each collector run."}]
[INFO ] 2016-11-08 18:53:43.597 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.cpu","Tags":{"host":"tpmint","type":"idle"},"Name":"desc","Value":"Twiddling thumbs."}]
[INFO ] 2016-11-08 18:53:43.597 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.sockets.tcp_in_use","Name":"rate","Value":"gauge"}]
[INFO ] 2016-11-08 18:53:43.597 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.ip.outbcastpkts","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.597 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.synchallenge","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.597 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.icmp.outmsgs","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.597 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collect.post.duration_99","Name":"unit","Value":"milliseconds"}]
[INFO ] 2016-11-08 18:53:43.597 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collector.error","Tags":{"collector":"bosun.org/cmd/scollector/collectors.c_omreport_volts","host":"tpmint","os":"linux"},"Name":"desc","Value":"Status of collector run. 1=Error, 0=Success."}]
[INFO ] 2016-11-08 18:53:43.597 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.part.write_sectors","Tags":{"dev":"sda5","host":"tpmint"},"Name":"desc","Value":"Total number of sectors written successfully."}]
[INFO ] 2016-11-08 18:53:43.598 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.nfs_unstable","Name":"rate","Value":"gauge"}]
[INFO ] 2016-11-08 18:53:43.598 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.cpu","Tags":{"host":"tpmint","type":"guest_nice"},"Name":"desc","Value":"Running a niced guest vm."}]
[INFO ] 2016-11-08 18:53:43.598 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.sockets.udp_in_use","Name":"rate","Value":"gauge"}]
[INFO ] 2016-11-08 18:53:43.598 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.hardwarecorrupted","Name":"unit","Value":"kbytes"}]
[INFO ] 2016-11-08 18:53:43.598 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collector.duration","Tags":{"collector":"bosun.org/cmd/scollector/collectors.c_omreport_fans","host":"tpmint","os":"linux"},"Name":"desc","Value":"Duration in seconds for each collector run."}]
[INFO ] 2016-11-08 18:53:43.598 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.mlocked","Name":"rate","Value":"gauge"}]
[INFO ] 2016-11-08 18:53:43.598 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.pgscan_kswapd_dma","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.598 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.interrupts","Name":"unit","Value":"interupts"}]
[INFO ] 2016-11-08 18:53:43.598 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.memorypressures","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.598 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.ip.outoctets","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.598 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collect.post.batchsize_max","Name":"rate","Value":"gauge"}]
[INFO ] 2016-11-08 18:53:43.598 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.part.msec_weighted_total","Tags":{"dev":"sda5","host":"tpmint"},"Name":"desc","Value":"Measure of recent I/O completion time and backlog."}]
[INFO ] 2016-11-08 18:53:43.598 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collector.duration","Tags":{"collector":"bosun.org/cmd/scollector/collectors.c_omreport_storage_controller","host":"tpmint","os":"linux"},"Name":"desc","Value":"Duration in seconds for each collector run."}]
[INFO ] 2016-11-08 18:53:43.598 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.loadavg_runnable","Name":"unit","Value":"processes"}]
[INFO ] 2016-11-08 18:53:43.598 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"os.mem.total","Tags":{"host":"tpmint"},"Name":"desc","Value":"Total amount, in bytes, of physical memory available to the operating system."}]
[INFO ] 2016-11-08 18:53:43.598 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.nr_unevictable","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.599 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.pswp","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.599 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.cpu.percpu","Tags":{"cpu":"2","host":"tpmint","type":"guest"},"Name":"desc","Value":"Running a guest vm."}]
[INFO ] 2016-11-08 18:53:43.599 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.pawspassive","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.599 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.part.msec_write","Name":"unit","Value":"milliseconds"}]
[INFO ] 2016-11-08 18:53:43.599 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.collisions","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.599 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.part.read_sectors","Tags":{"dev":"sda5","host":"tpmint"},"Name":"desc","Value":"Total number of sectors read successfully."}]
[INFO ] 2016-11-08 18:53:43.599 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.nr_dirty","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.599 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.pgpg","Name":"unit","Value":"pages"}]
[INFO ] 2016-11-08 18:53:43.599 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.passiveopens","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.599 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.unevictable_pgs_stranded","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.599 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.sockets.udp_mem","Name":"unit","Value":"pages"}]
[INFO ] 2016-11-08 18:53:43.599 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.dsackignoredold","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.599 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.interrupts","Tags":{"cpu":"1","host":"tpmint","type":"PMI"},"Name":"desc","Value":"Performance monitoring interrupts."}]
[INFO ] 2016-11-08 18:53:43.599 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.ip.inunknownprotos","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.599 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.nr_slab_reclaimable","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.599 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.pgsteal_direct_movable","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.599 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Tags":{"addr":"Addr0","host":"tpmint","iface":"Interface4"},"Name":"addr","Value":"172.17.42.1"}]
[INFO ] 2016-11-08 18:53:43.600 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"os.disk.fs.space_free","Tags":{"disk":"/boot","host":"tpmint"},"Name":"desc","Value":"The space_free property indicates in bytes how much free space is available on the disk."}]
[INFO ] 2016-11-08 18:53:43.600 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.cpu.percpu","Tags":{"cpu":"2","host":"tpmint","type":"steal"},"Name":"desc","Value":"Involuntary wait."}]
[INFO ] 2016-11-08 18:53:43.600 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collector.duration","Tags":{"collector":"bosun.org/cmd/scollector/collectors.c_meta_linux_ifaces","host":"tpmint","os":"linux"},"Name":"desc","Value":"Duration in seconds for each collector run."}]
[INFO ] 2016-11-08 18:53:43.600 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.cpu.clock","Tags":{"cpu":"3","host":"tpmint"},"Name":"desc","Value":"The current speed of the processor in MHz."}]
[INFO ] 2016-11-08 18:53:43.600 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.interrupts","Tags":{"cpu":"1","host":"tpmint","type":"TLB"},"Name":"desc","Value":"TLB (translation lookaside buffer) shootdowns."}]
[INFO ] 2016-11-08 18:53:43.600 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.interrupts","Tags":{"cpu":"1","host":"tpmint","type":"TRM"},"Name":"desc","Value":"Thermal event interrupts."}]
[INFO ] 2016-11-08 18:53:43.600 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.fastretrans","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.600 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collect.post.duration_95","Name":"rate","Value":"gauge"}]
[INFO ] 2016-11-08 18:53:43.600 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.read_merged","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.600 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collector.error","Tags":{"collector":"bosun.org/cmd/scollector/collectors.c_omreport_ps","host":"tpmint","os":"linux"},"Name":"desc","Value":"Status of collector run. 1=Error, 0=Success."}]
[INFO ] 2016-11-08 18:53:43.600 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.kswapd_high_wmark_hit_quickly","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.600 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.interrupts","Tags":{"cpu":"3","host":"tpmint","type":"LOC"},"Name":"desc","Value":"Local timer interrupts."}]
[INFO ] 2016-11-08 18:53:43.600 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.prequeuedropped","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.600 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collector.error","Tags":{"collector":"bosun.org/cmd/scollector/collectors.yum_update_stats_linux","host":"tpmint","os":"linux"},"Name":"desc","Value":"Status of collector run. 1=Error, 0=Success."}]
[INFO ] 2016-11-08 18:53:43.600 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.part.write_merged","Tags":{"dev":"dm-1","host":"tpmint"},"Name":"desc","Value":" Adjacent write requests merged in a single req."}]
[INFO ] 2016-11-08 18:53:43.601 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.cpu.percpu","Tags":{"cpu":"2","host":"tpmint","type":"nice"},"Name":"desc","Value":"Niced processes executing in user mode."}]
[INFO ] 2016-11-08 18:53:43.601 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collect.post.bad_status","Name":"desc","Value":"Counter of HTTP POST requests where resp.StatusCode != http.StatusNoContent."}]
[INFO ] 2016-11-08 18:53:43.601 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collector.error","Tags":{"collector":"bosun.org/cmd/scollector/collectors.c_ntp_peers_unix","host":"tpmint","os":"linux"},"Name":"desc","Value":"Status of collector run. 1=Error, 0=Success."}]
[INFO ] 2016-11-08 18:53:43.601 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collector.error","Tags":{"collector":"bosun.org/cmd/scollector/collectors.c_omreport_storage_enclosure","host":"tpmint","os":"linux"},"Name":"desc","Value":"Status of collector run. 1=Error, 0=Success."}]
[INFO ] 2016-11-08 18:53:43.601 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.nr_bounce","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.601 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.embryonicrsts","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.601 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collect.post.duration_95","Name":"unit","Value":"milliseconds"}]
[INFO ] 2016-11-08 18:53:43.601 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collector.duration","Tags":{"collector":"bosun.org/cmd/scollector/collectors.c_omreport_volts","host":"tpmint","os":"linux"},"Name":"desc","Value":"Duration in seconds for each collector run."}]
[INFO ] 2016-11-08 18:53:43.601 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.bytes","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.601 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.fifo_errs","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.601 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.nr_page_table_pages","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.601 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.sockets.udplite_in_use","Name":"unit","Value":"sockets"}]
[INFO ] 2016-11-08 18:53:43.601 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collect.goroutines","Name":"unit","Value":""}]
[INFO ] 2016-11-08 18:53:43.601 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.nr_isolated_anon","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.601 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collector.duration","Tags":{"collector":"bosun.org/cmd/scollector/collectors.c_netbackup_frequency","host":"tpmint","os":"linux"},"Name":"desc","Value":"Duration in seconds for each collector run."}]
[INFO ] 2016-11-08 18:53:43.601 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collector.duration","Tags":{"collector":"bosun.org/cmd/scollector/collectors.c_ntp_peers_unix","host":"tpmint","os":"linux"},"Name":"desc","Value":"Duration in seconds for each collector run."}]
[INFO ] 2016-11-08 18:53:43.602 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.nr_writeback","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.602 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.cpu.percpu","Tags":{"cpu":"1","host":"tpmint","type":"system"},"Name":"desc","Value":"Processes executing in kernel mode."}]
[INFO ] 2016-11-08 18:53:43.602 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.lossfailures","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.602 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collect.post.duration_count","Name":"rate","Value":"gauge"}]
[INFO ] 2016-11-08 18:53:43.602 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collector.duration","Tags":{"collector":"bosun.org/cmd/scollector/collectors.c_omreport_storage_battery","host":"tpmint","os":"linux"},"Name":"desc","Value":"Duration in seconds for each collector run."}]
[INFO ] 2016-11-08 18:53:43.602 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.cpu.percpu","Tags":{"cpu":"0","host":"tpmint","type":"system"},"Name":"desc","Value":"Processes executing in kernel mode."}]
[INFO ] 2016-11-08 18:53:43.602 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.sockets.used","Name":"unit","Value":"sockets"}]
[INFO ] 2016-11-08 18:53:43.602 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.ofopruned","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.602 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.listenoverflows","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.602 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collect.post.restore","Name":"desc","Value":"Counter of data points restored from batches that could not be sent to the server."}]
[INFO ] 2016-11-08 18:53:43.602 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.read_sectors","Tags":{"dev":"sda","host":"tpmint"},"Name":"desc","Value":"Total number of sectors read successfully."}]
[INFO ] 2016-11-08 18:53:43.602 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collector.duration","Tags":{"collector":"bosun.org/cmd/scollector/collectors.c_omreport_system","host":"tpmint","os":"linux"},"Name":"desc","Value":"Duration in seconds for each collector run."}]
[INFO ] 2016-11-08 18:53:43.602 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"os.cpu.clock","Tags":{"cpu":"2","host":"tpmint"},"Name":"desc","Value":"The current speed of the processor in MHz."}]
[INFO ] 2016-11-08 18:53:43.602 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.part.msec_weighted_total","Name":"rate","Value":"gauge"}]
[INFO ] 2016-11-08 18:53:43.602 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"os.net.packets","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.602 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.numa_pte_updates","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.602 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.fs.space_free","Tags":{"host":"tpmint","mount":"/"},"Name":"desc","Value":"The space_free property indicates in bytes how much free space is available on the disk."}]
[INFO ] 2016-11-08 18:53:43.602 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.fackreorder","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.603 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.ip.fragoks","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.603 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.pawsestab","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.603 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collect.gc.total_pause","Name":"unit","Value":"milliseconds"}]
[INFO ] 2016-11-08 18:53:43.603 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collector.duration","Tags":{"collector":"bosun.org/cmd/scollector/collectors.c_netbackup_jobs","host":"tpmint","os":"linux"},"Name":"desc","Value":"Duration in seconds for each collector run."}]
[INFO ] 2016-11-08 18:53:43.603 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.write_requests","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.603 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.part.time_per_write","Name":"rate","Value":"rate"}]
[INFO ] 2016-11-08 18:53:43.603 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collector.duration","Tags":{"collector":"bosun.org/cmd/scollector/collectors.c_omreport_ps","host":"tpmint","os":"linux"},"Name":"desc","Value":"Duration in seconds for each collector run."}]
[INFO ] 2016-11-08 18:53:43.603 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.sockets.tcp_time_wait","Name":"unit","Value":"sockets"}]
[INFO ] 2016-11-08 18:53:43.603 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.thp_fault_alloc","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.603 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.cpu","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.603 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.loadavg_1_min","Name":"rate","Value":"gauge"}]
[INFO ] 2016-11-08 18:53:43.603 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.part.msec_write","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.603 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.part.msec_write","Tags":{"dev":"sda2","host":"tpmint"},"Name":"desc","Value":"Total number of ms spent by all writes."}]
[INFO ] 2016-11-08 18:53:43.603 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"os.mem.free","Name":"unit","Value":"bytes"}]
[INFO ] 2016-11-08 18:53:43.603 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.cpu","Name":"unit","Value":"CentiHertz"}]
[INFO ] 2016-11-08 18:53:43.603 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.read_requests","Tags":{"dev":"sdb","host":"tpmint"},"Name":"desc","Value":"Total number of reads completed successfully."}]
[INFO ] 2016-11-08 18:53:43.604 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.bytes","Tags":{"dev":"dm-0","host":"tpmint","type":"read"},"Name":"desc","Value":"Total number of bytes read to disk."}]
[INFO ] 2016-11-08 18:53:43.604 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.ip.fragcreates","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.604 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collect.post.duration_max","Name":"desc","Value":"How many milliseconds it took to send HTTP POST requests to the server."}]
[INFO ] 2016-11-08 18:53:43.604 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collect.post.duration_99","Name":"rate","Value":"gauge"}]
[INFO ] 2016-11-08 18:53:43.604 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"os.mem.percent_free","Name":"rate","Value":"gauge"}]
[INFO ] 2016-11-08 18:53:43.604 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.unevictable_pgs_rescued","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.604 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.cpu.percpu","Tags":{"cpu":"1","host":"tpmint","type":"iowait"},"Name":"desc","Value":"Waiting for I/O to complete."}]
[INFO ] 2016-11-08 18:53:43.604 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Tags":{"addr":"Addr1","host":"tpmint","iface":"Interface5"},"Name":"addr","Value":"fe80::250:56ff:fec0:1"}]
[INFO ] 2016-11-08 18:53:43.604 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.part.write_requests","Tags":{"dev":"sda1","host":"tpmint"},"Name":"desc","Value":"Total number of writes completed successfully."}]
[INFO ] 2016-11-08 18:53:43.604 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.shmem","Name":"rate","Value":"gauge"}]
[INFO ] 2016-11-08 18:53:43.604 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.pgalloc_dma32","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.604 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.pgsteal_kswapd_normal","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.604 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collect.post.total_bytes","Name":"unit","Value":"bytes"}]
[INFO ] 2016-11-08 18:53:43.604 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.ip_count","Name":"unit","Value":"IP_Addresses"}]
[INFO ] 2016-11-08 18:53:43.604 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.pgscan_direct_dma","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.604 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.interrupts","Tags":{"cpu":"3","host":"tpmint","type":"IWI"},"Name":"desc","Value":"IRQ work interrupts."}]
[INFO ] 2016-11-08 18:53:43.604 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.interrupts","Tags":{"cpu":"3","host":"tpmint","type":"RES"},"Name":"desc","Value":"Rescheduling interrupts."}]
[INFO ] 2016-11-08 18:53:43.604 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.sackrecoveryfail","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.604 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Tags":{"host":"tpmint","iface":"Interface3"},"Name":"name","Value":"wlan0"}]
[INFO ] 2016-11-08 18:53:43.604 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.interrupts","Tags":{"cpu":"3","host":"tpmint","type":"TRM"},"Name":"desc","Value":"Thermal event interrupts."}]
[INFO ] 2016-11-08 18:53:43.604 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.inactive","Name":"unit","Value":"kbytes"}]
[INFO ] 2016-11-08 18:53:43.604 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.part.ios_in_progress","Tags":{"dev":"sda2","host":"tpmint"},"Name":"desc","Value":"Number of actual I/O requests currently in flight."}]
[INFO ] 2016-11-08 18:53:43.604 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.numa_huge_pte_updates","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.604 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collector.error","Tags":{"collector":"bosun.org/cmd/scollector/collectors.c_iostat_linux","host":"tpmint","os":"linux"},"Name":"desc","Value":"Status of collector run. 1=Error, 0=Success."}]
[INFO ] 2016-11-08 18:53:43.604 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.loadavg_5_min","Name":"rate","Value":"gauge"}]
[INFO ] 2016-11-08 18:53:43.604 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.interrupts","Tags":{"cpu":"1","host":"tpmint","type":"THR"},"Name":"desc","Value":"Threshold APIC interrupts."}]
[INFO ] 2016-11-08 18:53:43.604 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collect.post.batchsize_max","Name":"unit","Value":""}]
[INFO ] 2016-11-08 18:53:43.604 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collector.duration","Name":"rate","Value":"gauge"}]
[INFO ] 2016-11-08 18:53:43.604 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.numa_pages_migrated","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.605 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.delayedacklocked","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.605 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.sockets.frag_mem","Name":"unit","Value":"bytes"}]
[INFO ] 2016-11-08 18:53:43.605 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.inactive","Name":"rate","Value":"gauge"}]
[INFO ] 2016-11-08 18:53:43.605 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.thp_zero_page_alloc_failed","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.605 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.cpu.percpu","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.605 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.cached","Name":"unit","Value":"kbytes"}]
[INFO ] 2016-11-08 18:53:43.605 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.tcp.ofoqueue","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.605 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.cpu.percpu","Tags":{"cpu":"0","host":"tpmint","type":"nice"},"Name":"desc","Value":"Niced processes executing in user mode."}]
[INFO ] 2016-11-08 18:53:43.605 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.pgfree","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.605 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.cpu.percpu","Tags":{"cpu":"1","host":"tpmint","type":"idle"},"Name":"desc","Value":"Twiddling thumbs."}]
[INFO ] 2016-11-08 18:53:43.605 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.part.msec_read","Tags":{"dev":"sdb1","host":"tpmint"},"Name":"desc","Value":"Total number of ms spent by all reads."}]
[INFO ] 2016-11-08 18:53:43.605 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.vmalloctotal","Name":"unit","Value":"kbytes"}]
[INFO ] 2016-11-08 18:53:43.605 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.nr_tlb_remote_flush_received","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.605 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.cpu.percpu","Tags":{"cpu":"0","host":"tpmint","type":"guest_nice"},"Name":"desc","Value":"Running a niced guest vm."}]
[INFO ] 2016-11-08 18:53:43.605 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.loadavg_1_min","Name":"unit","Value":"load"}]
[INFO ] 2016-11-08 18:53:43.605 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.net.stat.ip.innoroutes","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.605 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.active","Name":"unit","Value":"kbytes"}]
[INFO ] 2016-11-08 18:53:43.605 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"os.mem.percent_free","Name":"unit","Value":"percent"}]
[INFO ] 2016-11-08 18:53:43.605 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.mem.nr_tlb_local_flush_all","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.605 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.interrupts","Name":"rate","Value":"counter"}]
[INFO ] 2016-11-08 18:53:43.605 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.interrupts","Tags":{"cpu":"2","host":"tpmint","type":"LOC"},"Name":"desc","Value":"Local timer interrupts."}]
[INFO ] 2016-11-08 18:53:43.605 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.collect.post.batchsize_count","Name":"unit","Value":""}]
[INFO ] 2016-11-08 18:53:43.605 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"scollector.hi","Name":"rate","Value":"gauge"}]
[INFO ] 2016-11-08 18:53:43.605 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.disk.fs.space_used","Tags":{"host":"tpmint","mount":"/"},"Name":"desc","Value":"The space_used property indicates in bytes how much space is used on the disk."}]
[INFO ] 2016-11-08 18:53:43.605 [EpollServerWorkerThread#2] HttpJsonRpcHandler - -------> [{"Metric":"linux.cpu","Tags":{"host":"tpmint","type":"guest"},"Name":"desc","Value":"Running a guest vm."}]



*/
