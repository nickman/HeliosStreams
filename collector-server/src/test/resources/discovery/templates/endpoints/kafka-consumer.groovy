/*
	Kafka Consumer Collection Script
	Whitehead, 2016
*/

/* =========================================================================
	Populate one timers
   ========================================================================= */	
@Field
hostTag = host;
@Field
appTag = app;
@Field
groupConsumerPattern = jmxHelper.objectName("com.heliosapm.streams.consumer:app=$appTag,host=$hostTag,group=*");
@Field
topicPartConsumerPattern = jmxHelper.objectName("com.heliosapm.streams.consumer:app=$appTag,host=$hostTag,group=*,partition=0,topic=*");
@Field
consumerAttributes = ["Msgs15mRate", "Msgs5mRate", "Msgs1mRate", "MsgsMeanRate", "MsgsRateCount"] as String[];
@Field 
deltaTraces = new HashSet(["MsgsRateCount"]);


tracer.reset().tags([host : hostTag, app : appTag, service : "kafkaclient", type : "consumer"]);


tracer {
	jmxClient.queryNames(groupConsumerPattern, null).each() { on ->
		tracer {
			try {
				tracer.pushTag("group", on.getKeyProperty("group"));
				ts = System.currentTimeMillis();		
				attributeValues = jmxHelper.getAttributes(on, jmxClient, consumerAttributes);
				attributeValues.each() { k, v ->
					if(deltaTraces.contains(k)) {
						tracer.pushSeg(k).dtrace(v, ts).popSeg();
					} else {
						tracer.pushSeg(k).trace(v, ts).popSeg();
					}
				}
			} catch (x) {
				log.error("Failed to collect on [{}]", on, x);					
			} finally {
				tracer.popTag();
			}
			
		}
	}
}
tracer {
	jmxClient.queryNames(topicPartConsumerPattern, null).each() { on ->
		tracer {
			try {
				tracer.pushTag("group", on.getKeyProperty("group"));
				tracer.pushTag("topic", on.getKeyProperty("topic"));
				tracer.pushTag("partition", on.getKeyProperty("partition"));
				ts = System.currentTimeMillis();					
				attributeValues = jmxHelper.getAttributes(on, jmxClient, consumerAttributes);
				attributeValues.each() { k, v ->
					tracer.pushSeg(k).trace(v, ts).popSeg();
				}
			} catch (x) {
				log.error("Failed to collect on [{}]", on, x);					
			} finally {
				tracer.popTag();
				tracer.popTag();
				tracer.popTag();
			}				
		}
	}
}

tracer.flush();
log.info("Completed Collection");

/*
Connecting to [service:jmx:jmxmp://localhost:1421]
Connector Acquired
kafka.consumer:type=consumer-coordinator-metrics,client-id=StreamHubNode.17255.tpmint-StreamThread-1-restore-consumer
StreamHubNodetpmintproducer
	"assigned-partitions","commit-latency-avg","commit-latency-max","commit-rate","heartbeat-rate","heartbeat-response-time-max","join-rate","join-time-avg","join-time-max","last-heartbeat-seconds-ago","sync-rate","sync-time-avg","sync-time-max",
kafka.consumer:type=consumer-fetch-manager-metrics,client-id=StreamHubNode.17255.tpmint-StreamThread-1-consumer,topic=tsdb_metrics_text_meter
StreamHubNodetpmintproducer
	"bytes-consumed-rate","fetch-size-avg","fetch-size-max","records-consumed-rate","records-per-request-avg",
kafka.consumer:type=consumer-fetch-manager-metrics,client-id=StreamHubNode.17255.tpmint-StreamThread-1-consumer
StreamHubNodetpmintproducer
	"bytes-consumed-rate","fetch-latency-avg","fetch-latency-max","fetch-rate","fetch-size-avg","fetch-size-max","fetch-throttle-time-avg","fetch-throttle-time-max","records-consumed-rate","records-lag-max","records-per-request-avg",
kafka.consumer:type=consumer-node-metrics,client-id=StreamHubNode.17255.tpmint-StreamThread-1-consumer,node-id=node-2147483647
StreamHubNodetpmintproducer
	"incoming-byte-rate","outgoing-byte-rate","request-latency-avg","request-latency-max","request-rate","request-size-avg","request-size-max","response-rate",
kafka.consumer:type=consumer-metrics,client-id=StreamHubNode.17255.tpmint-StreamThread-1-restore-consumer
StreamHubNodetpmintproducer
	"connection-close-rate","connection-count","connection-creation-rate","incoming-byte-rate","io-ratio","io-time-ns-avg","io-wait-ratio","io-wait-time-ns-avg","network-io-rate","outgoing-byte-rate","request-rate","request-size-avg","request-size-max","response-rate","select-rate",
kafka.consumer:type=consumer-fetch-manager-metrics,client-id=StreamHubNode.17255.tpmint-StreamThread-1-restore-consumer
StreamHubNodetpmintproducer
	"bytes-consumed-rate","fetch-latency-avg","fetch-latency-max","fetch-rate","fetch-size-avg","fetch-size-max","fetch-throttle-time-avg","fetch-throttle-time-max","records-consumed-rate","records-lag-max","records-per-request-avg",
kafka.consumer:type=consumer-node-metrics,client-id=StreamHubNode.17255.tpmint-StreamThread-1-consumer,node-id=node-0
StreamHubNodetpmintproducer
	"incoming-byte-rate","outgoing-byte-rate","request-latency-avg","request-latency-max","request-rate","request-size-avg","request-size-max","response-rate",
kafka.consumer:type=consumer-fetch-manager-metrics,client-id=StreamHubNode.17255.tpmint-StreamThread-1-consumer,topic=tsdb_metrics_meter
StreamHubNodetpmintproducer
	"bytes-consumed-rate","fetch-size-avg","fetch-size-max","records-consumed-rate","records-per-request-avg",
kafka.consumer:type=consumer-coordinator-metrics,client-id=StreamHubNode.17255.tpmint-StreamThread-1-consumer
StreamHubNodetpmintproducer
	"assigned-partitions","commit-latency-avg","commit-latency-max","commit-rate","heartbeat-rate","heartbeat-response-time-max","join-rate","join-time-avg","join-time-max","last-heartbeat-seconds-ago","sync-rate","sync-time-avg","sync-time-max",
kafka.consumer:type=consumer-metrics,client-id=StreamHubNode.17255.tpmint-StreamThread-1-consumer
StreamHubNodetpmintproducer
	"connection-close-rate","connection-count","connection-creation-rate","incoming-byte-rate","io-ratio","io-time-ns-avg","io-wait-ratio","io-wait-time-ns-avg","network-io-rate","outgoing-byte-rate","request-rate","request-size-avg","request-size-max","response-rate","select-rate",
kafka.consumer:type=consumer-node-metrics,client-id=StreamHubNode.17255.tpmint-StreamThread-1-consumer,node-id=node--1
StreamHubNodetpmintproducer
	"incoming-byte-rate","outgoing-byte-rate","request-latency-avg","request-latency-max","request-rate","request-size-avg","request-size-max","response-rate",
*/