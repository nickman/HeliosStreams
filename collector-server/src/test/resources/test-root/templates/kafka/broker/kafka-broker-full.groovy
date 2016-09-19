
@Field
hostTag = navmap_1;
@Field
appTag = navmap_0;
@Field
jmxClient = null;
@Field clusterUnderReplicated = [
	on : jmxHelper.objectName("kafka.cluster:topic=*,name=UnderReplicated,partition=*,type=Partition"),
	pattern : true,
	attrs : ["Value"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggrkeys : ["topic","partition"],
	aggr : true
];
@Field controllerActiveControllerCount = [
	on : jmxHelper.objectName("kafka.controller:type=KafkaController,name=ActiveControllerCount"),
	pattern : false,
	attrs : ["Value"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggr : false
];
@Field controllerLeaderElectionRateAndTimeMs = [
	on : jmxHelper.objectName("kafka.controller:type=ControllerStats,name=LeaderElectionRateAndTimeMs"),
	pattern : false,
	attrs : ["MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate","Max","Count","50thPercentile","StdDev","75thPercentile","95thPercentile","Min","Mean","98thPercentile","99thPercentile","999thPercentile"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggr : false
];
@Field controllerOfflinePartitionsCount = [
	on : jmxHelper.objectName("kafka.controller:type=KafkaController,name=OfflinePartitionsCount"),
	pattern : false,
	attrs : ["Value"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggr : false
];
@Field controllerPreferredReplicaImbalanceCount = [
	on : jmxHelper.objectName("kafka.controller:type=KafkaController,name=PreferredReplicaImbalanceCount"),
	pattern : false,
	attrs : ["Value"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggr : false
];
@Field controllerUncleanLeaderElectionsPerSec = [
	on : jmxHelper.objectName("kafka.controller:type=ControllerStats,name=UncleanLeaderElectionsPerSec"),
	pattern : false,
	attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggr : false
];
@Field coordinatorNumGroups = [
	on : jmxHelper.objectName("kafka.coordinator:type=GroupMetadataManager,name=NumGroups"),
	pattern : false,
	attrs : ["Value"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggr : false
];
@Field coordinatorNumOffsets = [
	on : jmxHelper.objectName("kafka.coordinator:type=GroupMetadataManager,name=NumOffsets"),
	pattern : false,
	attrs : ["Value"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggr : false
];
@Field logLogEndOffset = [
	on : jmxHelper.objectName("kafka.log:topic=*,name=LogEndOffset,partition=*,type=Log"),
	pattern : true,
	attrs : ["Value"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggrkeys : ["topic","partition"],
	aggr : true
];
@Field logLogFlushRateAndTimeMs = [
	on : jmxHelper.objectName("kafka.log:type=LogFlushStats,name=LogFlushRateAndTimeMs"),
	pattern : false,
	attrs : ["MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate","Max","Count","50thPercentile","StdDev","75thPercentile","95thPercentile","Min","Mean","98thPercentile","99thPercentile","999thPercentile"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggr : false
];
@Field logLogStartOffset = [
	on : jmxHelper.objectName("kafka.log:topic=*,name=LogStartOffset,partition=*,type=Log"),
	pattern : true,
	attrs : ["Value"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggrkeys : ["topic","partition"],
	aggr : true
];
@Field logNumLogSegments = [
	on : jmxHelper.objectName("kafka.log:topic=*,name=NumLogSegments,partition=*,type=Log"),
	pattern : true,
	attrs : ["Value"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggrkeys : ["topic","partition"],
	aggr : true
];
@Field logSize = [
	on : jmxHelper.objectName("kafka.log:topic=*,name=Size,partition=*,type=Log"),
	pattern : true,
	attrs : ["Value"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggrkeys : ["topic","partition"],
	aggr : true
];
@Field logcleaner_recopy_percent = [
	on : jmxHelper.objectName("kafka.log:type=LogCleaner,name=cleaner-recopy-percent"),
	pattern : false,
	attrs : ["Value"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggr : false
];
@Field logmax_buffer_utilization_percent = [
	on : jmxHelper.objectName("kafka.log:type=LogCleaner,name=max-buffer-utilization-percent"),
	pattern : false,
	attrs : ["Value"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggr : false
];
@Field logmax_clean_time_secs = [
	on : jmxHelper.objectName("kafka.log:type=LogCleaner,name=max-clean-time-secs"),
	pattern : false,
	attrs : ["Value"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggr : false
];
@Field logmax_dirty_percent = [
	on : jmxHelper.objectName("kafka.log:type=LogCleanerManager,name=max-dirty-percent"),
	pattern : false,
	attrs : ["Value"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggr : false
];
@Field networkIdlePercent = [
	on : jmxHelper.objectName("kafka.network:type=Processor,name=IdlePercent,networkProcessor=0"),
	pattern : false,
	attrs : ["Value"] as String[],
	segs : ["name"],
	keys : ["type","networkProcessor"],
	aggr : false
];
@Field networkLocalTimeMs = [
	on : jmxHelper.objectName("kafka.network:type=RequestMetrics,name=LocalTimeMs,request=Offsets"),
	pattern : false,
	attrs : ["Max","Count","50thPercentile","StdDev","75thPercentile","95thPercentile","Min","Mean","98thPercentile","99thPercentile","999thPercentile"] as String[],
	segs : ["name"],
	keys : ["type","request"],
	aggr : false
];
@Field networkNetworkProcessorAvgIdlePercent = [
	on : jmxHelper.objectName("kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent"),
	pattern : false,
	attrs : ["Value"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggr : false
];
@Field networkRemoteTimeMs = [
	on : jmxHelper.objectName("kafka.network:type=RequestMetrics,name=RemoteTimeMs,request=Produce"),
	pattern : false,
	attrs : ["Max","Count","50thPercentile","StdDev","75thPercentile","95thPercentile","Min","Mean","98thPercentile","99thPercentile","999thPercentile"] as String[],
	segs : ["name"],
	keys : ["type","request"],
	aggr : false
];
@Field networkRequestQueueSize = [
	on : jmxHelper.objectName("kafka.network:type=RequestChannel,name=RequestQueueSize"),
	pattern : false,
	attrs : ["Value"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggr : false
];
@Field networkRequestQueueTimeMs = [
	on : jmxHelper.objectName("kafka.network:type=RequestMetrics,name=RequestQueueTimeMs,request=LeaveGroup"),
	pattern : false,
	attrs : ["Max","Count","50thPercentile","StdDev","75thPercentile","95thPercentile","Min","Mean","98thPercentile","99thPercentile","999thPercentile"] as String[],
	segs : ["name"],
	keys : ["type","request"],
	aggr : false
];
@Field networkRequestsPerSec = [
	on : jmxHelper.objectName("kafka.network:type=RequestMetrics,name=RequestsPerSec,request=ApiVersions"),
	pattern : false,
	attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
	segs : ["name"],
	keys : ["type","request"],
	aggr : false
];
@Field networkResponseQueueSize = [
	on : jmxHelper.objectName("kafka.network:type=RequestChannel,name=ResponseQueueSize,processor=1"),
	pattern : false,
	attrs : ["Value"] as String[],
	segs : ["name"],
	keys : ["processor","type"],
	aggr : false
];
@Field networkResponseQueueTimeMs = [
	on : jmxHelper.objectName("kafka.network:type=RequestMetrics,name=ResponseQueueTimeMs,request=LeaderAndIsr"),
	pattern : false,
	attrs : ["Max","Count","50thPercentile","StdDev","75thPercentile","95thPercentile","Min","Mean","98thPercentile","99thPercentile","999thPercentile"] as String[],
	segs : ["name"],
	keys : ["type","request"],
	aggr : false
];
@Field networkResponseSendTimeMs = [
	on : jmxHelper.objectName("kafka.network:type=RequestMetrics,name=ResponseSendTimeMs,request=Produce"),
	pattern : false,
	attrs : ["Max","Count","50thPercentile","StdDev","75thPercentile","95thPercentile","Min","Mean","98thPercentile","99thPercentile","999thPercentile"] as String[],
	segs : ["name"],
	keys : ["type","request"],
	aggr : false
];
@Field networkThrottleTimeMs = [
	on : jmxHelper.objectName("kafka.network:type=RequestMetrics,name=ThrottleTimeMs,request=ApiVersions"),
	pattern : false,
	attrs : ["Max","Count","50thPercentile","StdDev","75thPercentile","95thPercentile","Min","Mean","98thPercentile","99thPercentile","999thPercentile"] as String[],
	segs : ["name"],
	keys : ["type","request"],
	aggr : false
];
@Field networkTotalTimeMs = [
	on : jmxHelper.objectName("kafka.network:type=RequestMetrics,name=TotalTimeMs,request=FetchConsumer"),
	pattern : false,
	attrs : ["Max","Count","50thPercentile","StdDev","75thPercentile","95thPercentile","Min","Mean","98thPercentile","99thPercentile","999thPercentile"] as String[],
	segs : ["name"],
	keys : ["type","request"],
	aggr : false
];
@Field server = [
	on : jmxHelper.objectName("kafka.server:type=Produce"),
	pattern : false,
	attrs : ["queue-size"] as String[],
	segs : [],
	keys : ["type"],
	aggr : false
];
@Field serverBrokerState = [
	on : jmxHelper.objectName("kafka.server:type=KafkaServer,name=BrokerState"),
	pattern : false,
	attrs : ["Value"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggr : false
];
@Field serverBytesInPerSec = [
	on : jmxHelper.objectName("kafka.server:topic=*,name=BytesInPerSec,type=BrokerTopicMetrics"),
	pattern : true,
	attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggrkeys : ["topic"],
	aggr : true
];
@Field serverBytesOutPerSec = [
	on : jmxHelper.objectName("kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec"),
	pattern : false,
	attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggr : false
];
@Field serverBytesPerSec = [
	on : jmxHelper.objectName("kafka.server:type=FetcherStats,name=BytesPerSec,clientId=*,brokerHost=*,brokerPort=*"),
	pattern : true,
	attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
	segs : ["name"],
	keys : ["clientId","brokerHost","type","brokerPort"],
	aggr : false
];
@Field serverBytesRejectedPerSec = [
	on : jmxHelper.objectName("kafka.server:topic=*,name=BytesRejectedPerSec,type=BrokerTopicMetrics"),
	pattern : true,
	attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggrkeys : ["topic"],
	aggr : true
];
@Field serverConsumerLag = [
	on : jmxHelper.objectName("kafka.server:clientId=*,topic=*,name=ConsumerLag,partition=*,type=FetcherLagMetrics"),
	pattern : true,
	attrs : ["Value"] as String[],
	segs : ["name"],
	keys : ["clientId","type"],
	aggrkeys : ["topic","partition"],
	aggr : true
];
@Field serverExpiresPerSec = [
	on : jmxHelper.objectName("kafka.server:type=DelayedFetchMetrics,name=ExpiresPerSec,fetcherType=consumer"),
	pattern : false,
	attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
	segs : ["name"],
	keys : ["type","fetcherType"],
	aggr : false
];
@Field serverFailedFetchRequestsPerSec = [
	on : jmxHelper.objectName("kafka.server:topic=*,name=FailedFetchRequestsPerSec,type=BrokerTopicMetrics"),
	pattern : true,
	attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggrkeys : ["topic"],
	aggr : true
];
@Field serverFailedProduceRequestsPerSec = [
	on : jmxHelper.objectName("kafka.server:topic=*,name=FailedProduceRequestsPerSec,type=BrokerTopicMetrics"),
	pattern : true,
	attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggrkeys : ["topic"],
	aggr : true
];
@Field serverIsrExpandsPerSec = [
	on : jmxHelper.objectName("kafka.server:type=ReplicaManager,name=IsrExpandsPerSec"),
	pattern : false,
	attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggr : false
];
@Field serverIsrShrinksPerSec = [
	on : jmxHelper.objectName("kafka.server:type=ReplicaManager,name=IsrShrinksPerSec"),
	pattern : false,
	attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggr : false
];
@Field serverLeaderCount = [
	on : jmxHelper.objectName("kafka.server:type=ReplicaManager,name=LeaderCount"),
	pattern : false,
	attrs : ["Value"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggr : false
];
@Field serverMaxLag = [
	on : jmxHelper.objectName("kafka.server:type=ReplicaFetcherManager,name=MaxLag,clientId=*"),
	pattern : true,
	attrs : ["Value"] as String[],
	segs : ["name"],
	keys : ["clientId","type"],
	aggr : false
];
@Field serverMessagesInPerSec = [
	on : jmxHelper.objectName("kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec"),
	pattern : false,
	attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggr : false
];
@Field serverMinFetchRate = [
	on : jmxHelper.objectName("kafka.server:type=ReplicaFetcherManager,name=MinFetchRate,clientId=*"),
	pattern : true,
	attrs : ["Value"] as String[],
	segs : ["name"],
	keys : ["clientId","type"],
	aggr : false
];
@Field serverNumDelayedOperations = [
	on : jmxHelper.objectName("kafka.server:type=DelayedOperationPurgatory,name=NumDelayedOperations,delayedOperation=Rebalance"),
	pattern : false,
	attrs : ["Value"] as String[],
	segs : ["name"],
	keys : ["type","delayedOperation"],
	aggr : false
];
@Field serverPartitionCount = [
	on : jmxHelper.objectName("kafka.server:type=ReplicaManager,name=PartitionCount"),
	pattern : false,
	attrs : ["Value"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggr : false
];
@Field serverPurgatorySize = [
	on : jmxHelper.objectName("kafka.server:type=DelayedOperationPurgatory,name=PurgatorySize,delayedOperation=Rebalance"),
	pattern : false,
	attrs : ["Value"] as String[],
	segs : ["name"],
	keys : ["type","delayedOperation"],
	aggr : false
];
@Field serverRequestHandlerAvgIdlePercent = [
	on : jmxHelper.objectName("kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent"),
	pattern : false,
	attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggr : false
];
@Field serverRequestsPerSec = [
	on : jmxHelper.objectName("kafka.server:type=FetcherStats,name=RequestsPerSec,clientId=*,brokerHost=*,brokerPort=*"),
	pattern : true,
	attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
	segs : ["name"],
	keys : ["clientId","brokerHost","type","brokerPort"],
	aggr : false
];
@Field serverTotalFetchRequestsPerSec = [
	on : jmxHelper.objectName("kafka.server:topic=*,name=TotalFetchRequestsPerSec,type=BrokerTopicMetrics"),
	pattern : true,
	attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggrkeys : ["topic"],
	aggr : true
];
@Field serverTotalProduceRequestsPerSec = [
	on : jmxHelper.objectName("kafka.server:topic=*,name=TotalProduceRequestsPerSec,type=BrokerTopicMetrics"),
	pattern : true,
	attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggrkeys : ["topic"],
	aggr : true
];
@Field serverUnderReplicatedPartitions = [
	on : jmxHelper.objectName("kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions"),
	pattern : false,
	attrs : ["Value"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggr : false
];
@Field serverZooKeeperAuthFailuresPerSec = [
	on : jmxHelper.objectName("kafka.server:type=SessionExpireListener,name=ZooKeeperAuthFailuresPerSec"),
	pattern : false,
	attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggr : false
];
@Field serverZooKeeperDisconnectsPerSec = [
	on : jmxHelper.objectName("kafka.server:type=SessionExpireListener,name=ZooKeeperDisconnectsPerSec"),
	pattern : false,
	attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggr : false
];
@Field serverZooKeeperExpiresPerSec = [
	on : jmxHelper.objectName("kafka.server:type=SessionExpireListener,name=ZooKeeperExpiresPerSec"),
	pattern : false,
	attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggr : false
];
@Field serverZooKeeperReadOnlyConnectsPerSec = [
	on : jmxHelper.objectName("kafka.server:type=SessionExpireListener,name=ZooKeeperReadOnlyConnectsPerSec"),
	pattern : false,
	attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggr : false
];
@Field serverZooKeeperSaslAuthenticationsPerSec = [
	on : jmxHelper.objectName("kafka.server:type=SessionExpireListener,name=ZooKeeperSaslAuthenticationsPerSec"),
	pattern : false,
	attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggr : false
];
@Field serverZooKeeperSyncConnectsPerSec = [
	on : jmxHelper.objectName("kafka.server:type=SessionExpireListener,name=ZooKeeperSyncConnectsPerSec"),
	pattern : false,
	attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggr : false
];
@Field utilscleaner_io = [
	on : jmxHelper.objectName("kafka.utils:type=Throttler,name=cleaner-io"),
	pattern : false,
	attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
	segs : ["name"],
	keys : ["type"],
	aggr : false
];
@Field names = [serverUnderReplicatedPartitions,serverPartitionCount,serverIsrShrinksPerSec,networkIdlePercent,serverNumDelayedOperations,serverZooKeeperExpiresPerSec,logmax_clean_time_secs,serverRequestHandlerAvgIdlePercent,networkThrottleTimeMs,clusterUnderReplicated,coordinatorNumOffsets,serverBytesOutPerSec,serverPurgatorySize,serverFailedFetchRequestsPerSec,serverZooKeeperSyncConnectsPerSec,controllerLeaderElectionRateAndTimeMs,serverBrokerState,networkResponseQueueSize,logmax_dirty_percent,serverRequestsPerSec,networkLocalTimeMs,networkRequestQueueSize,serverZooKeeperDisconnectsPerSec,controllerActiveControllerCount,serverZooKeeperAuthFailuresPerSec,serverTotalFetchRequestsPerSec,serverZooKeeperReadOnlyConnectsPerSec,networkResponseSendTimeMs,networkRequestsPerSec,serverConsumerLag,logLogFlushRateAndTimeMs,networkResponseQueueTimeMs,serverMaxLag,controllerPreferredReplicaImbalanceCount,serverTotalProduceRequestsPerSec,logLogEndOffset,logSize,logLogStartOffset,serverBytesRejectedPerSec,networkRemoteTimeMs,coordinatorNumGroups,utilscleaner_io,logcleaner_recopy_percent,controllerUncleanLeaderElectionsPerSec,serverMinFetchRate,server,serverZooKeeperSaslAuthenticationsPerSec,serverMessagesInPerSec,serverBytesPerSec,serverFailedProduceRequestsPerSec,serverLeaderCount,serverBytesInPerSec,networkRequestQueueTimeMs,networkTotalTimeMs,networkNetworkProcessorAvgIdlePercent,serverExpiresPerSec,logmax_buffer_utilization_percent,controllerOfflinePartitionsCount,serverIsrExpandsPerSec,logNumLogSegments,];
@Field mapAttrs = [
	"MeanRate" : 			"ratemean",
	"OneMinuteRate" : 		"rate1m",
	"FiveMinuteRate" : 		"rate5m",
	"FifteenMinuteRate" : 	"rate15m",
	"50thPercentile" : 		"pct50",
	"75thPercentile" : 		"pct75",
	"95thPercentile" : 		"pct95",
	"98thPercentile" : 		"pct98",
	"99thPercentile" : 		"pct99",
	"999thPercentile" : 	"pct999"
]

remapAttr = {attr -> 
	String name = mapAttrs.get(attr);
	return name==null ? attr : name;
}

clean = {s ->
	return s.replace("-", "_");
}

if(jmxClient==null) jmxClient = JMXClient.newInstance(this, "service:jmx:rmi:///jndi/rmi://$navmap_1:$port/jmxrmi");
tracer.reset().tags([host : hostTag, app : appTag]);



trace = { f, on ->
	tracer {
		attrMap = jmxHelper.getAttributes(on, jmxClient, f.attrs);
		ts = System.currentTimeMillis();
		try {
			tracer.pushSeg(on.getDomain());
			f.segs.each() { seg ->
				tracer.pushSeg(on.getKeyProperty(seq));
			}
			f.keys.each() { k ->
				tracer.pushTag(k, on.getKeyProperty(k));
			}
	        attrMap.each() { k,v ->
	        	tracer.pushSeg(remapAttr(k)).trace(v, ts).popSeg();
	        }
		} catch (x) {
			log.error("Failed to collect on [{}]", on, x);					
		} finally {
			/*
			tracer.popSeg();
			f.segs.each() { seg ->
				tracer.popSeg();
			}
			keys.each() { k ->
				tracer.popTag();
			}
			*/
		}
	}
}

mtrace = { f, on ->
	tracer {
		attrMap = jmxHelper.getAttributes(on, jmxClient, f.attrs);
		ts = System.currentTimeMillis();
		try {
			tracer.pushSeg(on.getDomain());
			f.segs.each() { seg ->
				tracer.pushSeg(on.getKeyProperty(seq));
			}
			f.keys.each() { k ->
				tracer.pushTag(clean(k), on.getKeyProperty(k));
			}
			f.aggrkeys.each() {k ->
				tracer.pushTag(clean(k), on.getKeyProperty(k));
			}
	        attrMap.each() { k,v ->
	        	tracer.pushSeg(remapAttr(k)).trace(v, ts).popSeg();
	        }
		} catch (x) {
			log.error("Failed to collect on [{}]", on, x);					
		} finally {
			/*
			tracer.popSeg();
			f.segs.each() { seg ->
				tracer.popSeg();
			}
			keys.each() { k ->
				tracer.popTag();
			}
			*/
		}
	}
}

names.each() { f -> 
	ons = f.pattern ? jmxClient.queryNames(f.on, null).toArray(new ObjectName[0]) : [f.on] as ObjectName[];
	if(f.aggr) {
		ons.each() {
			mtrace(f, it);
		}

	} else {
		ons.each() {
			trace(f, it);
		}
	}
}


