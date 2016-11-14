import groovy.transform.*;
import javax.management.*;
import javax.management.remote.*;
import com.heliosapm.utils.jmx.JMXHelper as jmxHelper;


class mxtracer {
    int count = 0;
    String app = "kafka-broker";
    String host = "PP-DT-NWHI-01";
    LinkedList seg = new LinkedList();
    TreeMap tags = new TreeMap<String, String>();
    Closure pushSeg = { String key ->
        seg.addLast(key);
    };
    Closure popSeg = {
        seg.removeLast();
    };
    Closure pushTag = { key, value -> 
        tags.put(key, value);
    };
    Closure clear = {
        seg.clear();
        tags.clear();
        count = 0;
    };
    Closure trace = { value, time ->
        def tmap = new TreeMap<String, String>();
        tmap.putAll(tags);
        tmap.put("app", app);
        tmap.put("host", host);
        //println "${seg.join('.')}${tmap} : $value";
        count++;
    };
}

@Field xtracer = new mxtracer();

@Field clusterUnderReplicated = [
    on : new ObjectName("kafka.cluster:topic=*,name=UnderReplicated,partition=*,type=Partition"),
    pattern : true,
    attrs : ["Value"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggrkeys : ["topic","partition"],
    aggr : true
];
@Field controllerActiveControllerCount = [
    on : new ObjectName("kafka.controller:type=KafkaController,name=ActiveControllerCount"),
    pattern : false,
    attrs : ["Value"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggr : false
];
@Field controllerLeaderElectionRateAndTimeMs = [
    on : new ObjectName("kafka.controller:type=ControllerStats,name=LeaderElectionRateAndTimeMs"),
    pattern : false,
    attrs : ["MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate","Max","Count","50thPercentile","StdDev","75thPercentile","95thPercentile","Min","Mean","98thPercentile","99thPercentile","999thPercentile"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggr : false
];
@Field controllerOfflinePartitionsCount = [
    on : new ObjectName("kafka.controller:type=KafkaController,name=OfflinePartitionsCount"),
    pattern : false,
    attrs : ["Value"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggr : false
];
@Field controllerPreferredReplicaImbalanceCount = [
    on : new ObjectName("kafka.controller:type=KafkaController,name=PreferredReplicaImbalanceCount"),
    pattern : false,
    attrs : ["Value"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggr : false
];
@Field controllerUncleanLeaderElectionsPerSec = [
    on : new ObjectName("kafka.controller:type=ControllerStats,name=UncleanLeaderElectionsPerSec"),
    pattern : false,
    attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggr : false
];
@Field coordinatorNumGroups = [
    on : new ObjectName("kafka.coordinator:type=GroupMetadataManager,name=NumGroups"),
    pattern : false,
    attrs : ["Value"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggr : false
];
@Field coordinatorNumOffsets = [
    on : new ObjectName("kafka.coordinator:type=GroupMetadataManager,name=NumOffsets"),
    pattern : false,
    attrs : ["Value"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggr : false
];
@Field logLogEndOffset = [
    on : new ObjectName("kafka.log:topic=*,name=LogEndOffset,partition=*,type=Log"),
    pattern : true,
    attrs : ["Value"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggrkeys : ["topic","partition"],
    aggr : true
];
@Field logLogFlushRateAndTimeMs = [
    on : new ObjectName("kafka.log:type=LogFlushStats,name=LogFlushRateAndTimeMs"),
    pattern : false,
    attrs : ["MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate","Max","Count","50thPercentile","StdDev","75thPercentile","95thPercentile","Min","Mean","98thPercentile","99thPercentile","999thPercentile"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggr : false
];
@Field logLogStartOffset = [
    on : new ObjectName("kafka.log:topic=*,name=LogStartOffset,partition=*,type=Log"),
    pattern : true,
    attrs : ["Value"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggrkeys : ["topic","partition"],
    aggr : true
];
@Field logNumLogSegments = [
    on : new ObjectName("kafka.log:topic=*,name=NumLogSegments,partition=*,type=Log"),
    pattern : true,
    attrs : ["Value"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggrkeys : ["topic","partition"],
    aggr : true
];
@Field logSize = [
    on : new ObjectName("kafka.log:topic=*,name=Size,partition=*,type=Log"),
    pattern : true,
    attrs : ["Value"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggrkeys : ["topic","partition"],
    aggr : true
];
@Field logcleaner_recopy_percent = [
    on : new ObjectName("kafka.log:type=LogCleaner,name=cleaner-recopy-percent"),
    pattern : false,
    attrs : ["Value"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggr : false
];
@Field logmax_buffer_utilization_percent = [
    on : new ObjectName("kafka.log:type=LogCleaner,name=max-buffer-utilization-percent"),
    pattern : false,
    attrs : ["Value"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggr : false
];
@Field logmax_clean_time_secs = [
    on : new ObjectName("kafka.log:type=LogCleaner,name=max-clean-time-secs"),
    pattern : false,
    attrs : ["Value"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggr : false
];
@Field logmax_dirty_percent = [
    on : new ObjectName("kafka.log:type=LogCleanerManager,name=max-dirty-percent"),
    pattern : false,
    attrs : ["Value"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggr : false
];
@Field networkIdlePercent = [
    on : new ObjectName("kafka.network:type=Processor,name=IdlePercent,networkProcessor=0"),
    pattern : false,
    attrs : ["Value"] as String[],
    segs : ["name"],
    keys : ["type","networkProcessor"],
    aggr : false
];
@Field networkLocalTimeMs = [
    on : new ObjectName("kafka.network:type=RequestMetrics,name=LocalTimeMs,request=Offsets"),
    pattern : false,
    attrs : ["Max","Count","50thPercentile","StdDev","75thPercentile","95thPercentile","Min","Mean","98thPercentile","99thPercentile","999thPercentile"] as String[],
    segs : ["name"],
    keys : ["type","request"],
    aggr : false
];
@Field networkNetworkProcessorAvgIdlePercent = [
    on : new ObjectName("kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent"),
    pattern : false,
    attrs : ["Value"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggr : false
];
@Field networkRemoteTimeMs = [
    on : new ObjectName("kafka.network:type=RequestMetrics,name=RemoteTimeMs,request=Produce"),
    pattern : false,
    attrs : ["Max","Count","50thPercentile","StdDev","75thPercentile","95thPercentile","Min","Mean","98thPercentile","99thPercentile","999thPercentile"] as String[],
    segs : ["name"],
    keys : ["type","request"],
    aggr : false
];
@Field networkRequestQueueSize = [
    on : new ObjectName("kafka.network:type=RequestChannel,name=RequestQueueSize"),
    pattern : false,
    attrs : ["Value"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggr : false
];
@Field networkRequestQueueTimeMs = [
    on : new ObjectName("kafka.network:type=RequestMetrics,name=RequestQueueTimeMs,request=LeaveGroup"),
    pattern : false,
    attrs : ["Max","Count","50thPercentile","StdDev","75thPercentile","95thPercentile","Min","Mean","98thPercentile","99thPercentile","999thPercentile"] as String[],
    segs : ["name"],
    keys : ["type","request"],
    aggr : false
];
@Field networkRequestsPerSec = [
    on : new ObjectName("kafka.network:type=RequestMetrics,name=RequestsPerSec,request=ApiVersions"),
    pattern : false,
    attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
    segs : ["name"],
    keys : ["type","request"],
    aggr : false
];
@Field networkResponseQueueSize = [
    on : new ObjectName("kafka.network:type=RequestChannel,name=ResponseQueueSize,processor=1"),
    pattern : false,
    attrs : ["Value"] as String[],
    segs : ["name"],
    keys : ["processor","type"],
    aggr : false
];
@Field networkResponseQueueTimeMs = [
    on : new ObjectName("kafka.network:type=RequestMetrics,name=ResponseQueueTimeMs,request=LeaderAndIsr"),
    pattern : false,
    attrs : ["Max","Count","50thPercentile","StdDev","75thPercentile","95thPercentile","Min","Mean","98thPercentile","99thPercentile","999thPercentile"] as String[],
    segs : ["name"],
    keys : ["type","request"],
    aggr : false
];
@Field networkResponseSendTimeMs = [
    on : new ObjectName("kafka.network:type=RequestMetrics,name=ResponseSendTimeMs,request=Produce"),
    pattern : false,
    attrs : ["Max","Count","50thPercentile","StdDev","75thPercentile","95thPercentile","Min","Mean","98thPercentile","99thPercentile","999thPercentile"] as String[],
    segs : ["name"],
    keys : ["type","request"],
    aggr : false
];
@Field networkThrottleTimeMs = [
    on : new ObjectName("kafka.network:type=RequestMetrics,name=ThrottleTimeMs,request=ApiVersions"),
    pattern : false,
    attrs : ["Max","Count","50thPercentile","StdDev","75thPercentile","95thPercentile","Min","Mean","98thPercentile","99thPercentile","999thPercentile"] as String[],
    segs : ["name"],
    keys : ["type","request"],
    aggr : false
];
@Field networkTotalTimeMs = [
    on : new ObjectName("kafka.network:type=RequestMetrics,name=TotalTimeMs,request=FetchConsumer"),
    pattern : false,
    attrs : ["Max","Count","50thPercentile","StdDev","75thPercentile","95thPercentile","Min","Mean","98thPercentile","99thPercentile","999thPercentile"] as String[],
    segs : ["name"],
    keys : ["type","request"],
    aggr : false
];
@Field server = [
    on : new ObjectName("kafka.server:type=Produce"),
    pattern : false,
    attrs : ["queue-size"] as String[],
    segs : [],
    keys : ["type"],
    aggr : false
];
@Field serverBrokerState = [
    on : new ObjectName("kafka.server:type=KafkaServer,name=BrokerState"),
    pattern : false,
    attrs : ["Value"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggr : false
];
@Field serverBytesInPerSec = [
    on : new ObjectName("kafka.server:topic=*,name=BytesInPerSec,type=BrokerTopicMetrics"),
    pattern : true,
    attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggrkeys : ["topic"],
    aggr : true
];
@Field serverBytesOutPerSec = [
    on : new ObjectName("kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec"),
    pattern : false,
    attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggr : false
];
@Field serverBytesPerSec = [
    on : new ObjectName("kafka.server:type=FetcherStats,name=BytesPerSec,clientId=*,brokerHost=*,brokerPort=*"),
    pattern : true,
    attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
    segs : ["name"],
    keys : ["clientId","brokerHost","type","brokerPort"],
    aggr : false
];
@Field serverBytesRejectedPerSec = [
    on : new ObjectName("kafka.server:topic=*,name=BytesRejectedPerSec,type=BrokerTopicMetrics"),
    pattern : true,
    attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggrkeys : ["topic"],
    aggr : true
];
@Field serverConsumerLag = [
    on : new ObjectName("kafka.server:clientId=*,topic=*,name=ConsumerLag,partition=*,type=FetcherLagMetrics"),
    pattern : true,
    attrs : ["Value"] as String[],
    segs : ["name"],
    keys : ["clientId","type"],
    aggrkeys : ["topic","partition"],
    aggr : true
];
@Field serverExpiresPerSec = [
    on : new ObjectName("kafka.server:type=DelayedFetchMetrics,name=ExpiresPerSec,fetcherType=consumer"),
    pattern : false,
    attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
    segs : ["name"],
    keys : ["type","fetcherType"],
    aggr : false
];
@Field serverFailedFetchRequestsPerSec = [
    on : new ObjectName("kafka.server:topic=*,name=FailedFetchRequestsPerSec,type=BrokerTopicMetrics"),
    pattern : true,
    attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggrkeys : ["topic"],
    aggr : true
];
@Field serverFailedProduceRequestsPerSec = [
    on : new ObjectName("kafka.server:topic=*,name=FailedProduceRequestsPerSec,type=BrokerTopicMetrics"),
    pattern : true,
    attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggrkeys : ["topic"],
    aggr : true
];
@Field serverIsrExpandsPerSec = [
    on : new ObjectName("kafka.server:type=ReplicaManager,name=IsrExpandsPerSec"),
    pattern : false,
    attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggr : false
];
@Field serverIsrShrinksPerSec = [
    on : new ObjectName("kafka.server:type=ReplicaManager,name=IsrShrinksPerSec"),
    pattern : false,
    attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggr : false
];
@Field serverLeaderCount = [
    on : new ObjectName("kafka.server:type=ReplicaManager,name=LeaderCount"),
    pattern : false,
    attrs : ["Value"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggr : false
];
@Field serverMaxLag = [
    on : new ObjectName("kafka.server:type=ReplicaFetcherManager,name=MaxLag,clientId=*"),
    pattern : true,
    attrs : ["Value"] as String[],
    segs : ["name"],
    keys : ["clientId","type"],
    aggr : false
];
@Field serverMessagesInPerSec = [
    on : new ObjectName("kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec"),
    pattern : false,
    attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggr : false
];
@Field serverMinFetchRate = [
    on : new ObjectName("kafka.server:type=ReplicaFetcherManager,name=MinFetchRate,clientId=*"),
    pattern : true,
    attrs : ["Value"] as String[],
    segs : ["name"],
    keys : ["clientId","type"],
    aggr : false
];
@Field serverNumDelayedOperations = [
    on : new ObjectName("kafka.server:type=DelayedOperationPurgatory,name=NumDelayedOperations,delayedOperation=Rebalance"),
    pattern : false,
    attrs : ["Value"] as String[],
    segs : ["name"],
    keys : ["type","delayedOperation"],
    aggr : false
];
@Field serverPartitionCount = [
    on : new ObjectName("kafka.server:type=ReplicaManager,name=PartitionCount"),
    pattern : false,
    attrs : ["Value"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggr : false
];
@Field serverPurgatorySize = [
    on : new ObjectName("kafka.server:type=DelayedOperationPurgatory,name=PurgatorySize,delayedOperation=Rebalance"),
    pattern : false,
    attrs : ["Value"] as String[],
    segs : ["name"],
    keys : ["type","delayedOperation"],
    aggr : false
];
@Field serverRequestHandlerAvgIdlePercent = [
    on : new ObjectName("kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent"),
    pattern : false,
    attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggr : false
];
@Field serverRequestsPerSec = [
    on : new ObjectName("kafka.server:type=FetcherStats,name=RequestsPerSec,clientId=*,brokerHost=*,brokerPort=*"),
    pattern : true,
    attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
    segs : ["name"],
    keys : ["clientId","brokerHost","type","brokerPort"],
    aggr : false
];
@Field serverTotalFetchRequestsPerSec = [
    on : new ObjectName("kafka.server:topic=*,name=TotalFetchRequestsPerSec,type=BrokerTopicMetrics"),
    pattern : true,
    attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggrkeys : ["topic"],
    aggr : true
];
@Field serverTotalProduceRequestsPerSec = [
    on : new ObjectName("kafka.server:topic=*,name=TotalProduceRequestsPerSec,type=BrokerTopicMetrics"),
    pattern : true,
    attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggrkeys : ["topic"],
    aggr : true
];
@Field serverUnderReplicatedPartitions = [
    on : new ObjectName("kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions"),
    pattern : false,
    attrs : ["Value"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggr : false
];
@Field serverZooKeeperAuthFailuresPerSec = [
    on : new ObjectName("kafka.server:type=SessionExpireListener,name=ZooKeeperAuthFailuresPerSec"),
    pattern : false,
    attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggr : false
];
@Field serverZooKeeperDisconnectsPerSec = [
    on : new ObjectName("kafka.server:type=SessionExpireListener,name=ZooKeeperDisconnectsPerSec"),
    pattern : false,
    attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggr : false
];
@Field serverZooKeeperExpiresPerSec = [
    on : new ObjectName("kafka.server:type=SessionExpireListener,name=ZooKeeperExpiresPerSec"),
    pattern : false,
    attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggr : false
];
@Field serverZooKeeperReadOnlyConnectsPerSec = [
    on : new ObjectName("kafka.server:type=SessionExpireListener,name=ZooKeeperReadOnlyConnectsPerSec"),
    pattern : false,
    attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggr : false
];
@Field serverZooKeeperSaslAuthenticationsPerSec = [
    on : new ObjectName("kafka.server:type=SessionExpireListener,name=ZooKeeperSaslAuthenticationsPerSec"),
    pattern : false,
    attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggr : false
];
@Field serverZooKeeperSyncConnectsPerSec = [
    on : new ObjectName("kafka.server:type=SessionExpireListener,name=ZooKeeperSyncConnectsPerSec"),
    pattern : false,
    attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggr : false
];
@Field utilscleaner_io = [
    on : new ObjectName("kafka.utils:type=Throttler,name=cleaner-io"),
    pattern : false,
    attrs : ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[],
    segs : ["name"],
    keys : ["type"],
    aggr : false
];
//@Field names = [serverUnderReplicatedPartitions,serverPartitionCount,serverIsrShrinksPerSec,networkIdlePercent,serverNumDelayedOperations,serverZooKeeperExpiresPerSec,logmax_clean_time_secs,serverRequestHandlerAvgIdlePercent,networkThrottleTimeMs,clusterUnderReplicated,coordinatorNumOffsets,serverBytesOutPerSec,serverPurgatorySize,serverFailedFetchRequestsPerSec,serverZooKeeperSyncConnectsPerSec,controllerLeaderElectionRateAndTimeMs,serverBrokerState,networkResponseQueueSize,logmax_dirty_percent,serverRequestsPerSec,networkLocalTimeMs,networkRequestQueueSize,serverZooKeeperDisconnectsPerSec,controllerActiveControllerCount,serverZooKeeperAuthFailuresPerSec,serverTotalFetchRequestsPerSec,serverZooKeeperReadOnlyConnectsPerSec,networkResponseSendTimeMs,networkRequestsPerSec,serverConsumerLag,logLogFlushRateAndTimeMs,networkResponseQueueTimeMs,serverMaxLag,controllerPreferredReplicaImbalanceCount,serverTotalProduceRequestsPerSec,logLogEndOffset,logSize,logLogStartOffset,serverBytesRejectedPerSec,networkRemoteTimeMs,coordinatorNumGroups,utilscleaner_io,logcleaner_recopy_percent,controllerUncleanLeaderElectionsPerSec,serverMinFetchRate,server,serverZooKeeperSaslAuthenticationsPerSec,serverMessagesInPerSec,serverBytesPerSec,serverFailedProduceRequestsPerSec,serverLeaderCount,serverBytesInPerSec,networkRequestQueueTimeMs,networkTotalTimeMs,networkNetworkProcessorAvgIdlePercent,serverExpiresPerSec,logmax_buffer_utilization_percent,controllerOfflinePartitionsCount,serverIsrExpandsPerSec,logNumLogSegments,];
//@Field names = [serverUnderReplicatedPartitions,serverPartitionCount,serverIsrShrinksPerSec];
@Field names = [serverBytesInPerSec];
@Field mapAttrs = [
    "MeanRate" :             "ratemean",
    "OneMinuteRate" :         "rate1m",
    "FiveMinuteRate" :         "rate5m",
    "FifteenMinuteRate" :     "rate15m",
    "50thPercentile" :         "pct50",
    "75thPercentile" :         "pct75",
    "95thPercentile" :         "pct95",
    "98thPercentile" :         "pct98",
    "99thPercentile" :         "pct99",
    "999thPercentile" :     "pct999"
]

remapAttr = {attr -> 
    String name = mapAttrs.get(attr);
    return name==null ? attr : name;
}

clean = {s ->
    return s.replace("-", "_");
}

    xtrace = { tracer, jmxClient, f, on ->        
        String[] attributeNames = f.attrs;
        
        
        println "init xtrace: $on --> $attributeNames";
        
        attributes = jmxClient.getAttributes(on, f.attrs);
        println "xtrace attributes: $attributes";
        attrMap = [:];
        attributes.asList().each() { a ->
            attrMap.put(a.getName(), a.getValue());
        }
        println "xtrace attrMap: $attrMap";
        ts = System.currentTimeMillis();
        try {
            tracer.pushSeg(on.getDomain());
            f.segs.each() { seg ->
                tracer.pushSeg(on.getKeyProperty(seg));
            }
            f.keys.each() { k ->
                tracer.pushTag(k, on.getKeyProperty(k));
            }
            attrMap.each() { k,v ->
                tracer.pushSeg(remapAttr(k));
                tracer.trace(v, ts);
                tracer.popSeg();
            }
        } catch (x) {        
            System.err.println("Failed to collect on [$on] : $x");                    
            x.printStackTrace(System.err);
        } finally {
            println "Traced ${tracer.count} metrics";
            tracer.clear();
        }
    }
    
    mtrace = { tracer, jmxClient,  f, on ->
        attrMap = jmxHelper.getAttributes(on, jmxClient, f.attrs);
        ts = System.currentTimeMillis();
        try {
            tracer.pushSeg(on.getDomain());
            f.segs.each() { seg ->
                tracer.pushSeg(on.getKeyProperty(seg));
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
            System.err.println("Failed to collect on [$on] : $x");                    
        } finally {
            println "Traced ${tracer.count} metrics";
            tracer.clear();
        }
    }
    


connector = null;

try {
    //surl = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:9082/jmxrmi");
    surl = new JMXServiceURL("service:jmx:jmxmp://localhost:8334");
    connector = JMXConnectorFactory.connect(surl);
    println "CONNECTOR: $connector";
    connector.connect();
    jmxClient = connector.getMBeanServerConnection();
    println "Connected to [${jmxClient.getAttribute(jmxHelper.MXBEAN_RUNTIME_ON, 'Name')}]";
    
    
    
    
    names.each() { f -> 
        println "Processing $f";
        ons = f.pattern ? jmxClient.queryNames(f.on, null).toArray(new ObjectName[0]) : [f.on] as ObjectName[];
        println ons;
        if(ons.length > 0) {
            if(f.aggr) {
                ons.each() {
                    println "mtrace";
                    mtrace(xtracer, jmxClient, f, it);
                }
        
            } else {
                ons.each() {
                    println "xtrace";
                    xtrace(xtracer, jmxClient, f, it);
                }
            }
        }
    }
} finally {
    if(connector!=null) try { connector.close(); println "Connector Closed"; } catch (x) {}
}

return null;