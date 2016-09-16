
/*
	Kafka Broker JMX Collection Script
	Whitehead, 2016
*/

/* =========================================================================
	Populate one timers
   ========================================================================= */	
@Field
hostTag = navmap_1;
@Field
appTag = navmap_0;
@Field
controllerPattern = jmxHelper.objectName("kafka.controller:type=KafkaController,name=*");
@Field
leaderElectionOn = jmxHelper.objectName("kafka.controller:type=ControllerStats,name=LeaderElectionRateAndTimeMs");
@Field
leaderElectionAttrs = ["MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate","50thPercentile",
	"Min","Mean","StdDev","75thPercentile","95thPercentile","98thPercentile","99thPercentile","999thPercentile","Count","Max"] as String[];
@Field
uncleanLeaderElectionOn = jmxHelper.objectName("kafka.controller:type=ControllerStats,name=UncleanLeaderElectionsPerSec");
@Field
uncleanLeaderElectionAttrs = ["Count","MeanRate","OneMinuteRate","FiveMinuteRate","FifteenMinuteRate"] as String[];
@Field
groupMetaManagerPattern = jmxHelper.objectName("kafka.coordinator:type=GroupMetadataManager,name=*");
@Field
logCleanerPattern = jmxHelper.objectName("kafka.log:type=LogCleaner,name=*");

log.info("Collecting for ${appTag}@${hostTag}");
log.info("JMX: service:jmx:rmi:///jndi/rmi://$navmap_1:$port/jmxrmi");

jmxClient = JMXClient.newInstance(this, "service:jmx:rmi:///jndi/rmi://$navmap_1:$port/jmxrmi");

tracer.reset().tags([host : hostTag, app : appTag]);

traceStats = { on, attrs, seg ->
	tracer {
		try {
			tracer.pushSeg(on.getDomain()).pushSeg(seg);
			attributeValues = jmxHelper.getAttributes(on, jmxClient, attrs);
	        ts = System.currentTimeMillis();
	        attributeValues.each() { k,v -> 
	        	tracer.pushSeg(k).trace(v, ts).popSeg();
	        }        
		} catch (x) {
			log.error("Failed to collect on [{}]", on, x);					
		}				
	}	
}

traceValues = { pattern ->
	jmxClient.queryNames(pattern, null).each() { on ->
		tracer {
			tracer.pushSeg(on.getDomain());
			try {
				tracer.pushSeg(on.getDomain());
		        name = on.getKeyProperty("name");
		        int value = jmxClient.getAttribute(on, "Value");
		        ts = System.currentTimeMillis();
		        tracer.pushSeg(name).trace(value, ts).popSeg();
			} catch (x) {
				log.error("Failed to collect on [{}]", on, x);					
			} finally {
				tracer.popSeg();
			}
		}
	}
}	

traceValues(controllerPattern);
traceValues(groupMetaManagerPattern);
traceValues(logCleanerPattern);
traceStats(leaderElectionOn, leaderElectionAttrs, "leaderelection");
traceStats(uncleanLeaderElectionOn, uncleanLeaderElectionAttrs, "ucleaderelection");


