@Field
hostTag = navmap_1;
@Field
appTag = navmap_0;
@Field
jmxClient = null;
@Field mapAttrs = [
	"Count"	: 				"count",
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

//if(jmxClient==null) jmxClient = JMXClient.newInstance(this, "service:jmx:rmi:///jndi/rmi://$navmap_1:$port/jmxrmi");
jmxClient = JMXClient.newInstance(this, "service:jmx:jmxmp://$navmap_1:$port");

/*
if(jmxClient==null) {
	_jmxUrl_ = "service:jmx:jmxmp://$navmap_1:$port";
	jmxClient = JMXClient.newInstance(this, _jmxUrl_);
	log.info("\n\t================\n\tConnected to Kafka Broker at $_jmxUrl_\n\t================\n");

}
*/
tracer.reset().tags([host : hostTag, app : appTag]);

try {
	//kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic=tsdb.metrics.binary
	jmxClient.queryNames(jmxHelper.objectName("kafka.server:type=BrokerTopicMetrics,name=*,topic=*"), null).each() { on ->
		tracer {
			StringBuilder b = new StringBuilder("\nBrokerTopicMetrics:").append(on);
			
			tracer.pushSeg(on.getDomain()).pushSeg("topic").pushSeg(on.getKeyProperty('name'));
			tracer {
				attrMap = jmxHelper.getAttributes(on, jmxClient, "MeanRate", "OneMinuteRate", "FiveMinuteRate", "FifteenMinuteRate", "Count");
				//log.info("Seg: [{}]. Attrs for [{}] : [{}]", "${on.getDomain()}.topic.${on.getKeyProperty('name')}", on, attrMap);
				ts = System.currentTimeMillis();
				attrMap.each() { k, v ->
					tracer.pushSeg(remapAttr(k))
				   		.pushTag("topic", clean(on.getKeyProperty("topic")))
				   		.trace(v, ts)
						.popTag().popSeg();
					//log.info("Traced [{}]", "${remapAttr(k)}/${clean(on.getKeyProperty('topic'))}:$v");
				}
			}
			//log.info(b.toString());
			
		}
	}

} catch (e) {
	log.error("BrokerTopicMetrics Collection Failure", e);
} finally {
	tracer.reset().tags([host : hostTag, app : appTag]);

}


try {
	tracer.pushSeg("kafka.log.topic.size");
	def accumulator = [:];
	//kafka.log:type=Log,name=Size,topic=__consumer_offsets,partition=0
	jmxClient.queryNames(jmxHelper.objectName("kafka.log:type=Log,name=Size,topic=*,partition=*"), null).each() { on ->
		def topic = on.getKeyProperty('topic');
		def partition = on.getKeyProperty('partition'); 
		def size = jmxHelper.getAttribute(jmxClient, on, 'Value');
		if(accumulator.containsKey(topic)) {
			accumulator.put(topic, accumulator.get(topic) + size);
		} else {
			accumulator.put(topic, size);
		}
	}
	ts = System.currentTimeMillis();
	accumulator.each() { k, v ->
		tracer {
			tracer.pushTag("topic", clean(k))
				.trace(v, ts)
				.popTag();
		}
	}
	accumulator.clear();

} catch (e) {
	log.error("BrokerTopicLogSize Collection Failure", e);
} finally {
	tracer.reset().tags([host : hostTag, app : appTag]);
}



