
def jmxClient = JMXClient.newInstance(this, "service:jmx:jmxmp://heliosleopard:1421"); 


/* =========================================================================
	Populate one timers
   ========================================================================= */	

groupConsumerPattern = jmxHelper.objectName("com.heliosapm.streams.consumer:app=$app,host=$host,group=*");

topicPartConsumerPattern = jmxHelper.objectName("com.heliosapm.streams.consumer:app=$app,host=$host,group=*,partition=0,topic=*");
@Field
consumerAttributes = ["Msgs15mRate", "Msgs5mRate", "Msgs1mRate", "MsgsMeanRate", "MsgsRateCount"] as String[];
@Field
topicPartConsumerAttributes = ["BytesMedian","BytesMean","BytesPct95","BytesPct75","BytesPct98","BytesPct99","BytesMax","Msgs1mRate","BytesMin","TotalBytesCount","BytesHistCount","Msgs15mRate","MsgsMeanRate","Msgs5mRate","BytesStdDev","MsgsRateCount","BytesPct999"] as String[];
@Field 
deltaTraces = new HashSet(["MsgsRateCount"]);


tracer.reset().tags([host : host, app : app, service : "kafkaclient", type : "consumer"]);


tracer {
	jmxClient.queryNames(groupConsumerPattern, null).each() { on ->
		log.info("Tracing Consumer ObjectName [{}]", on);
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
		log.info("Tracing Partition Consumer ObjectName [{}]", on);
		tracer {
			try {
				tracer.pushTag("group", on.getKeyProperty("group"));
				tracer.pushTag("topic", on.getKeyProperty("topic"));
				tracer.pushTag("partition", on.getKeyProperty("partition"));
				ts = System.currentTimeMillis();					
				attributeValues = jmxHelper.getAttributes(on, jmxClient, topicPartConsumerAttributes);
				attributeValues.each() { k, v ->
					//log.info("Tracing $k : $v")
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
