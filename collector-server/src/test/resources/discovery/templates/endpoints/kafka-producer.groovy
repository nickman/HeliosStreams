/*
	Kafka Producer Collection Script
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
producerPattern = jmxHelper.objectName("com.heliosapm.streams.producer:app=$appTag,host=$hostTag");
@Field
topicProducerPattern = jmxHelper.objectName("com.heliosapm.streams.producer:app=$appTag,host=$hostTag,topic=*");
@Field
perTopicMetricsPattern = jmxHelper.objectName("kafka.producer:type=producer-topic-metrics,client-id=*,topic=*");
@Field
producerMetricsPattern = jmxHelper.objectName("kafka.producer:type=producer-metrics,client-id=*,*");

@Field
producerAttributes = ["BytesHistCount","BytesMax","BytesMean","BytesMedian","BytesMin","BytesPct75","BytesPct95","BytesPct98","BytesPct99","BytesPct999","BytesStdDev","Msgs15mRate","Msgs1mRate","Msgs5mRate","MsgsMeanRate","MsgsRateCount","TotalBytesCount"] as String[];
@Field 
deltaTraces = new HashSet(["MsgsRateCount","TotalBytesCount"]);
@Field
perTopicAttributes = ["byte-rate","compression-rate","record-error-rate","record-retry-rate","record-send-rate"] as String[];
@Field
producerMetricAttributes = ["batch-size-avg","batch-size-max","buffer-available-bytes","buffer-exhausted-rate","buffer-total-bytes","bufferpool-wait-ratio","compression-rate-avg","connection-close-rate","connection-count","connection-creation-rate","incoming-byte-rate","io-ratio","io-time-ns-avg","io-wait-ratio","io-wait-time-ns-avg","metadata-age","network-io-rate","outgoing-byte-rate","produce-throttle-time-avg","produce-throttle-time-max","record-error-rate","record-queue-time-avg","record-queue-time-max","record-retry-rate","record-send-rate","record-size-avg","record-size-max","records-per-request-avg","request-latency-avg","request-latency-max","request-rate","request-size-avg","request-size-max","requests-in-flight","response-rate","select-rate","waiting-threads"] as String[];


tracer.reset().tags([host : hostTag, app : appTag, service : "kafkaclient", type : "producer"]);

tracer {
	jmxClient.queryNames(producerMetricsPattern, null).each() { on ->
		tracer {
			try {
				clientId =  clientId = on.getKeyProperty("client-id").replaceAll("\\d", "").replace(".", "").replace("-", "").replace(hostTag, "");
				tracer.pushTag("clientId", clientId);
				ts = System.currentTimeMillis();					
				attributeValues = jmxHelper.getAttributes(on, jmxClient, producerMetricAttributes);
				attributeValues.each() { k, v ->
					tracer.pushSeg(k).trace(v, ts).popSeg();
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
	jmxClient.queryNames(perTopicMetricsPattern, null).each() { on ->
		tracer {
			try {
				tracer.pushTag("topic", on.getKeyProperty("topic"));
				ts = System.currentTimeMillis();					
				attributeValues = jmxHelper.getAttributes(on, jmxClient, perTopicAttributes);
				attributeValues.each() { k, v ->
					tracer.pushSeg(k).trace(v, ts).popSeg();
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
	jmxClient.queryNames(producerPattern, null).each() { on ->
		tracer {
			try {
				ts = System.currentTimeMillis();		
				attributeValues = jmxHelper.getAttributes(on, jmxClient, producerAttributes);
				attributeValues.each() { k, v ->
					if(deltaTraces.contains(k)) {
						tracer.pushSeg(k).dtrace(v, ts).popSeg();
					} else {
						tracer.pushSeg(k).trace(v, ts).popSeg();
					}
				}
			} catch (x) {
				log.error("Failed to collect on [{}]", on, x);					
			}
		}
	}
}
tracer {
	jmxClient.queryNames(topicProducerPattern, null).each() { on ->
		tracer {
			try {
				tracer.pushTag("topic", on.getKeyProperty("topic"));
				ts = System.currentTimeMillis();					
				attributeValues = jmxHelper.getAttributes(on, jmxClient, producerAttributes);
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

tracer.flush();
log.info("Completed Collection");

