import org.apache.kafka.clients.producer.*;
import com.heliosapm.streams.metrics.*;
import java.lang.management.*;

Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("acks", "all");
props.put("retries", 0);
props.put("batch.size", 16384);
props.put("linger.ms", 10);
props.put("buffer.memory", 33554432);
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "com.heliosapm.streams.metrics.StreamedMetricValueSerializer");
//props.put("compression.codec", "1");
//props.put("compression.topics", "tsdb.metrics.binary");
final Random R = new Random(System.currentTimeMillis());
 
 nextPosDouble = {
     return Math.abs(R.nextDouble());
 }
 
 randomInt = { max ->
     return Math.abs(R.nextInt(max+1));
 }
 
 
 host = ManagementFactory.getRuntimeMXBean().getName().split("@")[1].toLowerCase();
 app = "groovysender";
 metricNames = ["sys.cpu.total", "sys.fs.usage", "sys.mem.utilized", "jvm.gc.rate"] as String[];
 mnsize = metricNames.length;
 
 
 mod = 1000;
 producer = null;
 try {
     Producer<String, StreamedMetricValue> producer = new KafkaProducer<String, StreamedMetricValue>(props);
     futures = [];
     for(i in 1..10000) {
         long now = System.currentTimeMillis();
         sm = StreamedMetric.fromString("$now,${nextPosDouble()},${metricNames[i%mnsize]},webserver05,login-sso,host=webserver05,app=login-sso,colo=false,dc=us-west1");
         //new StreamedMetricValue(nextPosDouble(), "sys.cpu.total", StreamedMetric.tagsFromArray("host=$host", "app=$app"));
         //println sm;
         pr = new ProducerRecord<String, StreamedMetricValue>("tsdb.metrics.binary", sm.getMetricName(), sm);
         futures.add(producer.send(pr));
         if(i%mod==0) {
             long start = System.currentTimeMillis();
             totalSize = 0;
             parts = new HashSet();
             futures.each() { f ->
                 sent = f.get();
                 totalSize += sent.serializedKeySize();
                 totalSize += sent.serializedValueSize();
                 parts.add(sent.partition());
             }
             long sentElapsed = System.currentTimeMillis() - start;
             println "Sent $mod messages. Total Size: $totalSize, Per Message: ${totalSize/mod} Partitions: $parts";
             futures.clear();
         }
         
     }
 } finally {
     if(producer != null) try { producer.close(); println "Producer Closed"; } catch (x) {}
 }