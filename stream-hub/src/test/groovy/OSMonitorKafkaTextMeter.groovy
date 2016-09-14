import java.util.concurrent.atomic.*;
import java.util.concurrent.*;
import java.util.zip.*;
import org.apache.kafka.clients.producer.*;
import com.heliosapm.streams.metrics.*;
import java.lang.management.*;

//TO_TOPIC = "tsdb.metrics.accumulator"
TO_TOPIC = "tsdb.metrics.text.meter"

Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9093,localhost:9094");
//props.put("bootstrap.servers", "localhost:9092");
//props.put("bootstrap.servers", "10.22.114.37:9092");
//props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");

props.put("acks", "all");
props.put("retries", 0);
props.put("batch.size", 16384);
props.put("linger.ms", 10);
props.put("buffer.memory", 33554432);
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

//props.put("compression.codec", "1");
//props.put("compressed.topics", TO_TOPIC);


METRIC_TEMPLATES = [
    "%s,1,ptms.ibs, pdk-pt-cepas-01, ptms",    // UNIX TIME !!
    "%s,1,ptms.xyz, pdk-pt-cepas-01, ptms",
    "%s,1,ptms.abc, pdk-pt-cepas-01, ptms",
    "%s,1,ptms.foo, pdk-pt-cepas-01, ptms",
    "%s,1.0,ptms.bar, pdk-pt-cepas-01, ptms",
    "%s,1.0,ptms.snafu, pdk-pt-cepas-01, ptms",
    "%s,1.0,act.ibs, pdk-pt-cepas-01, ptms"
]

final Random R = new Random(System.currentTimeMillis());
loops = {
    return Math.abs(R.nextInt(20));
}
longValue = {
	return Math.abs(R.nextInt(100));
}
doubleValue = {
	return Math.abs(R.nextInt(100) + R.nextDouble());
}

TOPIC_KEYS = new HashSet();
unixTime = {
    return TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis())
}
long totalMessagesSent = 0;
Producer<String, String> producer = null;

traceNewTime = { template ->
    String topicKey = template.split(",")[1];
    if(TOPIC_KEYS.add(topicKey)) {
        println "New Topic Key: [$topicKey]";
    }
    def pr = new ProducerRecord<String, String>(TO_TOPIC, String.format(template, unixTime()));
    def f = producer.send(pr);
    //println f.get().dump();
}

try {
    producer = new KafkaProducer<String, String>(props);
/*
    METRIC_TEMPLATES.each() { temp ->
        traceNewTime(temp);
    }
    println "Sent!";
    System.exit(0);
*/

    modLoop = 0;
    total = 0;
    while(true) {
        loopCount = loops();
        for(i in 1..loopCount) {
            modLoop++;
            METRIC_TEMPLATES.each() { temp ->
                rloop = loops();
                for(x in 1..rloop) {
                    traceNewTime(temp);
                    total++;
                }
                producer.flush();
            }
        }
        totalMessagesSent += total;
        if(modLoop%100==0) {
            println "\tSent Metrics This Batch: $total, Total: $totalMessagesSent";
            total = 0;
        }
        Thread.sleep(100);
    }

} finally {
    if(producer!=null) {
        producer.close();
    }
}

