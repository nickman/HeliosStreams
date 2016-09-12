import java.util.concurrent.atomic.*;
import java.util.concurrent.*;
import java.util.zip.*;
import org.apache.kafka.clients.producer.*;
import com.heliosapm.streams.metrics.*;
import java.lang.management.*;

//TO_TOPIC = "tsdb.metrics.accumulator"
TO_TOPIC = "tsdb.metrics.text.meter"
scheduler = Executors.newScheduledThreadPool(4);
Properties props = new Properties();
//props.put("bootstrap.servers", "localhost:9093,localhost:9094");
props.put("bootstrap.servers", "localhost:9092");
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
def R = new Random(System.currentTimeMillis());
unixTime = {
    return TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis())
}
currentSecWindow = { size ->
    def ut = unixTime();
    return ut - (ut%size); 
}

nextSecWindow = { size ->
    return currentSecWindow(size) + size;
}

nextWindowMs = { size ->
    return TimeUnit.SECONDS.toMillis(nextSecWindow(size));
}



METRIC_TEMPLATES = [
    "%s,%s,ptms.ibs, pdk-pt-cepas-01, ptms, type=A" : 30    // UNIX TIME !!
    ,"%s,%s,ptms.ibs, pdk-pt-cepas-01, ptms, type=B" : 30
    ,"%s,%s,ptms.ibs, pdk-pt-cepas-01, ptms, type=C" : 30
    ,"%s,%s,ptms.ibs, pdk-pt-cepas-01, ptms, type=XYZ" : 30
    ,"%s,%s,ptms.ibs, pdk-pt-cepas-01, ptms, type=BAR" : 30
    ,"%s,%s,ptms.ibs, pdk-pt-cepas-01, ptms, type=FOO" : 30
    ,"%s,%s,ptms.ibs, pdk-pt-cepas-01, ptms, type=SNAFU" : 30


    // ,"%s,%s,ptms.xyz, pdk-pt-cepas-01, ptms" : 1
    // ,"%s,%s,ptms.abc, pdk-pt-cepas-01, ptms" : 1
    // ,"%s,%s,ptms.foo, pdk-pt-cepas-01, ptms" : 1
    // ,"%s,%s,ptms.bar, pdk-pt-cepas-01, ptms" : 1
    // ,"%s,%s,ptms.snafu, pdk-pt-cepas-01, ptms" : 1
    // ,"%s,%s,act.ibs, pdk-pt-cepas-01, ptms" : 1
]




longValue = { max ->
	return Math.abs(R.nextInt(max));
}
doubleValue = { max ->
	return Math.abs(R.nextInt(max) + R.nextDouble());
}

Producer<String, String> producer = null;

traceNewTime = { count, template, value ->
    def mn = template.split(",")[2];
    def futures = [];
    for(i in 1..count) {
        def pr = new ProducerRecord<String, String>(TO_TOPIC, String.format(template, unixTime(), value));
        futures.add(producer.send(pr));    
    }
    producer.flush();
    Thread.start({
        int cnt = 0;
        futures.each() { f ->
            f.get();
            cnt++;
        }
        Date dt = new Date();
        println "[$dt]: Submitted $cnt messages for [$mn],  Adj: ${cnt/5}";
    });
}


try {
    producer = new KafkaProducer<String, String>(props);    
    long nextTime = 0L;
    for(i in 1..100) {
        if(nextTime==0L) nextTime = nextWindowMs(5) - System.currentTimeMillis();
        else nextTime += 5000;
        int loo = longValue(10)*5;
        final int index = i;
        r = [
            run : {
                METRIC_TEMPLATES.each() { k,v ->
                    traceNewTime(longValue(100)*5, k, 1L);
                }                
                if(index==100) {
                    println "\n\tTEST COMPLETE";
                }
            }
        ] as Runnable;         
        scheduler.schedule(r, nextTime, TimeUnit.MILLISECONDS);
    }
    println "All tasks scheduled";
    try {
        Thread.currentThread().join();
    } catch (iex) {
        println "Stopping Sender";
    }
} finally {
    if(producer!=null) {
        producer.close();
    }
}

