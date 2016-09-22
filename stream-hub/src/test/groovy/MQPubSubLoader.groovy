import com.ibm.mq.jms.*;
import com.ibm.msg.client.wmq.common.CommonConstants;
import java.util.regex.*;
import java.text.SimpleDateFormat;
import javax.jms.*;
import groovy.transform.*;
import java.util.concurrent.atomic.*;



MQConnectionFactory cf = null;
MQConnection conn = null;
MQConnection ponn = null;
Session csession = null;
Session psession = null;
MQTopic topic = new MQTopic("TOPIC.CLEARING.EVENT");
MessageConsumer consumer = null;
//TopicSubscriber consumer = null;
MessageProducer producer = null;

class MyListener implements MessageListener {
    final AtomicLong counter = new AtomicLong();
    public void onMessage(Message msg) {
        long count = counter.incrementAndGet();
        if(count%100==0)  println "Messages Received: $count";
    }
}

listener = new MyListener();

try {
    cf = new MQConnectionFactory();
    cf.setChannel("JBOSS.SVRCONN");
    cf.setHostName("127.0.0.1");
    cf.setPort(1430);
    cf.setTransportType(CommonConstants.WMQ_CM_CLIENT);
    conn = cf.createConnection();
    conn.start();
    ponn = cf.createConnection();
    //conn.setClientID("GroovyClient");    
    println "Connected: ${conn.getMetaData()}";
    csession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
    println "C Session Created";    
    psession = ponn.createSession(true, Session.AUTO_ACKNOWLEDGE);
    println "P Session Created";    
    //consumer = csession.createDurableSubscriber(topic, "Ye-Olde-Clearing-Bus");
    consumer = csession.createConsumer(topic, null, false);
    consumer.setMessageListener(listener);
    println "Consumer Created";
    producer = psession.createProducer(topic);
    println "Producer Created";
    
    ponn.start();
    println "Connection Started";
    boolean interrupted = false;
    try {
        for(i in 1..100000) {
            if(!interrupted) {
                msg = psession.createTextMessage("Hello World: $i");
                producer.send(msg);
                if(i%100==0) {
                    println "Sent Msg #$i";
                    psession.commit();
                }
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException iex) {
                    println "Stopping....";
                    interrupted = true;
                    throw new Throwable();
                }
            }
       }
   } catch (Exception ex) {
        if(interrupted) {
            println "-----> We're out";
        } else {
            println "Uh oh....";
            ex.printStackTrace(System.err);
        }
   }
} catch (e) {
    e.printStackTrace(System.err);
} finally {
    println "Cleaning Up....";
    if(conn!=null) {
        
        try { producer.close(); println "Producer Closed";} catch (x) {}
        try { consumer.close(); println "Consumer Closed";} catch (x) {}
        try { csession.close(); println "Consumer Session Closed";} catch (x) {}
        try { psession.close(); println "Producer Session Closed";} catch (x) {}        
        try { ponn.stop(); println "Producer Connection Stopped";} catch (x) {}
        try { ponn.close(); println "Producer Connection Closed";} catch (x) {}

        try { conn.stop(); println "Consumer Connection Stopped";} catch (x) {}
        try { conn.close(); println "Consumer Connection Closed";} catch (x) {}

    }
    println "Done";    
}

return null;