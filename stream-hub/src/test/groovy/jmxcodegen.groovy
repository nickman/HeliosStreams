import javax.management.*;
import javax.management.remote.*;

connection = null;
prims = ["byte", "short", "int", "long", "float", "double"];
nset = new HashSet(prims);
try {
    surl = new JMXServiceURL("service:jmx:jmxmp://localhost:1421");
    connection = JMXConnectorFactory.connect(surl, null);
    server = connection.getMBeanServerConnection();
    println "Connected";
    /*
    on = new ObjectName("com.heliosapm.streams.consumer:app=streamhub,host=heliosleopard,group=StreamHub,topic=tsdb.metrics.text.meter,partition=0");
    minfo = server.getMBeanInfo(on);
    minfo.getAttributes().each() { a ->
        print "\"${a.getName()}\",";
    }
    println "";
    on = new ObjectName("kafka.consumer:*");
    server.queryNames(on, null).each() { m ->
        minfo = server.getMBeanInfo(m);
        println m.getKeyPropertyList().keySet();
        print "\t";
        minfo.getAttributes().each() { a ->
            print "\"${a.getName()}\",";
        }
        println "\n=========";    
    }
    */
    on = new ObjectName("com.heliosapm.streams.buffers:service=BufferManagerStats,type=Direct");
    minfo = server.getMBeanInfo(on);
    minfo.getAttributes().each() { a ->
        if(nset.contains(a.getType())) {
            print "\"${a.getName()}\",";
        } else {
            try {
                ntype = Class.forName(a.getType());
                if(Number.isAssignableFrom(ntype)) {
                    print "\"${a.getName()}\",";
                }                
            } catch (x) {}
       }
        
    }
    println "";
    
} finally {
    try { connection.close(); println "Connector closed"; } catch (x) {}
}

return null;