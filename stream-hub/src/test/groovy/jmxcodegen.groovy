import javax.management.*;
import javax.management.remote.*;

connection = null;
prims = ["byte", "short", "int", "long", "float", "double"];
nset = new HashSet(prims);
aggregates = new HashSet(["topic","partition"]);

numericAttrs = { server, on ->
    StringBuilder b = new StringBuilder("[");
    minfo = server.getMBeanInfo(on);
    minfo.getAttributes().each() { a ->
        if(nset.contains(a.getType())) {
            b.append("\"${a.getName()}\",");
        } else {
            try {
                ntype = Class.forName(a.getType());
                if(Number.isAssignableFrom(ntype)) {
                    b.append("\"${a.getName()}\",");
                } else {
                    obj = server.getAttribute(on, a.getName());
                    //println "----- ${a.getName()} : $obj, type: ${obj.getClass().getName()}";
                    if(Number.isInstance(obj)) {
                        b.append("\"${a.getName()}\",");
                    }                
                }               
            } catch (x) {
                obj = server.getAttribute(on, a.getName());
                //println "XXXXX ${a.getName()} : $obj, type: ${obj.getClass().getName()}";
                if(Number.isInstance(obj)) {
                    b.append("\"${a.getName()}\",");
                }                
                
            }
       }
    }
    if(b.length() > 2) {
        b.deleteCharAt(b.length()-1);
    }
    return b.append("]").toString();
}

hasAggr = { on ->
    keys = on.getKeyPropertyList();
    int m = 0;
    aggregates.each() {
        if(keys.containsKey(it)) m++;
    }
    return m > 0;
}

try {
    surl = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:19204/jmxrmi");
    connection = JMXConnectorFactory.connect(surl, null);
    server = connection.getMBeanServerConnection();
    //println "Connected";
    set = new HashSet();
    on = new ObjectName("kafka.*:*");
    server.queryNames(on, null).each() {
        minfo = server.getMBeanInfo(it);
        set.add(minfo.getClassName());
    }
    //set.each() { println it; }
    

    int cnt = 0;
    names = new HashSet();
    aggrKeys = new HashSet();
    map = new TreeMap();
    server.queryNames(on, null).each() {
        boolean pattern = hasAggr(it);
        String domain = it.getDomain();
        boolean hasName = it.getKeyPropertyList().containsKey("name");
        StringBuilder b = new StringBuilder();
        cnt++;
        
        fname = null;
        if(hasName) {
            fname = "${domain.replace('kafka.', '')}${it.getKeyProperty('name').replace('-', '_')}";
        } else {
            fname = "${domain.replace('kafka.', '')}";
        }
        names.add(fname);
        b.append("@Field $fname = [\n");
        if(pattern) {
            kvs = it.getKeyPropertyList();
            aggregates.each() { a ->
                if(kvs.containsKey(a)) {
                    kvs.put(a, "*");
                }
            }
            on = new ObjectName(domain, kvs);
            b.append("\ton : jmxHelper.objectName(\"$on\"),\n");
        } else {
            b.append("\ton : jmxHelper.objectName(\"$it\"),\n");
        }
        
        b.append("\tpattern : ${hasAggr(it)},\n"); 
        b.append("\tattrs : ${numericAttrs(server, it)} as String[],\n");
        if(hasName) {
            b.append("\tsegs : [\"name\"],\n");
        } else {
            b.append("\tsegs : [],\n");
        }
        b.append("\tkeys : [");
        int ks = 0;
        it.getKeyPropertyList().keySet().each() { k ->
            if(!k.equals("name") && !aggregates.contains(k)) {
                b.append("\"$k\",");
                ks++;
            }
        }
        if(ks>0) b.deleteCharAt(b.length()-1);
        b.append("],\n");        
        
        ks = 0;
        kb = new StringBuilder();
        kb.append("\taggrkeys : [");
        it.getKeyPropertyList().keySet().each() { k ->
            if(aggregates.contains(k)) {
                kb.append("\"$k\",");
                aggrKeys.add("\"$k\"");
                ks++;
            }
        }
        if(ks>0) {
            kb.deleteCharAt(kb.length()-1);
            kb.append("],\n");
            b.append(kb);
        }
        if(ks>0) {
            b.append("\taggr : true\n");
        } else {
            b.append("\taggr : false\n");
        }
        b.append("];");
        map.put(fname, b.toString());
    }
    map.values().each() {
        println it;
    }
    StringBuilder b = new StringBuilder("@Field names = [");

    names.each() {
        b.append("$it,");
    }
    b.append("];");
    println b;    
} finally {
    try { connection.close();} catch (x) {}
}

return null;