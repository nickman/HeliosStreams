import com.ibm.mq.constants.MQConstants;
import static com.ibm.mq.constants.MQConstants.*;
import com.ibm.mq.pcf.*;
import static com.ibm.mq.pcf.CMQC.*;
import java.util.regex.*;
import java.text.SimpleDateFormat;

def SKIP_QUEUE = Pattern.compile("SYSTEM\\..*||AMQ\\..*");
def tnames = null;

listToMap = { list ->
    Map map = new HashMap();
    list.each() {
        map.putAll(it);
    }
    return map;
}


request = { byName, agent, type, parameters ->
    def responses = [];
    def PCFMessage request = new PCFMessage(type);
    if(parameters.getClass().isArray()) {
        parameters.each() { param ->
            request.addParameter(param);
        }
    } else {
        parameters.each() { name, value ->
            request.addParameter(name, value);
        }
    }

    agent.send(request).each() {
        def responseValues = [:];
        it.getParameters().toList().each() { pcfParam ->
            def value = pcfParam.getValue();
            //if(value instanceof String) value = value.trim();
            responseValues.put(byName ? pcfParam.getParameterName() : pcfParam.getParameter(), value);
        }
        responses.add(responseValues);
    }        
    return responses;
}


topicNames = { agent ->
    Map<String, Map> xtnames = new HashMap<String, Map>();
    request(true, agent, CMQCFC.MQCMD_INQUIRE_TOPIC_NAMES, [(CMQC.MQCA_TOPIC_NAME):"*"]).each() {
        it.get("MQCACF_TOPIC_NAMES").each() { tname ->
            if(!SKIP_QUEUE.matcher(tname).matches()) {                        
                def topicAttrs = listToMap(request(true, agent, CMQCFC.MQCMD_INQUIRE_TOPIC, [(CMQC.MQCA_TOPIC_NAME):tname]));
                xtnames.put(tname, topicAttrs)
            }
        }
    }
    tnames = xtnames;
    return tnames;
}



pcf = null;
qmgr = null;
try {
    pcf = new PCFMessageAgent("localhost", 1430, "JBOSS.SVRCONN");
    qmgr = pcf.getQManagerName();
    println "Connected to $qmgr";
    tnames = topicNames(pcf);
    println tnames.keySet();
} finally {
    if(pcf!=null) try { pcf.disconnect(); println "PCF Disconnected"; } catch (x) {}
}

println CMQCFC.MQBACF_CONNECTION_ID;
println MQConstants.MQIACF_CONN_INFO_ALL;
println MQConstants.MQIACF_CONN_INFO_HANDLE;
println MQConstants.MQIACF_CONN_INFO_CONN;