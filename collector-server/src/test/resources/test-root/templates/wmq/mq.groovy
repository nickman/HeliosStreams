import groovy.transform.*;
import com.ibm.mq.constants.MQConstants;
import static com.ibm.mq.constants.MQConstants.*;
import com.ibm.mq.pcf.*;
import static com.ibm.mq.pcf.CMQC.*;
import java.util.regex.*;
import java.text.SimpleDateFormat;

//==================================================================================
//      Constants
//==================================================================================
@Field
def int[] QUEUE_STATUS_ATTRS = [
    CMQC.MQCA_Q_NAME, CMQC.MQIA_CURRENT_Q_DEPTH,  
    CMQC.MQIA_OPEN_INPUT_COUNT, CMQC.MQIA_OPEN_OUTPUT_COUNT, 
    CMQCFC.MQIACF_UNCOMMITTED_MSGS,  CMQCFC.MQIACF_OLDEST_MSG_AGE
];
@Field
def CHANNEL_TYPES = [1:"Sender", 2:"Server", 3:"Receiver", 4:"Requester", 6:"Client Connection", 7:"Server Connection", 8:"Cluster Receiver", 9:"Cluster Sender"];
@Field
def CHANNEL_STATUSES = [0:"Inactive", 1:"Binding", 2:"Starting", 3:"Running", 4:"Stopping", 5:"Retrying", 6:"Stopped", 7:"Requesting", 8:"Paused", 13:"Initializing"];
@Field
def SERVICE_STATUSES = [0:"Stopped", 1:"Starting", 2:"Running", 3:"Stopping", 4:"Retrying"];
@Field
def MQ_STATUSES = [1:"Starting", 2:"Running", 3:"Quiescing"];
@Field
def MQ_DURABLE_SUB = [1:true, 2:false];
@Field
def APPL_TYPES = [
    (-1):"UNKNOWN",0:"NO_CONTEXT",1:"CICS",2:"MVS",2:"OS390",
    2:"ZOS",3:"IMS",4:"OS2",5:"DOS",6:"AIX",
    6:"UNIX",7:"QMGR",8:"OS400",9:"WINDOWS",10:"CICS_VSE",
    11:"WINDOWS_NT",12:"VMS",13:"GUARDIAN",13:"NSK",14:"VOS",
    15:"OPEN_TP1",18:"VM",19:"IMS_BRIDGE",20:"XCF",21:"CICS_BRIDGE",
    22:"NOTES_AGENT",23:"TPF",25:"USER",26:"BROKER",26:"QMGR_PUBLISH",
    28:"JAVA",29:"DQM",30:"CHANNEL_INITIATOR",31:"WLM",32:"BATCH",
    33:"RRS_BATCH",34:"SIB",35:"SYSTEM_EXTENSION",35:"SYSTEM",28:"DEFAULT",
    65536:"USER_FIRST",999999999:"USER_LAST"
];
@Field
def ASYNC_STATES = [
    0:"NONE",1:"STARTED",2:"START_WAIT",3:"STOPPED",4:"SUSPENDED",
    5:"SUSPENDED_TEMPORARY",6:"ACTIVE",7:"INACTIVE"
];
@Field
def CONN_INFO_TYPES = [
    1110:"TYPE",1111:"CONN",1112:"HANDLE",1113:"ALL"
];
@Field
def OBJECT_TYPE = [
    0:"NONE",1:"Q",10:"CF_STRUC",1001:"ALL",1002:"ALIAS_Q",
    1003:"MODEL_Q",1004:"LOCAL_Q",1005:"REMOTE_Q",1007:"SENDER_CHANNEL",1008:"SERVER_CHANNEL",
    1009:"REQUESTER_CHANNEL",1010:"RECEIVER_CHANNEL",1011:"CURRENT_CHANNEL",1012:"SAVED_CHANNEL",1013:"SVRCONN_CHANNEL",
    1014:"CLNTCONN_CHANNEL",1015:"SHORT_CHANNEL",11:"LISTENER",12:"SERVICE",2:"NAMELIST",
    3:"PROCESS",4:"STORAGE_CLASS",5:"Q_MGR",6:"CHANNEL",7:"AUTH_INFO",
    8:"TOPIC",999:"RESERVED_1",
];


@Field
def Q_TYPES = [(CMQC.MQQT_ALIAS) : "Alias", (CMQC.MQQT_CLUSTER) : "Cluster", (CMQC.MQQT_LOCAL) : "Local", (CMQC.MQQT_REMOTE) : "Remote", (CMQC.MQQT_MODEL) : "Model"];

@Field
def subPrefix = null;



@Field
def SKIP_QUEUE = Pattern.compile("SYSTEM\\..*||AMQ\\..*");
@Field
def SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");





    //==================================================================================
    //          Utility Methods
    //==================================================================================
    public int perc(part, total) {
        if(part<1 || total <1) return 0;
        return part/total*100;
    }
    public String channelType(int id){
        return CHANNEL_TYPES.get(id);
    }
    public String channelStatus(int id){
        return CHANNEL_STATUSES.get(id);
    }
    public String serviceStatus(int id){
        return SERVICE_STATUSES.get(id);
    }
    public String mqStatus(int id){
        return MQ_STATUSES.get(id);
    }
    xtractTimes = { map ->
        def tkeys = new HashSet();
        def dkeys = new HashSet();
        map.each() { k, v ->
            if(k!=null && k.endsWith("_TIME")) tkeys.add(k.replace("_TIME", ""));
            if(k!=null && k.endsWith("_DATE")) dkeys.add(k.replace("_DATE", ""));
        }
        tkeys.retainAll(dkeys);
        dkeys.retainAll(tkeys);
        def akeys = new HashSet(tkeys);
        akeys.addAll(dkeys);
        akeys.each() { 
            d = "${it}_DATE".toString(); t = "${it}_TIME".toString();
            dt = SDF.parse("${map.get(d).trim()} ${map.get(t).trim()}");
            map.put("${it}_TS", dt);
        }
        return map;
    }


    public List request(byName, agent, type, parameters) {
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

    public Map listToMap(list) {
        Map map = new HashMap();
        list.each() {
            map.putAll(it);
        }
        return map;
    }

    public Date formatDate(datePart, timePart) {
        //"yyyy-MM-dd HH.mm.ss"
        return SDF.parse("${datePart.toString().trim()} ${timePart.toString().trim()}");
    }

    public int[] getTopicPubSubCounts(topicString, agent) {
        int[] pubSubCounts = new int[2];
        request(false, agent, CMQCFC.MQCMD_INQUIRE_TOPIC_STATUS, [(CMQC.MQCA_TOPIC_STRING):topicString]).each() {
            pubSubCounts[0] = it.get(CMQC.MQIA_PUB_COUNT);
            pubSubCounts[1] = it.get(CMQC.MQIA_SUB_COUNT);
        }
        return pubSubCounts;
    }


    /**
     * Returns the cached meta data for the primary (non-admin) queues
     */
    public Map qNames(agent) {
        remove("QUEUES");
        return get("QUEUES", (60000 * 15), {
            log.info "Fetching Queue Meta";
            Map<String, Map> qnames = new HashMap<String, Map>();
            request(true, agent, CMQCFC.MQCMD_INQUIRE_Q_NAMES, [(CMQC.MQCA_Q_NAME):"*", (CMQC.MQIA_Q_TYPE) : CMQC.MQQT_LOCAL]).each() {
                it.get("MQCACF_Q_NAMES").each() { qname ->
                    if(!SKIP_QUEUE.matcher(qname).matches()) {
                        def qAttrs = listToMap(request(true, agent, CMQCFC.MQCMD_INQUIRE_Q, [(CMQC.MQCA_Q_NAME):qname]));
                        qnames.put(qname, qAttrs);
                    }
                }
            }
            return qnames;
        });
    }

    /**
     * Returns the cached meta data for the primary (non-admin) topics
     */
    public Map topicNames(agent) {
        return get("TOPICS", (60000 * 15), {
            log.info "Fetching Topic Names";
            Map<String, Map> tnames = new HashMap<String, Map>();
            request(true, agent, CMQCFC.MQCMD_INQUIRE_TOPIC_NAMES, [(CMQC.MQCA_TOPIC_NAME):"*"]).each() {
                it.get("MQCACF_TOPIC_NAMES").each() { tname ->
                    if(!SKIP_QUEUE.matcher(tname).matches()) {                        
                        def topicAttrs = listToMap(request(true, agent, CMQCFC.MQCMD_INQUIRE_TOPIC, [(CMQC.MQCA_TOPIC_NAME):tname]));
                        tnames.put(tname, topicAttrs)
                    }
                }
            }
            return tnames;
        });
    }

    /**
     * Returns the cached meta data for the channels
     */
    public Map channelNames(agent) {
        remove("CHANNELS");
        return get("CHANNELS", (60000 * 15), {
            log.info "Fetching Channel Names";
            Map<String, Map> chnames = new HashMap<String, Map>();
            request(true, agent, CMQCFC.MQCMD_INQUIRE_CHANNEL_NAMES, [(MQConstants.MQCACH_CHANNEL_NAME):"*"]).each() {
                it.get("MQCACH_CHANNEL_NAMES").each() { chname ->
                    def map = listToMap(request(true, agent, CMQCFC.MQCMD_INQUIRE_CHANNEL, [(MQConstants.MQCACH_CHANNEL_NAME):chname]));
                    chnames.put(chname, map);
                }
            }
            return chnames;
        });
    }

    public Map getSubName(agent, subId) {
        rez = request(true, agent, CMQCFC.MQCMD_INQUIRE_SUBSCRIPTION, [(CMQCFC.MQBACF_SUB_ID):subId]).get(0);
        if(rez == null || rez.isEmpty()) return null;
        //printMap("ANON SUB", rez);
        subName = rez.get("MQCACF_SUB_NAME");
        if(subName==null || subName.trim().isEmpty()) {
            rez.put("MQCACF_SUB_NAME", "ANONYMOUS");
        } else {
            rez.put(subName.replace(subPrefix, "").replace(":", "_"));
        }        
        return rez;
    }

    /**
     * Returns the cached meta data for the subscriptions
     */
    public Map subNames(agent) { 
        remove("SUBSCRIPTIONS")       ;
        return get("SUBSCRIPTIONS", (60000 * 15), {
            log.info "Fetching Subscriptions";            
            
            Map<String, Map> subs = new HashMap<String, Map>();  
            Map<String, Map> tnames = topicNames(agent);
            tnames.each() { topic, meta ->
                String topicString = meta.get('MQCA_TOPIC_STRING');
                int[] counts = getTopicPubSubCounts(topicString, agent);
                log.info "PubSub Counts for [${topicString}] : $counts"
                if(counts[1] > 0) {
                    Map topicSubs  = [:];
                    subs.put(topic.trim(), topicSubs);
                    try {
                        request(false, agent, CMQCFC.MQCMD_INQUIRE_TOPIC_STATUS, [(CMQC.MQCA_TOPIC_STRING):topicString, (CMQCFC.MQIACF_TOPIC_STATUS_TYPE):CMQCFC.MQIACF_TOPIC_SUB]).each() { sub ->
                            byte[] subId = sub.get(CMQCFC.MQBACF_SUB_ID);                            
                            String subName = null;
                            String destName = null;
                            request(true, agent, CMQCFC.MQCMD_INQUIRE_SUBSCRIPTION, [(CMQCFC.MQBACF_SUB_ID):subId]).each() {
                                subName = it.get("MQCACF_SUB_NAME");
                                topicSubs.put(subName, it);  
                                strSubId = "$subId".toString();
                                cleanSubName = subName.replace(subPrefix, "").replace(":", "_");
                                topicSubs.put(strSubId, cleanSubName);  
                                topicSubs.put(cleanSubName, it);                                

                            }
                        }
                    } catch (ex) {
                        log.info "PubSub Count ERR [${topicString}] : $ex"
                        ex.printStackTrace(System.err);

                    } finally {
                        if(topicSubs.isEmpty()) {
                            subs.remove(topic.trim());
                        }
                    }
                }                
            }
            return subs;
        });
    }

    public void traceQueueStats(agent, queueName) {
        try {
            trace { tracer ->
                ts = System.currentTimeMillis();
                tracer.pushSeg("queue");       
                q = queueName.trim();
                it = request(false, agent, CMQCFC.MQCMD_INQUIRE_Q_STATUS, [(CMQC.MQCA_Q_NAME):queueName]).get(0);
                qType = Q_TYPES.get(it.get(CMQC.MQCA_Q_NAME).trim());
                try {
                    tracer.pushTag("q=$q");
                    tracer.pushSeg("depth").value(it.get(CMQC.MQIA_CURRENT_Q_DEPTH)).trace(ts).popSeg();
                    tracer.pushSeg("openin").value(it.get(CMQC.MQIA_OPEN_INPUT_COUNT)).trace(ts).popSeg();
                    tracer.pushSeg("openout").value(it.get(CMQC.MQIA_OPEN_OUTPUT_COUNT)).trace(ts).popSeg();
                    tracer.pushSeg("uncommited").value(it.get(CMQCFC.MQIACF_UNCOMMITTED_MSGS)).trace(ts).popSeg();
                    tracer.pushSeg("ageoom").value(it.get(CMQCFC.MQIACF_OLDEST_MSG_AGE)).trace(ts).popSeg();
                    try {
                        long[] onQTimes = it.get(1226);     
                        if(onQTimes[0]>-1) {
                            tracer.pushSeg("onqtime-recent").value(TimeUnit.MILLISECONDS.convert(onQTimes[0], TimeUnit.MICROSECONDS)).trace(ts).popSeg();
                            tracer.pushSeg("onqtime-recent-mcr").value(onQTimes[0]).trace(ts).popSeg();
                        }
                        if(onQTimes[1]>-1) {
                            tracer.pushSeg("onqtime").value(TimeUnit.MILLISECONDS.convert(onQTimes[1], TimeUnit.MICROSECONDS)).trace(ts).popSeg();                        
                            tracer.pushSeg("onqtime-mcr").value(onQTimes[1]).trace(ts).popSeg();
                        }
                    } catch (x) {}
                } finally {
                    tracer.popTag();
                }                
            }
        } catch (x) {}
    }

    public void traceChannelStats(agent) {
        try {
            trace { tracer ->
                ts = System.currentTimeMillis();
                tracer.pushSeg("channel");                       
    	            request(true, agent, CMQCFC.MQCMD_INQUIRE_CHANNEL_STATUS, [(CMQCFC.MQCACH_CHANNEL_NAME):"*"]).each() { ch ->                    
                    try {
                        chName = ch.get('MQCACH_CHANNEL_NAME').trim();
                        chType = CHANNEL_TYPES.get(ch.get('MQIACH_CHANNEL_TYPE'));
                        tracer.pushTag("channel=$chName").pushTag("type=$chType");
                        // =====================================================================================================================================
                        //   Channel Batches
                        // =====================================================================================================================================
                        if(ch.containsKey('MQIACH_BATCHES')) tracer.pushSeg("batches").value(ch.remove('MQIACH_BATCHES')).trace(ts).popSeg();
                        if(ch.containsKey('MQIACH_BATCH_SIZE')) tracer.pushSeg("batchsize").value(ch.remove('MQIACH_BATCH_SIZE')).trace(ts).popSeg();

                        // =====================================================================================================================================
                        //   Channel Bytes/Buffers  Received/Sent
                        // =====================================================================================================================================
                        tracer.pushSeg("buffersreceived").value(ch.remove('MQIACH_BUFFERS_RCVD/MQIACH_BUFFERS_RECEIVED')).trace(ts).popSeg();
                        tracer.pushSeg("bytessreceived").value(ch.remove('MQIACH_BYTES_RCVD/MQIACH_BYTES_RECEIVED')).trace(ts).popSeg();
                        tracer.pushSeg("bufferssent").value(ch.remove('MQIACH_BUFFERS_SENT')).trace(ts).popSeg();
                        tracer.pushSeg("bytessent").value(ch.remove('MQIACH_BYTES_SENT')).trace(ts).popSeg();

                        // =====================================================================================================================================
                        //   Channel Batches
                        // =====================================================================================================================================
                        if(ch.containsKey('MQIACH_CURRENT_MSGS')) tracer.pushSeg("indoubtmsgs").value(ch.get('MQIACH_CURRENT_MSGS')).trace(ts).popSeg();  
                        if(ch.containsKey('MQIACH_CURRENT_SHARING_CONVS')) tracer.pushSeg("convs").value(ch.get('MQIACH_CURRENT_SHARING_CONVS')).trace(ts).popSeg();
                        if(ch.containsKey('MQIACH_MSGS')) tracer.pushSeg("msgs").value(ch.get('MQIACH_MSGS')).trace(ts).popSeg();
                        if(ch.containsKey('MQIACH_XMITQ_MSGS_AVAILABLE')) tracer.pushSeg("msgsavail").value(ch.get('MQIACH_XMITQ_MSGS_AVAILABLE')).trace(ts).popSeg();
                        
                    } finally {
                        tracer.popTag().popTag();
                    } 
                }
            }
        } catch (x) {
            log.info "Channel Trace Error: $x";
            x.printStackTrace(System.err);

        }
    }

    public void printMap(name, map) {
        if(map==null) {
            log.info "$name was null";
        } else {
            if(!(map instanceof Map)) {
                log.info "$name is not a map (${map.getClass().getName()})";
            } else {
                log.info "\tP:   $name";
                map.each() { k, v ->
                    log.info "\t\t[$k]  :  [$v]";
                }
            }
        }
    }

@Field
pcfConn = null;


try {
	if(pcfConn==null)  {
    	pcfConn = new PCFMessageAgent("wmq8", 1430, "JBOSS.SVRCONN")
    	qManager = pcfConn.getQManagerName();
    	log.info "Connected to $qManager";
	}
    

    //==================================================================================
    //          Meta-Data Cache
    //==================================================================================

    




    //==================================================================================
    //          Instance Vars
    //==================================================================================
    qManager = null;
    appTag = "app=wmq";

    /*
    host = get('host', { 
        String _hn = jmxHelper.getHostName(mbs);        
        return _hn;
    });
    */
    hostTag = "host=wmq8";


    //==================================================================================

    try {
        qManager = pcfConn.getQManagerName();
        subPrefix = "JMS:$qManager:"
        log.info "Connected to $qManager @ $mqHost";
        qnames = qNames(pcfConn);
        log.info "=========== Primary Queue Names ===========";
        qnames.keySet().each() { q ->
            log.info "\t$q"
        }
    } catch (ex) {
        log.info ex;
    }
    try {
        tnames = topicNames(pcfConn);
        log.info "=========== Topic Names ===========";
        tnames.keySet().each() { top ->
            log.info "\t$top"
        }
    } catch (ex) {
        log.info ex;
    }


    try {
        chnames = channelNames(pcfConn);
        log.info "=========== Channel Names ===========";
        chnames.keySet().each() { ch ->
            log.info "\t$ch"

        }
    } catch (ex) {
        ex.printStackTrace();
        //log.info ex;
    }
    try {
        subNames = subNames(pcfConn);
        log.info "=========== SUBS ===========";
        subNames.each() { k, v ->
            log.info "$k";
        }
    } catch (ex) {
        ex.printStackTrace();
        //log.info ex;
    }
    trace.clear();
    trace.pushSeg("mq").pushTag(hostTag).pushTag(appTag).pushTag("qm=$qManager");

    
    


    // QUEUE STATS
    trace { tracer ->        
        
        qnames = qNames(pcfConn);
        qnames.keySet().each() { qName ->
            log.info "TRACING [$qName]";
            traceQueueStats(pcfConn, qName);
        }
    }

    // TOPIC STATS
    trace { tracer ->
        ts = System.currentTimeMillis();
        tracer.pushSeg("topic");       
        tNames = tnames.keySet();
        Map<String, Map<String, Map>> subNames = subNames(pcfConn);
        request(true, pcfConn, CMQCFC.MQCMD_INQUIRE_TOPIC_STATUS, [(CMQC.MQCA_TOPIC_STRING):"#", (CMQCFC.MQIACF_TOPIC_STATUS_TYPE):CMQCFC.MQIACF_TOPIC_STATUS]).each() {  attrs ->
            topicName = attrs.get("MQCA_ADMIN_TOPIC_NAME");                
            if(!topicName.trim().isEmpty() && !topicName.trim().startsWith("SYSTEM.")) {
                try {
                    topicString = attrs.get("MQCA_TOPIC_STRING");
                    subAttributes = subNames.get(topicName.trim());
                    pubCount = attrs.get("MQIA_PUB_COUNT");
                    subCount = attrs.get("MQIA_SUB_COUNT");
                    tracer.pushTag("topic=${topicName.trim()}");
                    tracer.pushSeg("pubs").value(pubCount).trace(ts).popSeg();
                    tracer.pushSeg("subs").value(subCount).trace(ts).popSeg();
                    try {
                        if(pubCount > 0) {
                            request(true, pcfConn, CMQCFC.MQCMD_INQUIRE_TOPIC_STATUS, [(CMQC.MQCA_TOPIC_STRING):topicString, (CMQCFC.MQIACF_TOPIC_STATUS_TYPE):CMQCFC.MQIACF_TOPIC_PUB]).each() {
                                secondsSinceLastPub = (ts/1000) - (formatDate(it.get('MQCACF_LAST_PUB_DATE'), it.get('MQCACF_LAST_PUB_TIME')).getTime() / 1000);
                                msgCount = it.get('MQIACF_PUBLISH_COUNT');                                
                                tracer.pushSeg("pubs").pushSeg("ssincelastmsg").value(pubCount).trace(ts).popSeg().popSeg();
                            }                    
                        }
                    } catch (ex) {
                        log.info "PUB Inquiry Failed: $ex";
                    }                
                    try {
                        if(subCount > 0) {
                            request(false, pcfConn, CMQCFC.MQCMD_INQUIRE_TOPIC_STATUS, [(CMQC.MQCA_TOPIC_STRING):topicString, (CMQCFC.MQIACF_TOPIC_STATUS_TYPE):CMQCFC.MQIACF_TOPIC_SUB]).each() {
                                //log.info "SUB: $it \t\t----  [${it.getClass().getName()}]"
                                secondsSinceLastMsg = (ts/1000) - (formatDate(it.get(CMQCFC.MQCACF_LAST_MSG_DATE), it.get(CMQCFC.MQCACF_LAST_MSG_TIME)).getTime() / 1000);                                                                
                                msgCount = it.get(CMQCFC.MQIACF_MESSAGE_COUNT);
                                byte[] subId = it.get(CMQCFC.MQBACF_SUB_ID);
                                log.info "SUB ID: $subId";
                                persistentQueue = null;
                                strSubId = "$subId".toString();                                
                                subName = subAttributes.get(strSubId);
                                if(subName==null || subName.trim().isEmpty()) {
                                    m = getSubName(pcfConn, subId);
                                    subName = m.get("MQCACF_SUB_NAME");
                                    persistentQueue = m.get("MQCACF_DESTINATION");

                                } else {
                                    persistentQueue = subNames.get(topicName.trim()).get(subName).get('MQCACF_DESTINATION');
                                }
                                log.info "PQUEUE: $persistentQueue";
                                try {
                                    tracer.pushSeg("subs").pushTag("subscription=$subName");
                                    tracer.pushSeg("ssincelastmsg").value(secondsSinceLastMsg).trace(ts).popSeg();
                                    tracer.pushSeg("msgcount").value(msgCount).trace(ts).popSeg();
                                    if(persistentQueue != null && !persistentQueue.trim().isEmpty()) {
                                        traceQueueStats(pcfConn, persistentQueue)
                                    }

                                } finally {
                                    tracer.popSeg().popTag();
                                }
                                
                        //request(false, agent, CMQCFC.MQCMD_INQUIRE_TOPIC_STATUS, [(CMQC.MQCA_TOPIC_STRING):topicString, (CMQCFC.MQIACF_TOPIC_STATUS_TYPE):CMQCFC.MQIACF_TOPIC_SUB]).each() { sub ->
                        //    byte[] subId = sub.get(CMQCFC.MQBACF_SUB_ID);


                            }
                        }
                    } catch (ex) {
                        log.info "SUB Inquiry Failed: $ex";
                        ex.printStackTrace(System.err);
                    }
                } finally {
                    tracer.popTag();
                }                
            }
        }            
    }

    // CHANNEL STATS
    trace { tracer ->
        traceChannelStats(pcfConn);
    }



} catch (ex) {
    ex.printStackTrace(System.out);
} finally {
    //log.info "============= Done ============="    
    if(pcfConn!=null) try {pcfConn.disconnect();} catch (e) {}
}

