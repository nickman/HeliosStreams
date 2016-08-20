import org.helios.nativex.sigar.HeliosSigar;
import org.hyperic.sigar.*;
import java.util.concurrent.atomic.*;
import java.util.zip.*;
import org.hyperic.sigar.ptql.*;
import org.apache.kafka.clients.producer.*;
import com.heliosapm.streams.metrics.*;
import java.lang.management.*;

//TO_TOPIC = "tsdb.metrics.accumulator"
TO_TOPIC = "tsdb.metrics.meter"

Properties props = new Properties();
//props.put("bootstrap.servers", "localhost:9093,localhost:9094");
props.put("bootstrap.servers", "localhost:9093");
props.put("acks", "all");
props.put("retries", 0);
props.put("batch.size", 16384);
props.put("linger.ms", 10);
props.put("buffer.memory", 33554432);
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

props.put("compression.codec", "1");
props.put("compressed.topics", TO_TOPIC);



Producer<String, StreamedMetricValue> producer = new KafkaProducer<String, StreamedMetricValue>(props);
pendingBuffer = [:];


sigar = HeliosSigar.getInstance();

processFinder = new ProcessFinder(sigar.getSigar());

buff = new StringBuilder();
HOST = InetAddress.getLocalHost().getHostName();
DC = "dc5";
stime = {
    return (long)System.currentTimeMillis()/1000;
}



pflush = { 
    print buff;
    buff.setLength(0);
}

clean = { s ->
    //return s.toString().replace(":", ";");
    return s;
}



trace = { metric, value, tags ->
    now = System.currentTimeMillis();
    // 1466684806814,0.6563913125582113,sys.cpu.total,webserver05,login-sso,host=webserver05,app=login-sso,colo=false,dc=us-west1

    buff.append("$now,$value,$metric,$HOST,perfagent,dc=$DC");
    tags.each() { k, v ->
        buff.append(",").append(clean(k)).append("=").append(clean(v));
    }
    //sm = StreamedMetric.fromString(buff.toString());
    pendingBuffer.put(metric, buff.toString());
    buff.setLength(0);  
}

flush = {
    if(!pendingBuffer.isEmpty()) {
        futures = [];
        int mod = 0;
        pendingBuffer.each() { k, v ->
            if(k==null || k.trim().isEmpty()) throw new Exception("Null Key");
            pr = new ProducerRecord<String, String>(TO_TOPIC, k, v);            
            futures.add(producer.send(pr));
            mod++;
        }
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
        pendingBuffer.clear();
    }
}

ctrace = { metric, value, tags ->
    if(value!=-1) {
        trace(metric, value, tags);
    }
}


try {
    long loop = 0;
    while(true) {
        long start = System.currentTimeMillis();
        loop++;        
        sigar.getCpuPercList().eachWithIndex() { cpu, index ->
            trace("sys.cpu", cpu.getCombined()*100, ['cpu':index, 'type':'combined']);
            trace("sys.cpu", cpu.getIdle()*100, ['cpu':index, 'type':'idle']);
            trace("sys.cpu", cpu.getIrq()*100, ['cpu':index, 'type':'irq']);
            trace("sys.cpu", cpu.getNice()*100, ['cpu':index, 'type':'nice']);
            trace("sys.cpu", cpu.getSoftIrq()*100, ['cpu':index, 'type':'softirq']);
            trace("sys.cpu", cpu.getStolen()*100, ['cpu':index, 'type':'stolen']);
            trace("sys.cpu", cpu.getSys()*100, ['cpu':index, 'type':'sys']);
            trace("sys.cpu", cpu.getUser()*100, ['cpu':index, 'type':'user']);
            trace("sys.cpu", cpu.getWait()*100, ['cpu':index, 'type':'wait']);
            flush();        
        }
        // sigar.getFileSystemList().each() { fs ->
        //     //println "FS: dir:${fs.getDirName()},  dev:${fs.getDevName()}, type:${fs.getSysTypeName()}, opts:${fs.getOptions()}";
        //     fsu = sigar.getFileSystemUsage(fs.getDirName());
        //     ctrace("sys.fs.avail", fsu.getAvail(), ['name':fs.getDirName(), 'type':fs.getSysTypeName()]);
        //     ctrace("sys.fs.queue", fsu.getDiskQueue(), ['name':fs.getDirName(), 'type':fs.getSysTypeName()]);
        //     ctrace("sys.fs.files", fsu.getFiles(), ['name':fs.getDirName(), 'type':fs.getSysTypeName()]);
        //     ctrace("sys.fs.free", fsu.getFree(), ['name':fs.getDirName(), 'type':fs.getSysTypeName()]);
        //     ctrace("sys.fs.freefiles", fsu.getFreeFiles(), ['name':fs.getDirName(), 'type':fs.getSysTypeName()]);
        //     ctrace("sys.fs.total", fsu.getTotal(), ['name':fs.getDirName(), 'type':fs.getSysTypeName()]);
        //     ctrace("sys.fs.used", fsu.getUsed(), ['name':fs.getDirName(), 'type':fs.getSysTypeName()]);
        //     ctrace("sys.fs.usedperc", fsu.getUsePercent(), ['name':fs.getDirName(), 'type':fs.getSysTypeName()]);
            
        //     ctrace("sys.fs.bytes", fsu.getDiskReadBytes(), ['name':fs.getDirName(), 'type':fs.getSysTypeName(), 'dir':'reads']);
        //     ctrace("sys.fs.bytes", fsu.getDiskWriteBytes(), ['name':fs.getDirName(), 'type':fs.getSysTypeName(), 'dir':'writes']);

        //     ctrace("sys.fs.ios", fsu.getDiskReads(), ['name':fs.getDirName(), 'type':fs.getSysTypeName(), 'dir':'reads']);
        //     ctrace("sys.fs.ios", fsu.getDiskWrites(), ['name':fs.getDirName(), 'type':fs.getSysTypeName(), 'dir':'writes']);

            
        //     flush();
        //     //println "[$fs]: $fsu";
        // }
        sigar.getNetInterfaceList().each() { iface ->
            ifs = sigar.getNetInterfaceStat(iface);
            trace("sys.net.iface", ifs.getRxBytes(), ['name':iface, 'dir':'rx', 'unit':'bytes']);
            trace("sys.net.iface", ifs.getRxPackets(), ['name':iface, 'dir':'rx', 'unit':'packets']);
            trace("sys.net.iface", ifs.getRxDropped(), ['name':iface, 'dir':'rx', 'unit':'dropped']);
            trace("sys.net.iface", ifs.getRxErrors(), ['name':iface, 'dir':'rx', 'unit':'errors']);
            trace("sys.net.iface", ifs.getRxOverruns(), ['name':iface, 'dir':'rx', 'unit':'overruns']);
            trace("sys.net.iface", ifs.getRxFrame(), ['name':iface, 'dir':'rx', 'unit':'frame']);
            
            trace("sys.net.iface", ifs.getTxBytes(), ['name':iface, 'dir':'tx', 'unit':'bytes']);
            trace("sys.net.iface", ifs.getTxPackets(), ['name':iface, 'dir':'tx', 'unit':'packets']);
            trace("sys.net.iface", ifs.getTxDropped(), ['name':iface, 'dir':'tx', 'unit':'dropped']);
            trace("sys.net.iface", ifs.getTxErrors(), ['name':iface, 'dir':'tx', 'unit':'errors']);
            trace("sys.net.iface", ifs.getTxOverruns(), ['name':iface, 'dir':'tx', 'unit':'overruns']);

            //println ifs;
            flush();
        }
        
        tcp = sigar.getTcp();
        trace("sys.net.tcp", tcp.getRetransSegs(), ['type':'RetransSegs']);
        trace("sys.net.tcp", tcp.getPassiveOpens(), ['type':'PassiveOpens']);
        trace("sys.net.tcp", tcp.getCurrEstab(), ['type':'CurrEstab']);
        trace("sys.net.tcp", tcp.getEstabResets(), ['type':'EstabResets']);
        trace("sys.net.tcp", tcp.getAttemptFails(), ['type':'AttemptFails']);
        trace("sys.net.tcp", tcp.getInSegs(), ['type':'InSegs']);
        trace("sys.net.tcp", tcp.getActiveOpens(), ['type':'ActiveOpens']);
        trace("sys.net.tcp", tcp.getInErrs(), ['type':'InErrs']);        
        trace("sys.net.tcp", tcp.getOutRsts(), ['type':'OutRsts']);        
        trace("sys.net.tcp", tcp.getOutSegs(), ['type':'OutSegs']);       
        
        netstat = sigar.getNetStat();
        
        //===================================================================================================================
        //        INBOUND
        //===================================================================================================================
        trace("sys.net.socket", netstat.getAllInboundTotal(), ['dir':'inbound', 'protocol':'all', 'state':'all']);
        trace("sys.net.socket", netstat.getTcpInboundTotal(), ['dir':'inbound', 'protocol':'tcp', 'state':'all']);       
        trace("sys.net.socket", netstat.getTcpBound(), ['dir':'inbound', 'protocol':'tcp', 'state':'bound']);
        trace("sys.net.socket", netstat.getTcpListen(), ['dir':'inbound', 'protocol':'tcp', 'state':'lastack']);        
        trace("sys.net.socket", netstat.getTcpLastAck(), ['dir':'inbound', 'protocol':'tcp', 'state':'lastack']);        
        trace("sys.net.socket", netstat.getTcpCloseWait(), ['dir':'inbound', 'protocol':'tcp', 'state':'closewait']);
        
        //===================================================================================================================
        //        OUTBOUND
        //===================================================================================================================
        trace("sys.net.socket", netstat.getAllOutboundTotal(), ['dir':'outbound', 'protocol':'all', 'state':'all']);
        trace("sys.net.socket", netstat.getTcpOutboundTotal(), ['dir':'outbound', 'protocol':'tcp', 'state':'all']);        
        trace("sys.net.socket", netstat.getTcpSynRecv(), ['dir':'outbound', 'protocol':'tcp', 'state':'synrecv']);        
        trace("sys.net.socket", netstat.getTcpSynSent(), ['dir':'outbound', 'protocol':'tcp', 'state':'synsent']);        
        trace("sys.net.socket", netstat.getTcpEstablished(), ['dir':'outbound', 'protocol':'tcp', 'state':'established']);
        trace("sys.net.socket", netstat.getTcpClose(), ['dir':'outbound', 'protocol':'tcp', 'state':'close']);
        trace("sys.net.socket", netstat.getTcpClosing(), ['dir':'outbound', 'protocol':'tcp', 'state':'closing']);
        trace("sys.net.socket", netstat.getTcpFinWait1(), ['dir':'outbound', 'protocol':'tcp', 'state':'finwait1']);
        trace("sys.net.socket", netstat.getTcpFinWait2(), ['dir':'outbound', 'protocol':'tcp', 'state':'finwait2']);
        trace("sys.net.socket", netstat.getTcpIdle(), ['dir':'outbound', 'protocol':'tcp', 'state':'idle']);
        trace("sys.net.socket", netstat.getTcpTimeWait(), ['dir':'outbound', 'protocol':'tcp', 'state':'timewait']);        
        //===================================================================================================================
        //        SERVER SOCKETS
        //===================================================================================================================        
        connMap = new TreeMap<String, TreeMap<String, TreeMap<String, AtomicInteger>>>();
        sigar.getNetConnectionList(NetFlags.CONN_SERVER | NetFlags.CONN_PROTOCOLS).each() {
            addr = InetAddress.getByName(it.getLocalAddress()).getHostAddress();
            port = "${addr}:${it.getLocalPort()}";
            state = it.getStateString();
            protocol = it.getTypeString();
            stateMap = connMap.get(port);
            if(stateMap==null) {
                stateMap = new TreeMap<String, TreeMap<String, Integer>>();
                connMap.put(port, stateMap);
            }
            protocolMap = stateMap.get(state);
            if(protocolMap==null) {
                protocolMap = new TreeMap<String, AtomicInteger>();
                stateMap.put(state, protocolMap);
            }
            counter = protocolMap.get(protocol);
            if(counter==null) {
                counter = new AtomicInteger(0);
                protocolMap.put(protocol, counter);
            }
            counter.incrementAndGet();            
        }
        connMap.each() { port, stateMap ->
            stateMap.each() { state, protocolMap ->
                protocolMap.each() { protocol, counter ->
                    index = port.lastIndexOf(":");
                    addr = port.substring(0, index);
                    p = port.substring(index+1);
                    //println "Port: $port, State: $state, Protocol: $protocol, Count: ${counter.get()}";
                    trace("sys.net.server", counter.get(), ['protocol':protocol, 'state':state.toLowerCase(), 'port':p, 'bind':addr]);
                }
            }
        }
        //===================================================================================================================
        //        CLIENT SOCKETS
        //===================================================================================================================        
        connMap = new TreeMap<String, TreeMap<String, TreeMap<String, AtomicInteger>>>();
        sigar.getNetConnectionList(NetFlags.CONN_CLIENT | NetFlags.CONN_PROTOCOLS).each() {
            addr = InetAddress.getByName(it.getRemoteAddress()).getHostAddress();
            port = "${addr}:${it.getRemotePort()}";
            state = it.getStateString();
            protocol = it.getTypeString();
            stateMap = connMap.get(port);
            if(stateMap==null) {
                stateMap = new TreeMap<String, TreeMap<String, Integer>>();
                connMap.put(port, stateMap);
            }
            protocolMap = stateMap.get(state);
            if(protocolMap==null) {
                protocolMap = new TreeMap<String, AtomicInteger>();
                stateMap.put(state, protocolMap);
            }
            counter = protocolMap.get(protocol);
            if(counter==null) {
                counter = new AtomicInteger(0);
                protocolMap.put(protocol, counter);
            }
            counter.incrementAndGet();            
        }
        connMap.each() { port, stateMap ->
            stateMap.each() { state, protocolMap ->
                protocolMap.each() { protocol, counter ->
                    index = port.lastIndexOf(":");
                    addr = port.substring(0, index);
                    p = port.substring(index+1);
                    //println "Port: $port, State: $state, Protocol: $protocol, Count: ${counter.get()}";
                    trace("sys.net.client", counter.get(), ['protocol':protocol, 'state':state.toLowerCase(), 'port':p, 'address':addr]);
                }
            }
        }        
        flush();
        // ===================================================================================================================================
        //        SYSTEM MEMORY
        // ===================================================================================================================================
        mem = sigar.getMem();
        
        trace("sys.mem", mem.getUsed(), ['unit':'used']);       
        trace("sys.mem", mem.getFree(), ['unit':'used']);       
        
        trace("sys.mem.actual", mem.getActualFree(), ['unit':'free']);       
        trace("sys.mem.actual", mem.getActualUsed(), ['unit':'used']);       
        
        trace("sys.mem.total", mem.getTotal(), ['unit':'bytes']);       
        trace("sys.mem.total", mem.getRam(), ['unit':'MB']);       
        
        trace("sys.mem.percent", mem.getFreePercent(), ['unit':'free']);       
        trace("sys.mem.percent", mem.getUsedPercent(), ['unit':'used']);       
        
        // ===================================================================================================================================
        //    SWAP
        // ===================================================================================================================================
        swap = sigar.getSwap();
        swapFree = swap.getFree();
        swapUsed = swap.getUsed();
        swapTotal = swap.getTotal();
        trace("sys.swap", swapFree, ['unit': 'free']);
        trace("sys.swap", swapUsed, ['unit': 'used']);
        trace("sys.swap", swapTotal, ['unit': 'total']);
        trace("sys.swap.percent", swapUsed/swapTotal*100, ['unit': 'used']);
        trace("sys.swap.percent", swapFree/swapTotal*100, ['unit': 'free']);
        trace("sys.swap.page", swap.getPageIn(), ['dir': 'in']);
        trace("sys.swap.page", swap.getPageOut(), ['dir': 'out']);
        flush();
        // ===================================================================================================================================
        //    PROCESS STATS
        // ===================================================================================================================================
        procStat = sigar.getProcStat();
        trace("sys.procs.state", procStat.getIdle(), ['state': 'idle']);
        trace("sys.procs.state", procStat.getRunning(), ['state': 'running']);
        trace("sys.procs.state", procStat.getSleeping(), ['state': 'sleeping']);
        trace("sys.procs.state", procStat.getStopped(), ['state': 'stopped']);
        trace("sys.procs.state", procStat.getZombie(), ['state': 'zombie']);
        
        trace("sys.procs.threads", procStat.getThreads(), []);
        trace("sys.procs.count", procStat.getTotal(), []);
        flush();
        // ===================================================================================================================================
        //    LOAD AVERAGE
        // ===================================================================================================================================
        double[] load = sigar.getLoadAverage();
        trace("sys.load", load[0], ['period': '1m']);
        trace("sys.load", load[1], ['period': '5m']);
        trace("sys.load", load[2], ['period': '15m']);
        flush();
        
        // ===================================================================================================================================
        //    PROCESS GROUPS
        // ===================================================================================================================================
        processQueries = [
            "sshd" : "State.Name.ew=sshd",
            "apache2": "State.Name.ew=apache2",
            "java": "State.Name.ew=java",
            "python": "State.Name.ct=python"
        ];     
        // ===================================================================================================================================
        //    PROCESS GROUP CPU STATS
        // ===================================================================================================================================
        processQueries.each() { exe, query ->
            mcpu = sigar.getMultiProcCpu(query);
            trace("procs", mcpu.getPercent() * 100, ['exe':exe, 'unit':'percentcpu']);
            trace("procs", mcpu.getProcesses(), ['exe':exe, 'unit':'count']);            
        }
        flush();
        // ===================================================================================================================================
        //    PROCESS GROUP MEM STATS
        // ===================================================================================================================================
        processQueries.each() { exe, query ->
            
            mmem = sigar.getMultiProcMem(query);

            trace("procs", mmem.getMajorFaults(), ['exe':exe, 'unit':'majorfaults']);
            trace("procs", mmem.getMinorFaults(), ['exe':exe, 'unit':'minorfaults']);            
            trace("procs", mmem.getPageFaults(), ['exe':exe, 'unit':'pagefaults']);            
            trace("procs", mmem.getResident(), ['exe':exe, 'unit':'resident']);            
            trace("procs", mmem.getShare(), ['exe':exe, 'unit':'share']);            
            trace("procs", mmem.getSize(), ['exe':exe, 'unit':'size']);            
            trace("procs", mmem.getSize(), ['exe':exe, 'unit':'size']);            
        }
        flush();
        
        
        
        
        //println tcp;

        //NetFlags.CONN_TCP | NetFlags.CONN_CLIENT
        sigar.getNetConnectionList(NetFlags.CONN_SERVER | NetFlags.CONN_UDP ).each() {
            //println "SendQueue=${it.getSendQueue()}, ReceiveQueue=${it.getReceiveQueue()}, State=${it.getStateString()}, Type=${it.getTypeString()}, LocalPort=${it.getLocalPort()}, RemoteAddress=${it.getRemoteAddress()}, RemotePort=${it.getRemotePort()}";
        }
        
        long elapsed = System.currentTimeMillis() - start;
        if(loop%10==0) println "Scan complete in $elapsed ms.";
        //break;
        Thread.sleep(1000);
    }
    
    
        //trace(tsdbSocket);
        //     trace(tsdbSocket, "put sys.cpu.user ${stime()} 42.5 host=webserver1 cpu=0\n");
        //println "${it.getCombined()*100}  -  ${it.format(it.getCombined())}";
    //}

} finally {
    try { tsdbSocket.close(); } catch (e) {}
    println "Closed";
}

return null;