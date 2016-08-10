import com.heliosapm.streams.tracing.*;
import static com.heliosapm.streams.tracing.TracerFactory.*;
import static com.heliosapm.streams.tracing.SuppressPredicates.*;
import com.heliosapm.streams.tracing.writers.*;
import org.helios.nativex.sigar.HeliosSigar;
import org.hyperic.sigar.*;
import java.util.concurrent.atomic.*;
import org.hyperic.sigar.ptql.*;
import org.apache.logging.log4j.*;
import java.lang.management.*;
import static com.heliosapm.utils.jmx.JMXHelper.*;
TSDBHOST = "localhost";
TSDBPORT = 4242;
runtime = ManagementFactory.getRuntimeMXBean().getName();
println "I am [$runtime]";
sigar = HeliosSigar.getInstance();
System.setProperty(CONFIG_WRITER_CLASS, "com.heliosapm.streams.tracing.writers.KafkaSyncWriter");
//System.setProperty(CONFIG_WRITER_CLASS, "com.heliosapm.streams.tracing.writers.LoggingWriter");

System.setProperty("metricwriter.kafka.bootstrap.servers", "localhost:9093,localhost:9094");
System.setProperty("metricwriter.kafka.acks", "1");
System.setProperty("metricwriter.kafka.retries", "0");
System.setProperty("metricwriter.kafka.batch.size", "16384");
System.setProperty("metricwriter.kafka.linger.ms", "1");
System.setProperty("metricwriter.kafka.buffer.memory", "33554432");
System.setProperty("metricwriter.kafka.topics", "tsdb.metrics.binary");
System.setProperty("metricwriter.kafka.client.id", "OSMonitorTracer");
System.setProperty("metricwriter.kafka.compression.type", "gzip");
System.setProperty("metricwriter.kafka.interceptor.classes", "com.heliosapm.streams.common.kafka.interceptor.MonitoringProducerInterceptor");
//System.setProperty(CONFIG_WRITER_CLASS, "com.heliosapm.streams.tracing.writers.ConsoleWriter");

System.setProperty(NetWriter.CONFIG_REMOTE_URIS, "$TSDBHOST:$TSDBPORT");
ITracer tracer = TracerFactory.getInstance(new Properties()).getTracer().setMaxTracesBeforeFlush(1200);
println "Tracer Type: ${tracer.getClass().getName()}";
processFinder = new ProcessFinder(sigar.getSigar());
jmxServer = fireUpJMXMPServer(1294);

//LogManager.getLogger(com.heliosapm.streams.tracing.writers.TelnetWriter.class).setLevel(Level.DEBUG);

try {
while(true) {

    long elapsed = tracer.time {
        try {
            tracer.seg("sys.cpu").pushKeys("cpu").pushTs(System.currentTimeMillis());
            sigar.getCpuPercList().eachWithIndex() { cpu, index ->
                tracer
                    .pushSeg("combined").trace(cpu.getCombined()*100, "$index").popSeg()
                    .pushSeg("idle").trace(cpu.getIdle()*100, "$index").popSeg()
                    .pushSeg("irq").trace(cpu.getIrq()*100, "$index").popSeg()
                    .pushSeg("nice").trace(cpu.getNice()*100, "$index").popSeg()
                    .pushSeg("softirq").trace(cpu.getSoftIrq()*100, "$index").popSeg()
                    .pushSeg("stolen").trace(cpu.getStolen()*100, "$index").popSeg()
                    .pushSeg("sys").trace(cpu.getSys()*100, "$index").popSeg()
                    .pushSeg("user").trace(cpu.getUser()*100, "$index").popSeg()
                    .pushSeg("wait").trace(cpu.getWait()*100, "$index").popSeg()

            }        
        } catch (x) {
            x.printStackTrace(System.err);
        } finally {
            //tracer.flush();        
        }
    }
    println "Completed CPU Tracing in $elapsed ms.";

    elapsed = tracer.time {
        try {
            tracer.seg("sys.fs").pushTs(System.currentTimeMillis()).setSuppressPredicate(IGNORE_NEGATIVE);
            sigar.getFileSystemList().each() { fs ->
                fsu = null;
                try { fsu = sigar.getFileSystemUsage(fs.getDirName()); } catch (x) {}
                if(fsu != null) {
                    tracer.pushTags([name : fs.getDirName(), type : fs.getSysTypeName()]);                    
                    try {
                        tracer
                            .pushSeg("avail").trace(fsu.getAvail()).popSeg()
                            .pushSeg("queue").trace(fsu.getDiskQueue()).popSeg()
                            .pushSeg("files").trace(fsu.getFiles()).popSeg()
                            .pushSeg("free").trace(fsu.getFree()).popSeg()
                            .pushSeg("freefiles").trace(fsu.getFreeFiles()).popSeg()
                            .pushSeg("total").trace(fsu.getTotal()).popSeg()
                            .pushSeg("used").trace(fsu.getUsed()).popSeg()
                            .pushSeg("usedperc").trace(fsu.getUsePercent()).popSeg()
                                .pushSeg("bytes")
                            .pushSeg("reads").trace(fsu.getDiskReadBytes()).popSeg()
                            .pushSeg("writes").trace(fsu.getDiskWriteBytes()).popSeg()
                                .popSeg()
                                .pushSeg("ios")
                            .pushSeg("reads").trace(fsu.getDiskReads()).popSeg()
                            .pushSeg("writes").trace(fsu.getDiskWrites()).popSeg()
                            .popSeg();
                    } finally {
                        tracer.popTags(2);
                    }
                }
            }
        } catch (x) {
            x.printStackTrace(System.err);
        } finally {
            //tracer.flush();        
        }
    }

    println "Completed FileSystem Tracing in $elapsed ms.";

    elapsed = tracer.time {
        try {
            
            tracer.seg("sys.net.iface").pushTs(System.currentTimeMillis()).setSuppressPredicate(IGNORE_NEGATIVE);
            sigar.getNetInterfaceList().each() { iface ->                
                try {
                    ifs = sigar.getNetInterfaceStat(iface);
                    tracer.pushTags([name : iface, dir : "rx"]);                    
                    tracer
                        .pushSeg("bytes").trace(ifs.getRxBytes()).popSeg()
                        .pushSeg("packets").trace(ifs.getRxPackets()).popSeg()
                        .pushSeg("dropped").trace(ifs.getRxDropped()).popSeg()
                        .pushSeg("errors").trace(ifs.getRxErrors()).popSeg()
                        .pushSeg("overruns").trace(ifs.getRxOverruns()).popSeg();
                    tracer.popTags(2);
                    tracer.pushTags([name : iface, dir : "tx"]);                    
                    tracer
                        .pushSeg("bytes").trace(ifs.getTxBytes()).popSeg()
                        .pushSeg("packets").trace(ifs.getTxPackets()).popSeg()
                        .pushSeg("dropped").trace(ifs.getTxDropped()).popSeg()
                        .pushSeg("errors").trace(ifs.getTxErrors()).popSeg()
                        .pushSeg("overruns").trace(ifs.getTxOverruns()).popSeg();

                } finally {
                    tracer.popTags(2);
                }
            }
        } catch (x) {
            x.printStackTrace(System.err);
        } finally {
            //tracer.flush();                    
        }
    }

    println "Completed Net Interface Tracing in $elapsed ms.";
    
    elapsed = tracer.time {
        try {
            tracer.seg("sys.net.tcp").pushTs(System.currentTimeMillis()).setSuppressPredicate(IGNORE_NEGATIVE);
            tcp = sigar.getTcp();
            tracer
                .pushSeg("retrans").trace(tcp.getRetransSegs()).popSeg()
                .pushSeg("passiveopens").trace(tcp.getPassiveOpens()).popSeg()
                .pushSeg("currestab").trace(tcp.getCurrEstab()).popSeg()
                .pushSeg("estabresets").trace(tcp.getEstabResets()).popSeg()
                .pushSeg("attemptfails").trace(tcp.getAttemptFails()).popSeg()
                .pushSeg("insegs").trace(tcp.getInSegs()).popSeg()
                .pushSeg("outsegs").trace(tcp.getOutSegs()).popSeg()
                .pushSeg("activeopens").trace(tcp.getActiveOpens()).popSeg()
                .pushSeg("inerrs").trace(tcp.getInErrs()).popSeg()
                .pushSeg("outrsts").trace(tcp.getOutRsts()).popSeg();

        } catch (x) {
            x.printStackTrace(System.err);
        } finally {
            //tracer.flush();                    
        }
    }

    println "Completed TCP Tracing in $elapsed ms.";


    tracer.flush();    
    //println tracer.dump();


    Thread.sleep(5000);
} } finally {
    try { jmxServer.stop(); println "JMXServer Stopped"; } catch (x) {}
}