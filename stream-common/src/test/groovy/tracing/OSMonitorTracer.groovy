import com.heliosapm.streams.tracing.*;
import static com.heliosapm.streams.tracing.TracerFactory.*;
import com.heliosapm.streams.tracing.writers.*;
import org.helios.nativex.sigar.HeliosSigar;
import org.hyperic.sigar.*;
import java.util.concurrent.atomic.*;
import org.hyperic.sigar.ptql.*;
import org.apache.logging.log4j.*;
TSDBHOST = "localhost";
TSDBPORT = 4242;

sigar = HeliosSigar.getInstance();
System.setProperty(CONFIG_WRITER_CLASS, "com.heliosapm.streams.tracing.writers.TelnetWriter");
//System.setProperty(CONFIG_WRITER_CLASS, "com.heliosapm.streams.tracing.writers.ConsoleWriter");

System.setProperty(NetWriter.CONFIG_REMOTE_URIS, "$TSDBHOST:$TSDBPORT");
ITracer tracer = TracerFactory.getInstance(null).getTracer();
println "Tracer Type: ${tracer.getClass().getName()}";
processFinder = new ProcessFinder(sigar.getSigar());
LogManager.getLogger(com.heliosapm.streams.tracing.writers.TelnetWriter.StreamedMetricEncoder.class).setLevel(Level.DEBUG);
println LogManager.getLogger(com.heliosapm.streams.tracing.writers.TelnetWriter.StreamedMetricEncoder.class).getLevel();


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
			tracer.flush();		
		}
	}
	println "Completed CPU Tracing in $elapsed ms.";

	elapsed = tracer.time {
		try {
			tracer.seg("sys.fs").pushTs(System.currentTimeMillis());
			sigar.getFileSystemList().each() { fs ->
				println "Tracing FS: name: [${fs.getDirName()}], type: [${fs.getSysTypeName()}]";
				try {
					fsu = sigar.getFileSystemUsage(fs.getDirName());
					tracer.pushTags([name : fs.getDirName(), type : fs.getSysTypeName()]);					
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
		} catch (x) {
			x.printStackTrace(System.err);
		} finally {
			tracer.flush();		
		}
	}
	println "Completed FileSystem Tracing in $elapsed ms.";


	Thread.sleep(5000);
}



