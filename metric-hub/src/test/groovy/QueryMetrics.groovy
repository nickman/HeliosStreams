import com.heliosapm.streams.metrichub.*;
import com.heliosapm.utils.jmx.JMXHelper;

try { JMXHelper.fireUpJMXMPServer(4249); } catch(x) { x.printStackTrace(System.err); }

p = new Properties();
//p.setProperty("db.url", "jdbc:postgresql://pdk-pt-cltsdb-05:5432/tsdb");
p.setProperty("db.url", "jdbc:postgresql://localhost:5432/tsdb");
p.setProperty("db.testsql", "SELECT current_timestamp");
p.setProperty("db.datasource.username", "tsdb");
p.setProperty("db.datasource.password", "tsdb");
p.setProperty("db.datasource.classname", "org.postgresql.ds.PGSimpleDataSource");

HubManager.init(p);
new RequestBuilder("1d-ago", Aggregator.AVG)
    .downSampling("10m-avg")
    .execute("sys.cpu:host=*,*")
//    .execute("linux.cpu.percpu:host=*,cpu=*") 
    .stream()
    .forEach({qr -> System.err.println(qr)});