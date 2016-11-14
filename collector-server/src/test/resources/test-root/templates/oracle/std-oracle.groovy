Dependency("groovyds/${dbname}")
def sql;
//sql = globalCache.get("groovyds/testdb");
//sd = sql.firstRow("SELECT NOW() SYSDATE").SYSDATE;
import java.util.regex.*;

@Field PUNCT = Pattern.compile("\\p{Punct}");

@Field
systemEvents = """SELECT A.EVENT, A.TOTAL_WAITS WAITS, A.TIME_WAITED WAITED, A.AVERAGE_WAIT AWAIT
FROM V\$SYSTEM_EVENT A, V\$EVENT_NAME B, V\$SYSTEM_WAIT_CLASS C
WHERE A.EVENT_ID=B.EVENT_ID
AND B.WAIT_CLASS#=C.WAIT_CLASS#
AND C.WAIT_CLASS IN ('Application','Concurrency')""";

cleanEvent = { s ->
        return PUNCT.matcher(s).replaceAll("").replace(" ", "_");
}

log.info("\n\t========\n$systemEvents\n");
def app = navmap[0];
def host = navmap[1];
tracer.pushTags([app: "$app", host: "$host"]);
tracer {
        tracer.pushSeg("oracle");
        tracer {
                tracer.pushSeg("sysevent");
                long ts = System.currentTimeMillis();
                sql.eachRow(systemEvents, { row ->
                        tracer.pushSeg(cleanEvent(row.EVENT));
                        tracer.pushSeg("waits").dtrace(row.WAITS.longValue(), ts).popSeg();
                        tracer.pushSeg("waited").dtrace(row.WAITED.longValue(), ts).popSeg();
                        tracer.pushSeg("avgwait").trace(row.AWAIT.longValue(), ts).popSeg();
                        tracer.popSeg();
                });
                tracer.popSeg();
        }
}



// =================  REQUIRES A DS LIKE THIS ========================

dataSourceClassName=oracle.jdbc.pool.OracleDataSource
dataSource.user=ecs
dataSource.password=ecs
dataSource.url=jdbc:oracle:thin:@//<host>:1521/<service>
connectionTimeout=10000
idleTimeout=120000
minimumIdle=1
maximumPoolSize=3
maxLifetime=30000
initializationFailFast=true
allowPoolSuspension=true
readOnly=true
registerMbeans=true
validationTimeout=5000
leakDetectionThreshold=10000
