@Dependency("${postgres.datasource}")
def sql;
tracer.reset().tags([host : "leopard", app : "postgres"]).pushSeg("postgres");
// ============================= Queries =============================
@Field
pgstats = """select
datname db,
COALESCE(usename, 'anon') as "user",
COALESCE(NULLIF(trim(application_name), ''), 'none') dbapp,
COALESCE(NULLIF(trim(state), ''), 'none') state,
count(*) cnt
from pg_stat_activity
group by datname,
usename,
COALESCE(NULLIF(trim(application_name), ''), 'none'),
COALESCE(NULLIF(trim(state), ''), 'none')""";
@Field
db_pgstats = """select
datname db,
numbackends conns,
xact_commit,
xact_rollback,
blks_read,
blks_hit,
tup_returned,
tup_fetched,
tup_inserted,
tup_updated,
tup_deleted,
conflicts,
temp_files,
temp_bytes,
deadlocks,
blk_read_time,
blk_write_time
from pg_stat_database""";
@Field
db_pgstats_gauges = ["conns"];
@Field
db_pgstats_monot = ["xact_commit","xact_rollback","blks_read","blks_hit","tup_returned","tup_fetched","tup_inserted","tup_updated","tup_deleted","conflicts","temp_files","temp_bytes","deadlocks","blk_read_time","blk_write_time"];

// ===================================================================
tracer {
	tracer.pushKeys("db", "user", "dbapp", "state").pushSeg("session.states");
	sql.eachRow(pgstats, {
		tracer.trace(it.cnt, it.db, it.user, it.dbapp, it.state);
	});
}


tracer {
	tracer.pushKeys("db").pushSeg("db");
	sql.eachRow(db_pgstats, { row ->
		db_pgstats_gauges.each() { v ->
			tracer {
				tracer.pushSeg(v).trace(row."$v", row.db).popSeg();
			}
		}		
		db_pgstats_monot.each() { v ->
			tracer {
				tracer.pushSeg(v).dtrace(row."$v", row.db).popSeg();
			}
		}		
	});
}

sd = sql.firstRow("SELECT NOW() SYSDATE").SYSDATE; 
log.info("Time --X0X-- {}", sd);
