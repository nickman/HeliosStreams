@Dependency("${postgres.datasource}")
def sql;
sd = sql.firstRow("SELECT NOW() SYSDATE").SYSDATE; 
log.info("Time --XX-- {}", sd);
