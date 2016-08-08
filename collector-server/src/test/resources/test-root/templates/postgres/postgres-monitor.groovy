@Dependency("${postgres.datasource}")
def sql;
sd = sql.firstRow("SELECT NOW() SYSDATE").SYSDATE; 
log.info("Time --X0X-- {}", sd);
