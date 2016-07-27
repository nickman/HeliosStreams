println "Running ${getClass().getName()}";
sql = globalCache.get("groovyds/testdb");
sd = sql.firstRow("SELECT SYSDATE FROM DUAL").SYSDATE;
log.info("SYSDATE: [{}]", sd);
