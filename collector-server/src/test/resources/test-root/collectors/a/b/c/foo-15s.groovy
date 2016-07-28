println "Running ${getClass().getName()}";
sql = globalCache.get("groovyds/localtsdb");
sd = sql.firstRow("SELECT NOW() SYSDATE FROM DUAL").SYSDATE;
log.info("SYSDATE: [{}]", sd);
