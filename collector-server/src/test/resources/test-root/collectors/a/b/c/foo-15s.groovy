//sql = globalCache.get("groovyds/localtsdb");
long start = System.currentTimeMillis();
sql = globalCache.get("groovyds/testdb");
//sd = sql.firstRow("SELECT NOW() SYSDATE FROM DUAL").SYSDATE;
statMap = [:];
sql.eachRow("SELECT TRADE_QUEUE_STATUS_CODE Q, COUNT(*) C FROM ECS.TRADEQUEUE WHERE PARTITION_KEY = (SELECT TO_NUMBER(TO_CHAR(CURR_BUSINESS_DATE, 'DD')) FROM ECS.CLEARINGDOMAIN) GROUP BY TRADE_QUEUE_STATUS_CODE", {	
	statMap.put(it.Q, it.C);
});
long elapsed = System.currentTimeMillis() - start;
log.info("TQ Status - {}, Elapsed: {} ms.", statMap.toString(), elapsed);
