@echo off
set KAFKA_HOME=d:\services\kafka\kafka_2.11-0.10.0.0
set ZOOKEEP_URI=localhost:2181
set REPL=1
pushd .
cd %KAFKA_HOME%
:: The OpenTSDB ingestion topic. The OpenTSDB plugin reads metrics from this topic and writes them into HBase.
call bin\windows\kafka-topics.bat --zookeeper %ZOOKEEP_URI% --create --topic tsdb.metrics.binary --partitions 10 --replication-factor %REPL%
:: Accumulator topic. Metrics are counted infinitely and then written to tsdb.metrics.binary
call bin\windows\kafka-topics.bat --zookeeper %ZOOKEEP_URI% --create --topic tsdb.metrics.accumulator --partitions 10 --replication-factor %REPL%
:: Meter topic. Metrics are counted within a defined window to get a rate and then written to tsdb.metrics.binary
call bin\windows\kafka-topics.bat --zookeeper %ZOOKEEP_URI% --create --topic tsdb.metrics.meter --partitions 10 --replication-factor %REPL%
:: Delta topic. Metrics are computed as a delta between the current value and the prior and then written to tsdb.metrics.binary
call bin\windows\kafka-topics.bat --zookeeper %ZOOKEEP_URI% --create --topic tsdb.metrics.delta --partitions 10 --replication-factor %REPL%
:: Period Aggregation topic. Metrics are min/max/avg/count aggregated for a given window and then written to tsdb.metrics.binary
call bin\windows\kafka-topics.bat --zookeeper %ZOOKEEP_URI% --create --topic tsdb.metrics.pagg --partitions 10 --replication-factor %REPL%
:: Period Sticky Aggregation topic. Metrics are min/max/avg/count aggregated for a given window and then written to tsdb.metrics.binary
call bin\windows\kafka-topics.bat --zookeeper %ZOOKEEP_URI% --create --topic tsdb.metrics.psagg --partitions 10 --replication-factor %REPL%
:: Straight through topic. Metrics are written to tsdb.metrics.binary
call bin\windows\kafka-topics.bat --zookeeper %ZOOKEEP_URI% --create --topic tsdb.metrics.st --partitions 10 --replication-factor %REPL%
:: Metrics are examined to determine their type and then routed accordingly. Metrics are written to tsdb.metrics.binary
call bin\windows\kafka-topics.bat --zookeeper %ZOOKEEP_URI% --create --topic tsdb.metrics.directed --partitions 10 --replication-factor %REPL%

popd

