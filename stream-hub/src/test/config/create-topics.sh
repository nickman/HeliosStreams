#!/bin/bash
export KAFKA_HOME=/home/nwhitehe/services/kafka/kafka_2.11-0.10.0.0
export ZOOKEEP_URI=localhost:2181
export REPL=1
export RETENTION=120000
pushd .
cd $KAFKA_HOME
# The OpenTSDB ingestion topic. The OpenTSDB plugin reads metrics from this topic and writes them into HBase.
./bin/kafka-topics.sh --zookeeper $ZOOKEEP_URI --create --topic tsdb.metrics.binary --partitions 3 --replication-factor $REPL --config retention.ms=$RETENTION
# Accumulator topic. Metrics are counted infinitely and then written to tsdb.metrics.binary
./bin/kafka-topics.sh --zookeeper $ZOOKEEP_URI --create --topic tsdb.metrics.accumulator --partitions 3 --replication-factor $REPL --config retention.ms=$RETENTION
# Meter text topic. Metrics are assigned a key then written to tsdb.metrics.meter
./bin/kafka-topics.sh --zookeeper $ZOOKEEP_URI --create --topic tsdb.metrics.text.meter --partitions 3 --replication-factor $REPL --config retention.ms=$RETENTION

# Meter topic. Metrics are counted within a defined window to get a rate and then written to tsdb.metrics.binary
./bin/kafka-topics.sh --zookeeper $ZOOKEEP_URI --create --topic tsdb.metrics.meter --partitions 3 --replication-factor $REPL --config retention.ms=$RETENTION
# Delta topic. Metrics are computed as a delta between the current value and the prior and then written to tsdb.metrics.binary
./bin/kafka-topics.sh --zookeeper $ZOOKEEP_URI --create --topic tsdb.metrics.delta --partitions 3 --replication-factor $REPL --config retention.ms=$RETENTION
# Period Aggregation topic. Metrics are min/max/avg/count aggregated for a given window and then written to tsdb.metrics.binary
./bin/kafka-topics.sh --zookeeper $ZOOKEEP_URI --create --topic tsdb.metrics.pagg --partitions 3 --replication-factor $REPL --config retention.ms=$RETENTION
# Period Sticky Aggregation topic. Metrics are min/max/avg/count aggregated for a given window and then written to tsdb.metrics.binary
./bin/kafka-topics.sh --zookeeper $ZOOKEEP_URI --create --topic tsdb.metrics.psagg --partitions 3 --replication-factor $REPL --config retention.ms=$RETENTION
# Straight through topic. Metrics are written to tsdb.metrics.binary
./bin/kafka-topics.sh --zookeeper $ZOOKEEP_URI --create --topic tsdb.metrics.st --partitions 3 --replication-factor $REPL --config retention.ms=$RETENTION
# Metrics are examined to determine their type and then routed accordingly. Metrics are written to tsdb.metrics.binary
./bin/kafka-topics.sh --zookeeper $ZOOKEEP_URI --create --topic tsdb.metrics.directed --partitions 3 --replication-factor $REPL  --config retention.ms=$RETENTION
# Reads in JSON docs published by filebeat, extracts the message and routes (or drops) accordingly
./bin/kafka-topics.sh --zookeeper $ZOOKEEP_URI --create --topic tsdb.metrics.beats --partitions 3 --replication-factor $REPL --config retention.ms=$RETENTION

popd

