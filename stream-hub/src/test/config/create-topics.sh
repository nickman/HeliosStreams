#!/bin/bash
export KAFKA_HOME=/home/nwhitehead/services/kafka/kafka_2.11-0.10.0.0
export ZOOKEEP_URI=localhost:2181
pushd .
cd $KAFKA_HOME
./bin/kafka-topics.sh --zookeeper $ZOOKEEP_URI --create --topic tsdb.metrics.binary --partitions 2 --replication-factor 2
./bin/kafka-topics.sh --zookeeper $ZOOKEEP_URI --create --topic tsdb.metrics.accumulator --partitions 2 --replication-factor 2
./bin/kafka-topics.sh --zookeeper $ZOOKEEP_URI --create --topic tsdb.metrics.st --partitions 2 --replication-factor 2


popd

