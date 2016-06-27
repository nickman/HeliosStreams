#!/bin/bash
export KAFKA_HOME=/home/nwhitehead/services/kafka/kafka_2.11-0.10.0.0
export ZOOKEEP_URI=localhost:2181
pushd .
cd $KAFKA_HOME
./bin/kafka-topics.sh --zookeeper $ZOOKEEP_URI --delete --topic tsdb.metrics.binary
./bin/kafka-topics.sh --zookeeper $ZOOKEEP_URI --delete --topic tsdb.metrics.accumulator
./bin/kafka-topics.sh --zookeeper $ZOOKEEP_URI --delete --topic tsdb.metrics.st


popd

