#!/bin/bash
echo 'clientPort=2181' > zookeeper.properties
echo 'dataDir=/var/lib/zookeeper/data' >> zookeeper.properties
echo 'dataLogDir=/var/lib/zookeeper/log' >> zookeeper.properties
zookeeper-server-start zookeeper.properties &
export KAFKA_ZOOKEEPER_CONNECT="localhost:2181"
export KAFKA_ADVERTISED_LISTENERS="$1"
. /etc/confluent/docker/bash-config
/etc/confluent/docker/configure
/etc/confluent/docker/launch