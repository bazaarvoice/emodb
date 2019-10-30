#!/bin/bash

#
# Starts the following servers locally:
# - EmoDB Web (ports 8080, 8081)
# - Cassandra (port 9160)
# - ZooKeeper (port 2181)
# - EmoDB Megabus (ports 8082, 8083)
# - Kafka (port 9092)
#
# Cassandra:
#
# The first time this is run, Cassandra will be initialized with a default
# schema and an empty # data set.  Data will be stored in "target/cassandra".
# On subsequent runs where "target/cassandra" already exists, the Cassandra
# schema and data will not be modified.
#
# Once the server is running you can access the Cassandra command line
# interface using the following commands:
#
#   cd target/cassandra/bin
#   java -jar cassandra-cli.jar
#
# Kafka
#
# We assume Kafka is already installed by installing the
# Confluent OSS Platform. Install instructions can be found at
# https://docs.confluent.io/3.1.1/installation.html
# Or, if you're on OS X, you can `brew install confluent-oss`

[[ -x $(which kafka-server-start) ]] || {
    echo "Kafka not found. Please install and try again."
    echo "If you already installed and it still isn't working,"
    echo "make sure that Zookeeper and Kafka are on your PATH."
    echo "You may have to source your environment/profile for"
    echo "it to take effect."
    exit 2
}

function cleanup {
    kafka-server-stop
    until ! $(ps ax | grep -iq -e 'kafka\.Kafka' -e 'io\.confluent\.support\.metrics\.SupportedKafka'); do sleep 2 ; done
    zookeeper-server-stop
    until ! $(ps ax | grep java | grep -iq QuorumPeerMain) ; do sleep 2 ; done
    exit
}

# capture Ctrl+C and shutdown backends automatically
trap 'cleanup' SIGINT SIGTERM

# start Confluent-provided ZK
zookeeper-server-start -daemon /usr/local/etc/kafka/zookeeper.properties
echo -n "Starting ZK..."
until nc -z localhost 2181 ; do
    echo -n "."
    sleep 1
done

echo

# start Confluent-provided Kafka
kafka-server-start -daemon /usr/local/etc/kafka/server.properties
echo -n "Starting Kafka..."
until nc -z localhost 9092 ; do
    echo -n "."
    sleep 3
done

echo

# start emodb-web, cassandra, emodb-megabus
mvn verify -P init-cassandra,start-emodb-megabus-role
