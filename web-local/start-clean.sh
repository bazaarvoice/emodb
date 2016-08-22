#!/bin/bash

#
# Starts the following servers locally:
# - EmoDB (ports 8080, 8081)
# - Cassandra (port 9160)
# - ZooKeeper (port 2181)
#
# Cassandra will be initialized with a default schema and an empty data set.
# Data will stored in "target/cassandra".
#
# Once the server is running you can access the Cassandra command line
# interface using the following commands:
#
#   cd target/cassandra/bin
#   java -jar cassandra-cli.jar
#

mvn clean verify -P init-cassandra,start-emodb
