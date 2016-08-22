#!/bin/bash

#
# Starts the following servers locally:
# - EmoDB (ports 7546, 7548)
# - Cassandra (port 9160)
# - ZooKeeper (port 2181)
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

mvn verify -P init-cassandra,start-jenkins-emodb
