#!/bin/bash

# This starts Emodb in Stash role.
# It requires Emodb to be running in one of the following roles
# - ./start.sh OR
# - ./start-clean.sh OR
# - ./start-main-role.sh 
# Starts the following servers locally:
# - EmoDB (ports 8080, 8081)
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

mvn verify -P init-cassandra,start-emodb-scan-role
