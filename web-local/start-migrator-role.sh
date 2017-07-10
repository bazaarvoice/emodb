#!/bin/bash

# This starts Emodb in Migrator role.
# It requires Emodb to be running in one of the following roles
# - ./start.sh OR
# - ./start-clean.sh OR
# - ./start-main-role.sh
# Starts the following servers locally:
# - EmoDB MAIN (ports 8080, 8081)
# = EmoDB Migrator (ports 8082, 8083)
# - Cassandra (port 9164)
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

mvn verify -P init-cassandra,start-emodb-migrator-role
