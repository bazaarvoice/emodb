#!/bin/bash

#
# Starts the following server locally:
# - Cassandra (port 9160)
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

# First time around, initialize the Cassandra schema.
if [ ! -d target/cassandra/data/app_global ] ; then
    mvn -P init-cassandra
    sleep 1
fi

mvn cassandra:run
