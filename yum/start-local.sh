#!/bin/bash

#
# Starts the following servers locally:
# - EmoDB (ports 8080, 8081)
# - Cassandra (port 9160)
# - ZooKeeper (port 2181)
#
# The first time this is run, Cassandra will be initialized with a default
# schema and an empty # data set.  Data will be stored in "bin/data".
# On subsequent runs where "bin/data/" already exists, the Cassandra
# schema and data will not be modified.
#
# Once the server is running you can access Emodb using the following commands:
#
#   curl localhost:8081/ping
#   curl localhost:8080/sor/1/_table
#
# For production, use the web jar not web-local, and also you shouldn't be providing cassandra.yaml or -z switch since you wouldn't want to start a local cassandra or zookeeper
# java -jar bin/emodb-web.jar server conf/config-local.yaml conf/config-ddl-local.yaml
#

java -jar bin/emodb-web-local-*.jar server conf/config-local.yaml conf/config-ddl-local.yaml conf/cassandra.yaml -z
