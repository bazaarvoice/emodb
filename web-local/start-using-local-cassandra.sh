#!/bin/bash

# Relies on a locally started cassandra. Good for testing new versions of Cassandra with Emodb.
# Make sure the following config settings on your local cassandra.yaml are set as follows:
# - start_rpc: true
# - partitioner: org.apache.cassandra.dht.ByteOrderedPartitioner

# Starts the following services locally:
# - EmoDB (ports 8080, 8081)
# - ZooKeeper (port 2181)
#
# Cassandra will be initialized with a default schema and an empty data set.

mvn clean verify -P start-emodb
