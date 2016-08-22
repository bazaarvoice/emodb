#!/bin/bash

# Requires to start start.sh or start-clean.sh before running this script
# Starts a new data center locally: datacenter2
# This does not start a new cassandra cluster, but simply creates a new emo cluster in a different
# data center.


cd ..
java -jar web/target/emodb-web-*.jar server web-local/config-dc2.yaml
