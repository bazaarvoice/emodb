#!/bin/bash

# Requires to start start.sh or start-clean.sh before running this script
# Starts a new data center locally: datacenter2
# This does not start a new cassandra cluster, but simply creates a new emo cluster in a different
# data center.


java -jar web/target/emodb-web-*.jar server web-local/configs/config-local-dc2.yaml web-local/configs/config-ddl-local-dc2.yaml
