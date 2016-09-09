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
# Note: For local testing, we have enabled anonymous access. In production you should be using API Keys. Read more about it in "API Keys" section.
#
#   curl localhost:8081/ping
#   curl localhost:8080/sor/1/_table
#
# For production, use the web jar not web-local, and also you shouldn't be providing cassandra.yaml or -z switch since you wouldn't want to start a local cassandra or zookeeper
# java -jar bin/emodb-web.jar server conf/config-local.yaml conf/config-ddl-local.yaml
#

#traverse all links to arrive at the real directory of the script.
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

pushd $DIR
java -jar bin/emodb-web-local-*.jar server conf/config-local.yaml conf/config-ddl-local.yaml conf/cassandra.yaml -z
popd
