#!/usr/bin/env bash

# Docker Installation on Mac OS X: https://docs.docker.com/engine/installation/mac/
#
# Starts the following services locally (see configuration reference https://docs.docker.com/samples/library/zookeeper/):
# - ZooKeeper (port 2181)

docker run -p 2181:2181  -v $(pwd)/target/zookeeper:/data zookeeper:3.4.13