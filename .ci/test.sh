#!/bin/bash

set -x
set -euo pipefail

.ci/build-client-java.sh

export TIDB_HOST="127.0.0.1"
export TIDB_PORT="4000"
export TIDB_USER="root"
export TIDB_PASSWORD=""
mvn clean test -am -pl jdbc
mvn clean test -am -pl ticdc
mvn clean test -am -pl flink/flink-1.11
mvn clean test -am -pl flink/flink-1.12
mvn clean test -am -pl flink/flink-1.13
mvn clean test -am -pl mapreduce/mapreduce-base
mvn clean test -am -pl prestodb
