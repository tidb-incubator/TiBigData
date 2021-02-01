#!/bin/bash

set -x
set -euo pipefail

export TIDB_URL="jdbc:tidb://127.0.0.1:4000?user=root&password="
mvn clean test -am -pl jdbc
mvn clean test -am -pl flink/flink-1.11
mvn clean test -am -pl flink/flink-1.12
mvn clean test -am -pl prestodb