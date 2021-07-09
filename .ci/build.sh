#!/bin/bash

set -x
set -euo pipefail

.ci/build-client-java.sh

mvn clean compile -am -pl ticdc
mvn clean compile -am -pl flink/flink-1.11
mvn clean compile -am -pl flink/flink-1.12
mvn clean compile -am -pl flink/flink-1.13
mvn clean compile -am -pl mapreduce/mapreduce-base
mvn clean compile -am -pl prestodb
mvn clean compile -am -pl jdbc
