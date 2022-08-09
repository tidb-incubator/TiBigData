#!/bin/bash

set -x
set -euo pipefail

# see maven plugin
# git submodule update --init --recursive

mvn clean compile test -B -am -pl ticdc
mvn clean compile test -B -am -pl flink/flink-1.11
mvn clean compile test -B -am -pl flink/flink-1.12
mvn clean compile test -B -am -pl flink/flink-1.13
mvn clean compile test -B -am -pl flink/flink-1.14
mvn clean compile test -B -am -pl mapreduce/mapreduce-base
mvn clean compile test -B -am -pl prestodb
mvn clean compile test -B -am -pl jdbc
mvn clean compile test -B -am -pl hive/hive-2.2.0
mvn clean compile test -B -am -pl hive/hive-2.3.6
mvn clean compile test -B -am -pl hive/hive-3.1.2

export JAVA_HOME=/home/jenkins/agent/lib/jdk-11.0.12
mvn clean compile test -B -am -pl prestosql
mvn clean compile test -B -am -pl trino
