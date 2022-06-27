#!/bin/bash

set -x
set -euo pipefail

# see maven plugin
# git submodule update --init --recursive

mvn clean compile test -B -am -pl ticdc -DskipIntegrationTests=true
mvn clean compile test -B -am -pl flink/flink-1.11 -DskipIntegrationTests=true
mvn clean compile test -B -am -pl flink/flink-1.12 -DskipIntegrationTests=true
mvn clean compile test -B -am -pl flink/flink-1.13 -DskipIntegrationTests=true
mvn clean compile test -B -am -pl flink/flink-1.14 -DskipIntegrationTests=true
mvn clean compile test -B -am -pl mapreduce/mapreduce-base -DskipIntegrationTests=true
mvn clean compile test -B -am -pl prestodb -DskipIntegrationTests=true
mvn clean compile test -B -am -pl jdbc -DskipIntegrationTests=true
mvn clean compile test -B -am -pl hive/hive-2.2.0 -DskipIntegrationTests=true
mvn clean compile test -B -am -pl hive/hive-2.3.6 -DskipIntegrationTests=true
mvn clean compile test -B -am -pl hive/hive-3.1.2 -DskipIntegrationTests=true

export JAVA_HOME=/home/jenkins/agent/lib/jdk-11.0.12
mvn clean compile test -B -am -pl prestosql -DskipIntegrationTests=true
mvn clean compile test -B -am -pl trino -DskipIntegrationTests=true
