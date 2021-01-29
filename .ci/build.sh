#!/bin/bash

set -x
set -euo pipefail

mvn clean compile -am -pl flink/flink-1.12
mvn clean compile -am -pl prestodb
mvn clean compile -am -pl jdbc