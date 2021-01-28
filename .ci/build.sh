#!/bin/bash

set -x
set -euo pipefail

mvn clean compile -am -pl flink
mvn clean compile -am -pl prestodb
mvn clean compile -am -pl jdbc