#!/bin/bash

set -x
set -euo pipefail

git clone https://github.com/tikv/client-java.git
cd client-java
git reset --hard faf0cb435d5d796692c4d986ad7478d87f3cf19a
mvn install -DskipTests
cd ..
rm -Rf client-java
