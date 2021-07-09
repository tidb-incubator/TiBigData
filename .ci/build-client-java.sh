#!/bin/bash

set -x
set -euo pipefail

git clone https://github.com/tikv/client-java.git
cd client-java
git reset --hard 309c521e9842ee9717ce12cd54513641fdb0b8eb
mvn install -DskipTests
cd ..
rm -Rf client-java
