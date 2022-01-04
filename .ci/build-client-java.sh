#/bin/bash

set -x
set -euo pipefail

if [ ! -d "client-java" ]; then
  git clone https://github.com/tikv/client-java.git
fi
cd client-java
git reset --hard 1405fe7c800ba319cf3b13c943939b0012e5b899
mvn clean install -Dmaven.test.skip=true
cd ..
rm -Rf client-java
