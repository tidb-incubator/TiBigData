#/bin/bash

set -x
set -euo pipefail

git clone https://github.com/tikv/client-java.git
cd client-java
git reset --hard 1405fe7c800ba319cf3b13c943939b0012e5b899
mvn install -DskipTests
cd ..
rm -Rf client-java
