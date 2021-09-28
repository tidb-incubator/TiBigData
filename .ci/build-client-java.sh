#/bin/bash

set -x
set -euo pipefail

git clone https://github.com/tikv/client-java.git
cd client-java
git reset --hard 28380512f3adacc1acb54daa381c4c71b1a1fa8f
mvn install -DskipTests
cd ..
rm -Rf client-java
