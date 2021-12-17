#/bin/bash

set -x
set -euo pipefail

git clone https://github.com/tikv/client-java.git
cd client-java
git reset --hard 655d63fbba047334ab884224e5db6510fe16a5aa
mvn install -DskipTests
cd ..
rm -Rf client-java
