#!/usr/bin/env bash
# Build target/d1-dog-licensing.jar on the cluster without Maven (javac + jar, as in the labs).
# Usage: scripts/build_javac.sh        (run from anywhere)
set -euo pipefail
cd "$(dirname "$0")/.."
rm -rf target/classes && mkdir -p target/classes
javac -source 8 -target 8 -nowarn -classpath "$(yarn classpath)" -d target/classes \
  $(find src/main/java -name '*.java')
jar -cf target/d1-dog-licensing.jar -C target/classes .
ls -l target/d1-dog-licensing.jar
