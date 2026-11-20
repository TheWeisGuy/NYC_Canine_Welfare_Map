#!/usr/bin/env bash
# Run the D1 pipeline on this machine with Hadoop's LocalJobRunner (no cluster needed),
# to check the jobs on the real extract before running on HDFS.
#
# Usage: scripts/run_local.sh [extract.csv] [zcta.csv] [output-dir]
# Needs Maven and a JDK 8/11/17 (set JAVA_HOME; Hadoop 3.3 does not run on JDK 23+).
set -euo pipefail
cd "$(dirname "$0")/.."

IN=${1:-data/raw/NYC_Dog_Licensing_Dataset.csv}
REF=${2:-data/raw/d4_zcta.csv}
OUT=${3:-data/local_out}

# Hadoop 3.3 fails on JDK 23+ ("getSubject is not supported"). If the current java is
# too new, switch to an installed JDK 17, 11 or 8.
java_major() { "$1" -version 2>&1 | head -1 | sed -E 's/.*version "(1\.)?([0-9]+).*/\2/'; }
JAVA_BIN=${JAVA_HOME:+$JAVA_HOME/bin/}java
if [ "$(java_major "$JAVA_BIN")" -ge 23 ]; then
  for v in 17 11 8; do
    for d in /usr/lib/jvm/java-$v-openjdk* /usr/lib/jvm/temurin-$v* /Library/Java/JavaVirtualMachines/*-$v*/Contents/Home; do
      if [ -x "$d/bin/java" ]; then export JAVA_HOME=$d; break 2; fi
    done
  done
  if [ "$(java_major "${JAVA_HOME:+$JAVA_HOME/bin/}java")" -ge 23 ]; then
    echo "Hadoop 3.3 needs JDK 8, 11 or 17; install one or set JAVA_HOME to it." >&2
    exit 1
  fi
  echo "Using JAVA_HOME=$JAVA_HOME (default java is too new for Hadoop 3.3)"
fi
export PATH="$JAVA_HOME/bin:$PATH"

mvn -q -B -DskipTests package
mvn -q -B dependency:build-classpath -Dmdep.outputFile=target/classpath.txt -Dmdep.includeScope=provided

java -cp "target/d1-dog-licensing.jar:$(cat target/classpath.txt)" \
  -Dlog4j.configuration=file:/dev/null \
  edu.nyu.pawprint.d1.D1Pipeline \
  -D mapreduce.framework.name=local -D fs.defaultFS=file:/// -D d1.overwrite=true \
  "$IN" "$REF" "$OUT"
