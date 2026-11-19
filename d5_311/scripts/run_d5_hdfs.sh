#!/usr/bin/env bash
# Upload the D5 311 extract and D4 ZCTA reference to HDFS and run the D5 MapReduce pipeline.
#
# Usage: scripts/run_d5_hdfs.sh <local-311-extract.csv> <local-or-hdfs-zcta-reference> [hdfs-base]
#   hdfs-base defaults to /user/$USER/pawprint
# If the reference argument starts with hdfs:// or / and exists on HDFS (e.g. Member 3's
# cleaned reference), it is used in place; otherwise the local file is uploaded.
set -euo pipefail

EXTRACT=${1:?local 311 extract CSV}
ZCTA=${2:?ZCTA reference (local file or HDFS path)}
BASE=${3:-/user/$USER/pawprint}
JAR="$(dirname "$0")/../target/d5-311.jar"

RAW=$BASE/raw/d5_311
REF=$BASE/raw/d4_zcta
OUT=$BASE/d5_311

hdfs dfs -mkdir -p "$RAW" "$REF"
hdfs dfs -put -f "$EXTRACT" "$RAW/"
if hdfs dfs -test -e "$ZCTA" 2>/dev/null; then
  REF=$ZCTA
else
  hdfs dfs -put -f "$ZCTA" "$REF/"
fi

hadoop jar "$JAR" edu.nyu.pawprint.d5.D5Pipeline -D d5.overwrite=true "$RAW" "$REF" "$OUT"

echo "== cleaning reconciliation"; hdfs dfs -cat "$OUT/clean/reconciliation.txt"
echo "== aggregation reconciliation"; hdfs dfs -cat "$OUT/aggregate/reconciliation.txt"
