#!/usr/bin/env bash
# Upload the D1 dog licensing export and the D4 ZCTA reference to HDFS and run the D1 pipeline.
#
# Usage: scripts/run_d1_hdfs.sh <local-dog-licensing.csv> <local-or-hdfs-zcta-reference> [hdfs-base]
#   hdfs-base defaults to /user/$USER/pawprint (same layout as d5_311/scripts/run_d5_hdfs.sh)
# If the reference argument exists on HDFS (e.g. Member 3's cleaned reference or the file
# already used by D5), it is used in place; otherwise the local file is uploaded.
# Builds the jar with scripts/build_javac.sh if it does not exist yet.
set -euo pipefail

LICENSES=${1:?local dog licensing CSV}
ZCTA=${2:?ZCTA reference (local file or HDFS path)}
BASE=${3:-/user/$USER/pawprint}
HERE="$(cd "$(dirname "$0")" && pwd)"
JAR="$HERE/../target/d1-dog-licensing.jar"
[ -f "$JAR" ] || "$HERE/build_javac.sh"

RAW=$BASE/raw/d1_dogs
REF=$BASE/raw/d4_zcta
OUT=$BASE/d1_dogs

hdfs dfs -mkdir -p "$RAW" "$REF"
hdfs dfs -put -f "$LICENSES" "$RAW/"
if hdfs dfs -test -e "$ZCTA" 2>/dev/null; then
  REF=$ZCTA
else
  hdfs dfs -put -f "$ZCTA" "$REF/"
fi

hadoop jar "$JAR" edu.nyu.pawprint.d1.D1Pipeline -D d1.overwrite=true -D d1.extract.year=2026 \
  "$RAW" "$REF" "$OUT"

echo "== cleaning reconciliation"; hdfs dfs -cat "$OUT/clean/reconciliation.txt"
echo "== aggregation reconciliation"; hdfs dfs -cat "$OUT/aggregate/reconciliation.txt"
