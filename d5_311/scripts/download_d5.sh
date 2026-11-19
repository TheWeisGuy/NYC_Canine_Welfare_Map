#!/usr/bin/env bash
# Download the D5 extract: 2025 NYC 311 "Animal in a Park" requests (selected columns)
# from NYC Open Data, dataset erm2-nwe9 (311 Service Requests from 2020 to Present).
#
# Usage: scripts/download_d5.sh [output.csv]
set -euo pipefail

OUT=${1:-"$(dirname "$0")/../data/raw/311_animal_in_park_2025.csv"}
API=https://data.cityofnewyork.us/resource/erm2-nwe9
WHERE="complaint_type='Animal in a Park' AND created_date between '2025-01-01T00:00:00' and '2025-12-31T23:59:59'"

mkdir -p "$(dirname "$OUT")"

# Row count the API reports for the same filter, to check the download is complete.
EXPECTED=$(curl -sfG "$API.csv" --data-urlencode "\$select=count(*)" --data-urlencode "\$where=$WHERE" | tail -1 | tr -d '"')

curl -sfG "$API.csv" \
  --data-urlencode "\$select=unique_key,created_date,complaint_type,descriptor,incident_zip,borough,status" \
  --data-urlencode "\$where=$WHERE" \
  --data-urlencode "\$order=unique_key" \
  --data-urlencode "\$limit=50000" \
  -o "$OUT"

ROWS=$(( $(wc -l < "$OUT") - 1 ))
echo "downloaded:  $OUT"
echo "date (UTC):  $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "bytes:       $(wc -c < "$OUT")"
echo "sha256:      $(sha256sum "$OUT" | cut -d' ' -f1)"
echo "data rows:   $ROWS (API count: $EXPECTED)"
[ "$ROWS" -eq "$EXPECTED" ] || { echo "row count mismatch" >&2; exit 1; }
