#!/usr/bin/env bash
# Download the D1 NYC Dog Licensing Dataset (NYC Open Data nu7n-tubp) as the portal CSV export.
# The team baseline is the 2026-10-30 snapshot (64,518,360 bytes,
# MD5 9601d48d2487f3de628ed38f5b7771b2); a later download can differ.
#
# Usage: scripts/download_d1.sh [output.csv]
set -euo pipefail

OUT=${1:-"$(dirname "$0")/../data/raw/NYC_Dog_Licensing_Dataset.csv"}
URL="https://data.cityofnewyork.us/api/views/nu7n-tubp/rows.csv?accessType=DOWNLOAD"

mkdir -p "$(dirname "$OUT")"
curl -sfL "$URL" -o "$OUT"

echo "downloaded:  $OUT"
echo "date (UTC):  $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "bytes:       $(wc -c < "$OUT")"
echo "md5:         $(md5sum "$OUT" | cut -d' ' -f1)"
echo "data rows:   $(( $(wc -l < "$OUT") - 1 ))"
