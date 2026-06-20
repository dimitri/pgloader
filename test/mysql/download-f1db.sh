#!/bin/bash
# Download the Ergast F1DB dataset (2024) and prepare it for pgloader tests.
# The dataset is hosted at RaceOptiData as a free sample of the last official
# public Ergast release under CC BY-SA 4.0.
set -euo pipefail

DOWNLOAD_URL="https://raceoptidatapublicfiles.blob.core.windows.net/ergast2024/ergast_2024.zip"
OUTPUT_DIR="$(cd "$(dirname "$0")/../data" && pwd)"
OUTPUT_FILE="$OUTPUT_DIR/f1db.sql"
TMPDIR=$(mktemp -d)

echo "Downloading Ergast F1DB 2024 dataset..."
curl -sL -o "$TMPDIR/ergast_2024.zip" "$DOWNLOAD_URL"

echo "Extracting..."
unzip -q -o "$TMPDIR/ergast_2024.zip" ergast_2024.sql -d "$TMPDIR"

# Replace the database name from ergast_2024 to f1db
sed 's/`ergast_2024`/`f1db`/g; s/`ergast_2024`\.//g' "$TMPDIR/ergast_2024.sql" > "$OUTPUT_FILE"

rm -rf "$TMPDIR"

echo "Done. F1DB dataset saved to: $OUTPUT_FILE"
echo "Tables included: $(grep -c '^CREATE TABLE' "$OUTPUT_FILE")"
echo "Total size: $(du -h "$OUTPUT_FILE" | cut -f1)"
