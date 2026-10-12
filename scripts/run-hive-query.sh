#!/usr/bin/env bash
#
# run-hive-query.sh
# Runs a HiveQL query over the WordCount output written by run-wordcount.sh.
#
set -euo pipefail

HIVE_CONTAINER="${HIVE_CONTAINER:-hive-server}"
INPUT_DIR="${1:-/test/output}"
MAX_WAIT_SECONDS="${MAX_WAIT_SECONDS:-300}"

log()  { echo -e "\033[1;34m[INFO]\033[0m  $*"; }
err()  { echo -e "\033[1;31m[ERROR]\033[0m $*" >&2; }

if ! docker ps --format '{{.Names}}' | grep -q "^${HIVE_CONTAINER}$"; then
  err "Container '${HIVE_CONTAINER}' is not running."
  exit 2
fi

log "Waiting for HiveServer2 on port 10000 (timeout: ${MAX_WAIT_SECONDS}s)..."
elapsed=0
until docker exec "${HIVE_CONTAINER}" bash -c '</dev/tcp/localhost/10000' 2>/dev/null; do
  if (( elapsed >= MAX_WAIT_SECONDS )); then
    err "HiveServer2 not ready after ${MAX_WAIT_SECONDS}s."
    exit 2
  fi
  sleep 5
  elapsed=$(( elapsed + 5 ))
done
log "HiveServer2 is up (after ${elapsed}s)."

log "Running HiveQL query over ${INPUT_DIR}..."
docker exec "${HIVE_CONTAINER}" beeline \
  -u "jdbc:hive2://localhost:10000/default" \
  -n root \
  --silent=true \
  -e "
    DROP TABLE IF EXISTS wordcount;
    CREATE EXTERNAL TABLE wordcount (
      word STRING,
      count INT
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '${INPUT_DIR}';

    SELECT word, count
    FROM wordcount
    ORDER BY count DESC, word ASC
    LIMIT 10;
  "

log "Hive query completed successfully."
