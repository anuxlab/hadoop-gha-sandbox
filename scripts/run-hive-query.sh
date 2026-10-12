#!/usr/bin/env bash
#
# run-hive-query.sh
# Runs a HiveQL query over the WordCount output written by run-wordcount.sh.
#
set -euo pipefail

HIVE_CONTAINER="${HIVE_CONTAINER:-hive-server}"
INPUT_DIR="${1:-/test/output}"
MAX_WAIT_SECONDS="${MAX_WAIT_SECONDS:-300}"
QUERY_TIMEOUT="${QUERY_TIMEOUT:-300}"

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

log "Writing HiveQL to /tmp/query.hql inside the container..."
docker exec -i "${HIVE_CONTAINER}" bash -c "cat > /tmp/query.hql" <<EOF
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
EOF

log "Running HiveQL query over ${INPUT_DIR} (timeout: ${QUERY_TIMEOUT}s)..."

# Use timeout + </dev/null to prevent beeline from ever hanging
if ! docker exec -i "${HIVE_CONTAINER}" timeout "${QUERY_TIMEOUT}" \
      beeline \
        -u "jdbc:hive2://localhost:10000/default" \
        -n root \
        --silent=true \
        --showHeader=true \
        --outputformat=table \
        --hiveconf hive.server2.enable.doAs=false \
        --hiveconf hive.exec.mode.local.auto=true \
        -f /tmp/query.hql </dev/null; then
    err "Hive query failed or timed out."
    err "Container logs (last 100 lines):"
    docker logs --tail=100 "${HIVE_CONTAINER}" 2>&1 | sed 's/^/    /' || true
    exit 4
fi

log "Hive query completed successfully."