#!/usr/bin/env bash
#
# run-hive-query.sh
# Runs a HiveQL query over the WordCount output written by run-wordcount.sh.
#
set -euo pipefail

HIVE_CONTAINER="${HIVE_CONTAINER:-hive-server}"
INPUT_DIR="${1:-/test/output}"
READY_TIMEOUT="${READY_TIMEOUT:-300}"
QUERY_TIMEOUT="${QUERY_TIMEOUT:-180}"

log()  { echo -e "\033[1;34m[INFO]\033[0m  $*"; }
err()  { echo -e "\033[1;31m[ERROR]\033[0m $*" >&2; }

if ! docker ps --format '{{.Names}}' | grep -q "^${HIVE_CONTAINER}$"; then
  err "Container '${HIVE_CONTAINER}' is not running."
  exit 2
fi

# ---------------------------------------------------------------------------
# 1. Wait for HiveServer2 to be able to ACTUALLY serve queries.
#    TCP-open is not enough: the Thrift server accepts connections before the
#    metastore is loaded, causing queries to hang for minutes.
# ---------------------------------------------------------------------------
log "Waiting for HiveServer2 to be query-ready (timeout: ${READY_TIMEOUT}s)..."
elapsed=0
ready=0
while (( elapsed < READY_TIMEOUT )); do
    if docker exec "${HIVE_CONTAINER}" timeout 15 beeline \
            -u "jdbc:hive2://localhost:10000/;auth=noSasl" \
            -n root \
            --outputformat=tsv2 \
            -e "SELECT 1;" </dev/null 2>/dev/null | grep -qE '^1$'; then
        ready=1
        break
    fi
    sleep 10
    elapsed=$(( elapsed + 10 ))
done

if (( ready == 0 )); then
    err "HiveServer2 never became query-ready after ${READY_TIMEOUT}s."
    err "Dumping container logs (last 100 lines):"
    docker logs --tail=100 "${HIVE_CONTAINER}" 2>&1 | sed 's/^/    /' || true
    exit 2
fi
log "HiveServer2 is query-ready (after ${elapsed}s)."

# ---------------------------------------------------------------------------
# 2. Write the HiveQL to a file inside the container.
# ---------------------------------------------------------------------------
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

-- Simple projection, no ORDER BY (which would trigger a MapReduce job).
SELECT word, count FROM wordcount LIMIT 20;
EOF

# Print the file content for debugging in CI logs
log "Query file contents:"
docker exec "${HIVE_CONTAINER}" cat /tmp/query.hql | sed 's/^/    /'

# ---------------------------------------------------------------------------
# 3. Run the query with tsv2 output (streams rows, no buffering bugs).
# ---------------------------------------------------------------------------
log "Running HiveQL query over ${INPUT_DIR} (timeout: ${QUERY_TIMEOUT}s)..."

BEELINE_OPTS='-Djline.terminal=jline.UnsupportedTerminal -Djava.net.preferIPv4Stack=true'

if ! docker exec -i \
      -e HADOOP_CLIENT_OPTS="${BEELINE_OPTS}" \
      "${HIVE_CONTAINER}" \
      timeout "${QUERY_TIMEOUT}" \
      beeline \
        -u "jdbc:hive2://localhost:10000/;auth=noSasl" \
        -n root \
        --outputformat=tsv2 \
        --hiveconf hive.server2.enable.doAs=false \
        -f /tmp/query.hql </dev/null; then
    err "Hive query failed or timed out."
    err "Container logs (last 100 lines):"
    docker logs --tail=100 "${HIVE_CONTAINER}" 2>&1 | sed 's/^/    /' || true
    exit 4
fi

log "Hive query completed successfully."