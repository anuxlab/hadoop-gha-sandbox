#!/usr/bin/env bash
# Standalone Spark -> HDFS integration test; assumes MapReduce fixture was uploaded.
set -Eeuo pipefail
SPARK_SERVICE="${SPARK_SERVICE:-spark-master}"
MAX_WAIT_SECONDS="${MAX_WAIT_SECONDS:-180}"
OUTPUT_PATH="/test/spark-output"
log() { printf '[INFO] %s\n' "$*"; }
err() { printf '[ERROR] %s\n' "$*" >&2; }
compose() { docker compose "$@"; }

compose ps --services --filter status=running | grep -Fxq "$SPARK_SERVICE" || { err "Compose service '$SPARK_SERVICE' is not running."; exit 2; }

log "Waiting for Spark master and at least ${EXPECTED_SPARK_WORKERS:-1} registered worker(s)..."
elapsed=0
expected_workers="${EXPECTED_SPARK_WORKERS:-1}"
while (( elapsed < MAX_WAIT_SECONDS )); do
  worker_json=$(compose exec -T "$SPARK_SERVICE" curl -fsS http://localhost:8080/json/ 2>/dev/null || true)
  worker_count=$(printf '%s' "$worker_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(sum(1 for w in d.get("workers", []) if w.get("alive")))' 2>/dev/null || true)
  if [[ "$worker_count" =~ ^[0-9]+$ ]] && (( worker_count >= expected_workers )); then
    log "Spark reports ${worker_count} live worker(s)."
    break
  fi
  sleep 5
  elapsed=$((elapsed + 5))
done
if (( elapsed >= MAX_WAIT_SECONDS )); then
  err "Spark worker registration timed out (expected ${expected_workers}, got ${worker_count:-unknown})."
  compose logs --no-color --tail=100 spark-master spark-worker || true
  exit 3
fi

compose exec -T namenode hdfs dfs -rm -r -f "$OUTPUT_PATH" >/dev/null 2>&1 || true
log 'Checking Spark and Python runtimes...'
compose exec -T "$SPARK_SERVICE" /spark/bin/spark-submit --version
compose exec -T "$SPARK_SERVICE" python3 --version

log 'Submitting Spark WordCount to the standalone cluster...'
compose exec -T "$SPARK_SERVICE" /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --driver-memory 512m \
  --executor-memory 512m \
  --conf spark.executor.cores=1 \
  --conf spark.cores.max=1 \
  --conf spark.driver.host=spark-master \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  --conf spark.hadoop.dfs.replication="${EXPECTED_REPLICATION:-1}" \
  /spark/jobs/wordcount.py

compose exec -T namenode hdfs dfs -test -e "${OUTPUT_PATH}/_SUCCESS" || { err "Spark output marker missing at ${OUTPUT_PATH}/_SUCCESS."; exit 4; }

mr_file=$(mktemp)
spark_file=$(mktemp)
expected_file=$(mktemp)
trap 'rm -f "$mr_file" "$spark_file" "$expected_file"' EXIT
compose exec -T namenode hdfs dfs -cat /test/output/part-r-* | awk 'NF >= 2 {print $1 "\t" $2}' | LC_ALL=C sort > "$mr_file"
compose exec -T namenode hdfs dfs -cat "${OUTPUT_PATH}/part-*" | awk 'NF >= 2 {print $1 "\t" $2}' | LC_ALL=C sort > "$spark_file"
if ! diff -u "$mr_file" "$spark_file"; then
  err 'Spark output differs from MapReduce output.'
  exit 5
fi
log 'Spark and MapReduce outputs match exactly.'
