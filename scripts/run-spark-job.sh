#!/usr/bin/env bash
#
# run-spark-job.sh
# Submits the PySpark WordCount job to the standalone Spark cluster.
#
set -euo pipefail

SPARK_MASTER_CONTAINER="${SPARK_MASTER_CONTAINER:-spark-master}"
JOB_PATH="${JOB_PATH:-/spark/jobs/wordcount.py}"
MAX_WAIT_SECONDS="${MAX_WAIT_SECONDS:-180}"

log()  { echo -e "\033[1;34m[INFO]\033[0m  $*"; }
err()  { echo -e "\033[1;31m[ERROR]\033[0m $*" >&2; }

if ! docker ps --format '{{.Names}}' | grep -q "^${SPARK_MASTER_CONTAINER}$"; then
  err "Container '${SPARK_MASTER_CONTAINER}' is not running."
  exit 2
fi

log "Waiting for Spark Master UI on port 8080 (timeout: ${MAX_WAIT_SECONDS}s)..."
elapsed=0
until docker exec "${SPARK_MASTER_CONTAINER}" bash -c '</dev/tcp/localhost/8080' 2>/dev/null; do
  if (( elapsed >= MAX_WAIT_SECONDS )); then
    err "Spark Master not ready after ${MAX_WAIT_SECONDS}s."
    exit 2
  fi
  sleep 5
  elapsed=$(( elapsed + 5 ))
done
log "Spark Master is up (after ${elapsed}s)."

log "Submitting PySpark job..."
docker exec "${SPARK_MASTER_CONTAINER}" \
  /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  --conf spark.executor.memory=512m \
  --conf spark.driver.memory=512m \
  "${JOB_PATH}"

log "Spark job completed successfully."
