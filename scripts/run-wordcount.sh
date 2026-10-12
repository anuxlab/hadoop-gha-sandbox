#!/usr/bin/env bash
#
# run-wordcount.sh
# -----------------
# Runs a MapReduce WordCount job against the Hadoop cluster defined in
# docker-compose.yml. Verifies HDFS and YARN availability, uploads test
# data, runs the job, and prints the results.
#
# Usage:
#   ./scripts/run-wordcount.sh [input_dir] [output_dir]
#
# Defaults:
#   input_dir  = /test/input
#   output_dir = /test/output
#
# Exit codes:
#   0 - Success
#   1 - Missing dependency (docker)
#   2 - Cluster not reachable
#   3 - HDFS operation failed
#   4 - MapReduce job failed
#
set -euo pipefail

# ----- Configuration -------------------------------------------------------
NAMENODE_CONTAINER="${NAMENODE_CONTAINER:-namenode}"
INPUT_DIR="${1:-/test/input}"
OUTPUT_DIR="${2:-/test/output}"
HADOOP_VERSION="${HADOOP_VERSION:-3.2.1}"
EXAMPLES_JAR="/opt/hadoop-${HADOOP_VERSION}/share/hadoop/mapreduce/hadoop-mapreduce-examples-${HADOOP_VERSION}.jar"
NAMENODE_UI="${NAMENODE_UI:-http://localhost:9870}"
RESOURCEMANAGER_UI="${RESOURCEMANAGER_UI:-http://localhost:8088}"
MAX_WAIT_SECONDS="${MAX_WAIT_SECONDS:-180}"

# ----- Helpers -------------------------------------------------------------
log()  { echo -e "\033[1;34m[INFO]\033[0m  $*"; }
warn() { echo -e "\033[1;33m[WARN]\033[0m  $*"; }
err()  { echo -e "\033[1;31m[ERROR]\033[0m $*" >&2; }

# Wait until a URL responds (any HTTP status) or timeout.
wait_for_url() {
  local url="$1"
  local label="$2"
  local elapsed=0

  log "Waiting for ${label} at ${url} (timeout: ${MAX_WAIT_SECONDS}s)..."
  while ! curl -s -o /dev/null -w "%{http_code}" "${url}" | grep -qE '^[0-9]{3}$'; do
    if (( elapsed >= MAX_WAIT_SECONDS )); then
      err "${label} did not become reachable within ${MAX_WAIT_SECONDS}s."
      return 2
    fi
    sleep 5
    elapsed=$(( elapsed + 5 ))
  done
  log "${label} is up (after ${elapsed}s)."
}

# ----- Pre-flight checks ---------------------------------------------------
if ! command -v docker >/dev/null 2>&1; then
  err "'docker' is not installed or not on PATH."
  exit 1
fi

if ! docker ps --format '{{.Names}}' | grep -q "^${NAMENODE_CONTAINER}$"; then
  err "Container '${NAMENODE_CONTAINER}' is not running. Start the cluster first:"
  err "  docker-compose up -d"
  exit 2
fi

# ----- Cluster readiness ---------------------------------------------------
wait_for_url "${NAMENODE_UI}" "NameNode UI"
wait_for_url "${RESOURCEMANAGER_UI}" "ResourceManager UI"

# ----- Prepare HDFS paths --------------------------------------------------
log "Preparing HDFS directories..."
docker exec "${NAMENODE_CONTAINER}" hdfs dfs -rm -r -f "${OUTPUT_DIR}" >/dev/null 2>&1 || true
docker exec "${NAMENODE_CONTAINER}" hdfs dfs -mkdir -p "${INPUT_DIR}" || {
  err "Failed to create HDFS input directory '${INPUT_DIR}'."
  exit 3
}

# ----- Upload test data ----------------------------------------------------
log "Uploading test data to HDFS..."
docker exec "${NAMENODE_CONTAINER}" bash -c '
  cat > /tmp/wordcount-input.txt <<EOF
Hello Hadoop
Hello MapReduce
Hello Docker
Hadoop MapReduce Docker
Big Data with Hadoop and YARN
WordCount Example in Action
EOF
'
docker exec "${NAMENODE_CONTAINER}" hdfs dfs -put -f /tmp/wordcount-input.txt "${INPUT_DIR}/" || {
  err "Failed to upload test data to '${INPUT_DIR}'."
  exit 3
}

log "Input files in ${INPUT_DIR}:"
docker exec "${NAMENODE_CONTAINER}" hdfs dfs -ls "${INPUT_DIR}"

# ----- Run MapReduce WordCount --------------------------------------------
log "Running MapReduce WordCount job..."
if ! docker exec "${NAMENODE_CONTAINER}" hadoop jar "${EXAMPLES_JAR}" \
        wordcount "${INPUT_DIR}" "${OUTPUT_DIR}"; then
  err "MapReduce job failed. Check YARN logs:"
  err "  docker exec ${NAMENODE_CONTAINER} yarn logs -applicationId <appId>"
  exit 4
fi

# ----- Verify and print results -------------------------------------------
log "Verifying output..."
if ! docker exec "${NAMENODE_CONTAINER}" hdfs dfs -test -e "${OUTPUT_DIR}/_SUCCESS"; then
  err "Output marker _SUCCESS not found in ${OUTPUT_DIR}."
  exit 4
fi

echo
echo "================= WordCount Results ================="
docker exec "${NAMENODE_CONTAINER}" hdfs dfs -cat "${OUTPUT_DIR}/part-r-00000"
echo "====================================================="
echo

log "WordCount job completed successfully."
exit 0
