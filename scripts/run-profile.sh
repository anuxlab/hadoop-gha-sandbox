#!/usr/bin/env bash
# Run one named integration profile locally or from any Docker-capable CI runner.
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

profile="${1:-}"
if [[ -z "$profile" ]]; then
  echo "Usage: $0 {core|hive|spark|full|distributed|scale|resilience}" >&2
  exit 2
fi

case "$profile" in
  core)
    export COMPOSE_FILE='docker-compose.yml'
    export EXPECTED_DATANODES="${EXPECTED_DATANODES:-1}"
    export EXPECTED_NODEMANAGERS="${EXPECTED_NODEMANAGERS:-1}"
    export EXPECTED_SPARK_WORKERS="${EXPECTED_SPARK_WORKERS:-1}"
    export EXPECTED_REPLICATION="${EXPECTED_REPLICATION:-1}"
    ;;
  hive)
    export COMPOSE_FILE='docker-compose.yml:docker-compose.hive.yml'
    export EXPECTED_DATANODES="${EXPECTED_DATANODES:-1}"
    export EXPECTED_NODEMANAGERS="${EXPECTED_NODEMANAGERS:-1}"
    export EXPECTED_SPARK_WORKERS="${EXPECTED_SPARK_WORKERS:-1}"
    export EXPECTED_REPLICATION="${EXPECTED_REPLICATION:-1}"
    ;;
  spark)
    export COMPOSE_FILE='docker-compose.yml:docker-compose.spark.yml'
    export EXPECTED_DATANODES="${EXPECTED_DATANODES:-1}"
    export EXPECTED_NODEMANAGERS="${EXPECTED_NODEMANAGERS:-1}"
    export EXPECTED_SPARK_WORKERS="${EXPECTED_SPARK_WORKERS:-1}"
    export EXPECTED_REPLICATION="${EXPECTED_REPLICATION:-1}"
    ;;
  full)
    export COMPOSE_FILE='docker-compose.yml:docker-compose.hive.yml:docker-compose.spark.yml'
    export EXPECTED_DATANODES="${EXPECTED_DATANODES:-1}"
    export EXPECTED_NODEMANAGERS="${EXPECTED_NODEMANAGERS:-1}"
    export EXPECTED_SPARK_WORKERS="${EXPECTED_SPARK_WORKERS:-1}"
    export EXPECTED_REPLICATION="${EXPECTED_REPLICATION:-1}"
    ;;
  distributed|resilience)
    export COMPOSE_FILE='docker-compose.yml:docker-compose.spark.yml:docker-compose.distributed.yml'
    export EXPECTED_DATANODES="${EXPECTED_DATANODES:-2}"
    export EXPECTED_NODEMANAGERS="${EXPECTED_NODEMANAGERS:-2}"
    export EXPECTED_SPARK_WORKERS="${EXPECTED_SPARK_WORKERS:-2}"
    export EXPECTED_REPLICATION="${EXPECTED_REPLICATION:-2}"
    ;;
  scale)
    export COMPOSE_FILE='docker-compose.yml:docker-compose.spark.yml:docker-compose.distributed.yml:docker-compose.scale.yml'
    export EXPECTED_DATANODES="${EXPECTED_DATANODES:-2}"
    export EXPECTED_NODEMANAGERS="${EXPECTED_NODEMANAGERS:-2}"
    export EXPECTED_SPARK_WORKERS="${EXPECTED_SPARK_WORKERS:-2}"
    export EXPECTED_REPLICATION="${EXPECTED_REPLICATION:-2}"
    export SPARK_TOTAL_CORES="${SPARK_TOTAL_CORES:-2}"
    ;;
  *)
    echo "Unknown profile '$profile'. Use core, hive, spark, full, distributed, scale, or resilience." >&2
    exit 2
    ;;
esac

export MAX_WAIT_SECONDS="${MAX_WAIT_SECONDS:-180}"
export READY_TIMEOUT="${READY_TIMEOUT:-360}"
startup_timeout="${STARTUP_TIMEOUT:-450}"
pull_images="${PULL_IMAGES:-1}"
keep_on_failure="${KEEP_CONTAINERS_ON_FAILURE:-0}"
timestamp="$(date -u +%Y%m%d-%H%M%S)"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-hadoop-gha-${profile}-${timestamp}}"
artifact_dir="${ARTIFACT_DIR:-artifacts/${profile}-${timestamp}}"
mkdir -p "$artifact_dir"

log() { printf '[profile:%s] %s\n' "$profile" "$*"; }

collect_diagnostics() {
  local rc=$?
  set +e
  docker compose ps -a > "$artifact_dir/compose-ps.txt" 2>&1
  docker compose logs --no-color > "$artifact_dir/compose-logs.txt" 2>&1
  docker compose config > "$artifact_dir/compose-config.yml" 2>&1
  if [[ "$rc" -ne 0 && "$keep_on_failure" == '1' ]]; then
    log "Profile failed (exit ${rc}); containers and volumes are left running for inspection."
    log "Inspect with: COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME} COMPOSE_FILE='${COMPOSE_FILE}' docker compose ps -a"
    log "Tail logs with: COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME} COMPOSE_FILE='${COMPOSE_FILE}' docker compose logs --tail=200"
    log "Clean up with: COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME} COMPOSE_FILE='${COMPOSE_FILE}' docker compose down -v --remove-orphans"
  else
    docker compose down -v --remove-orphans
  fi
  log "Diagnostics saved under ${artifact_dir}/"
  return "$rc"
}
trap collect_diagnostics EXIT

log "Using Compose files: ${COMPOSE_FILE}"
log 'Running parser/regression checks...'
bash scripts/test-readiness-parsers.sh
log 'Validating resolved Compose configuration...'
docker compose config --quiet
if [[ "$pull_images" == '1' ]]; then
  log 'Pulling configured images (set PULL_IMAGES=0 to skip)...'
  docker compose pull
fi
log "Starting services (timeout ${startup_timeout}s)..."
docker compose up -d --wait --wait-timeout "$startup_timeout"
docker compose ps -a

log 'Running HDFS + YARN + MapReduce WordCount...'
bash scripts/run-wordcount.sh 2>&1 | tee "$artifact_dir/run-wordcount.log"
case "$profile" in
  hive|full)
    log 'Running Hive SQL integration test...'
    bash scripts/run-hive-query.sh /test/output 2>&1 | tee "$artifact_dir/run-hive-query.log"
    ;;
esac
case "$profile" in
  spark|full|distributed)
    log 'Running Spark integration and cross-engine comparison...'
    bash scripts/run-spark-job.sh 2>&1 | tee "$artifact_dir/run-spark-job.log"
    ;;
  scale)
    log 'Running multi-file, multi-block scale workload...'
    bash scripts/run-scale-test.sh 2>&1 | tee "$artifact_dir/run-scale-test.log"
    ;;
  resilience)
    log 'Running controlled DataNode outage and recovery test...'
    bash scripts/run-resilience-test.sh 2>&1 | tee "$artifact_dir/run-resilience-test.log"
    ;;
esac
log "Profile '${profile}' passed."
