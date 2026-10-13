# Hadoop GHA Sandbox

A disposable Apache Hadoop ecosystem proof of concept for GitHub Actions. It runs real Hadoop/HDFS/YARN, MapReduce, HiveServer2 and standalone Spark processes in Docker containers and validates their data paths with small deterministic integration tests.

> **Scope:** this is an integration-test sandbox, not a production Hadoop deployment. Containers share one runner kernel, CPU, memory and disk. The default profile is single-worker; a separate manual profile adds multiple workers to test selected distributed behaviors.

## Profiles

| Profile | Services | Main assertions | Trigger |
|---|---|---|---|
| `core` | NameNode, DataNode, ResourceManager, NodeManager | HDFS write/read, expected live workers, MapReduce-on-YARN WordCount output | Every push/PR |
| `hive` | Core + PostgreSQL metastore + HiveServer2 | Beeline readiness and exact SQL results over the MapReduce HDFS output | Every push/PR |
| `spark` | Core + standalone Spark master/worker | Worker registration, PySpark execution, `_SUCCESS`, output equals MapReduce | Every push/PR |
| `full-stack` | Core + Hive + Spark | Runs the three integration paths together | Manual workflow dispatch |
| `distributed` | Core + second DataNode/NodeManager + second Spark worker | Two live storage/scheduler workers, replication factor 2, MapReduce and Spark output equivalence | Manual workflow dispatch |

The profile overlays extend the shared `docker-compose.yml`; they do not copy the base services. Compose project-scoped networks and volumes isolate separate projects/runs. Do not use a single Docker network across separate GitHub-hosted runners: matrix jobs have independent hosts and Docker daemons.

## Run locally

Requirements: Docker Engine with the Docker Compose v2 plugin, `curl` inside the service images, and Bash on the host. Run commands from the repository root.

### Core Hadoop

```bash
docker compose -f docker-compose.yml up -d --wait --wait-timeout 400
bash scripts/run-wordcount.sh
docker compose -f docker-compose.yml logs --no-color
docker compose -f docker-compose.yml down -v --remove-orphans
```

### Hive

```bash
export COMPOSE_FILE=docker-compose.yml:docker-compose.hive.yml
docker compose up -d --wait --wait-timeout 450
bash scripts/run-wordcount.sh
bash scripts/run-hive-query.sh /test/output
docker compose down -v --remove-orphans
unset COMPOSE_FILE
```

### Standalone Spark

```bash
export COMPOSE_FILE=docker-compose.yml:docker-compose.spark.yml
docker compose up -d --wait --wait-timeout 400
bash scripts/run-wordcount.sh
bash scripts/run-spark-job.sh
docker compose down -v --remove-orphans
unset COMPOSE_FILE
```

### Multi-worker proof of concept

This profile uses more memory and is intended for manual/scheduled validation rather than every pull request.

```bash
export COMPOSE_FILE=docker-compose.yml:docker-compose.spark.yml:docker-compose.distributed.yml
export EXPECTED_DATANODES=2 EXPECTED_NODEMANAGERS=2 EXPECTED_SPARK_WORKERS=2 EXPECTED_REPLICATION=2

docker compose up -d --wait --wait-timeout 450
bash scripts/run-wordcount.sh
bash scripts/run-spark-job.sh
docker compose ps -a
docker compose logs --no-color
docker compose down -v --remove-orphans
unset COMPOSE_FILE EXPECTED_DATANODES EXPECTED_NODEMANAGERS EXPECTED_SPARK_WORKERS EXPECTED_REPLICATION
```

The distributed test waits for two registered DataNodes and NodeManagers, sets the input file's replication factor to two and asks the Spark master for two live workers. This is stronger than merely starting two containers, but it is still a same-host simulation—not proof of independent host failure domains or high availability.

## Test behavior

- `scripts/test-readiness-parsers.sh` runs fast regression tests for the MapReduce XML parser, Spark's `ALIVE` worker-state field, and the Hive/PostgreSQL healthcheck dependency.
- `scripts/run-wordcount.sh` waits for the expected HDFS/YARN workers, checks `mapreduce.framework.name=yarn` using only that XML property (without capturing the next property's value), uploads a deterministic fixture, submits the Hadoop examples WordCount job, checks `_SUCCESS`, and compares all expected word/count pairs.
- `scripts/run-hive-query.sh` waits for a real Beeline `SELECT 1`, creates an external Hive table over the MapReduce output and compares returned rows with expected counts. PostgreSQL health means SQL/database availability, not existence of Hive's `VERSION` table; the image initializes the Hive schema during PostgreSQL bootstrap, so HiveServer2 must not attempt to recreate existing objects as the non-owner `hive` role.
- `scripts/run-spark-job.sh` checks Spark master worker registration using the `state=ALIVE` field returned by the Spark 3.0 REST API, reports Spark/Python versions, submits a PySpark job, checks `_SUCCESS` and compares Spark output to MapReduce output.
- CI saves Compose configuration, service state and logs as artifacts, even after failure. Teardown removes containers and volumes to avoid state leaking into future runs.

## Resource and compatibility notes

- The base profile is intentionally memory constrained. The full-stack and multi-worker profiles use more memory and may need a larger runner if the image JVMs exceed their budgets.
- Hadoop, Hive and Spark images are old, pinned tags selected for this proof of concept. Validate Java/Python compatibility and image availability before upgrading; then pin tested digests if stronger supply-chain reproducibility is required.
- The pinned `bde2020/hive-metastore-postgresql:2.3.0` image initializes the `hive` database role with the test password `hive` and preloads the Hive 2.3 schema. HiveServer2 connects as `hive`; `datanucleus.schema.autoCreateAll=false` prevents it from trying to recreate indexes/tables owned by `postgres`. The Compose service connects to PostgreSQL as `postgres` so the image's init script can create the `metastore` database and schema. This credential is for the disposable test image only, not a production secret. Never reuse it outside this sandbox.
- The Spark job uses a tiny fixture and collects results for concise diagnostics. Do not treat this as a benchmark or increase the fixture substantially without revisiting driver memory and collection behavior.
- No Docker engine is required to edit the files, but actual integration success must be established by running the workflows. YAML parsing and shell syntax checks alone cannot prove that container images boot or jobs succeed.

## Failure investigation

Open the failed workflow run and download its `diagnostics-*` artifact. It contains `compose-ps.txt`, `compose-logs.txt`, and the resolved Compose configuration. The harness logs are also retained as `run-wordcount.log`, `run-hive-query.log` and `run-spark-job.log` where applicable. The most useful first checks are worker registration, the generated `mapred-site.xml`, the YARN application final state, and the first error in the failing service's logs.

## Security

This sandbox is designed for trusted CI code. Do not expose the unauthenticated Hadoop/Hive/Spark web interfaces to untrusted networks, do not use production data or credentials, and do not run untrusted pull-request code with secrets.
