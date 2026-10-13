# Scaling and Component Extension Guide

This document describes the next test layers for Hadoop GHA Sandbox. It is a design and implementation guide; a profile is considered validated only after the corresponding GitHub Actions run passes.

## Test tiers

| Tier | Profile | Main assertions | Recommended trigger |
|---|---|---|---|
| 1. Smoke | `core`, `hive`, `spark` | Service readiness, one HDFS/YARN job, Hive query, Spark/HDFS comparison | Each push/PR |
| 2. Interoperability | `full` | Hive and Spark read the same HDFS data produced by MapReduce | Relevant PRs/pushes; manual |
| 3. Worker topology | `distributed` | Two DataNodes, two NodeManagers, two Spark workers, live replica endpoints and executor host participation | Relevant PRs/pushes; manual |
| 4. Small scale | `scale` | Multiple multi-block files, more MapReduce splits than files, exact record counters, multiple reducers, per-block replication, actual Spark WordCount processing on both workers | Manual, bounded parameters |
| 5. Resilience (future) | `resilience` | Worker loss, task retry, data availability with one replica unavailable, bounded recovery/failure outcome | Manual or scheduled after stable baselines |

## What the `scale` profile proves

The scale profile keeps the cluster small but increases the number of input files and blocks. Its HDFS block size and NameNode `dfs.namenode.fs-limits.min-block-size` are both set to 16 KiB in `docker-compose.scale.yml`. Hadoop's default minimum block size is 1 MiB; setting only `dfs.blocksize` causes the NameNode to reject `hdfs dfs -put` with `Specified block size is less than configured minimum value`. The scale script preflights both effective configuration values before generating/uploading data. The lowered minimum is isolated to this synthetic test profile and is not a production recommendation. Each generated file is larger than the test block size, which exercises more than one HDFS block and input split per file.

Defaults are 24 files × 800 lines (19,200 records) and two reducers. Input bounds are validated before containers are started: 2–128 files, 600–2,000 lines per file, at most 128,000 records, 1–4 reducers, two DataNodes, two NodeManagers and two Spark workers. Keep these limits bounded on GitHub-hosted runners.

The test asserts:

1. Every scale input block has the requested number of live replicas on distinct DataNode endpoints.
2. MapReduce's input split count exceeds the number of files, its `Map input records` counter matches the fixture, the expected reducers launch, the YARN application succeeds, and the output counts are exact.
3. Spark writes `_SUCCESS`, its normalized word/count pairs match MapReduce, and the actual input partitions used by WordCount were processed on both Spark worker hosts.

This is a CI-sized behavioral scale test, not a throughput benchmark. All containers still share one host, one kernel, and host-level CPU, disk and network resources. Increase the fixture only after observing wall time, memory, disk and failure rates. Do not raise runner limits solely to make an assertion pass.

## Suggested next tests

### 1. HDFS operational semantics

Add focused tests using small deterministic fixtures before adding more services:

- Safe-mode behavior: verify a write is rejected while NameNode safe mode is enabled and succeeds after safe mode exits.
- Directory quotas: configure a test directory quota and verify the expected failure when exceeded.
- Permissions and ACLs: create a test user/group context, then assert authorized and unauthorized read/write outcomes.
- Snapshot lifecycle: enable snapshots on a test directory, create a snapshot, mutate the file, and verify the snapshot retains the old data.
- Recovery: stop one DataNode in the distributed profile, check expected replica health behavior, then restart it and verify re-replication within a bounded time.

These should be separate, opt-in profiles where they modify cluster state or need extra setup. Test expected failure messages and exit codes, not just that a command failed.

### 2. YARN resource and retry behavior

- Submit two applications concurrently and verify both final states plus their application IDs.
- Use a deliberately bounded memory request to verify the scheduler rejects or queues an unsupported allocation as expected.
- Add a controlled task-failure example and assert the task-attempt/retry counters and terminal application state.
- For multi-worker proof, capture task execution hosts and assert multiple NodeManagers participated. Do not equate two registered NodeManagers with distributed task execution.

### 3. Spark scheduling and HDFS integration

- Keep standalone Spark as the low-cost baseline.
- For a distinct integration path, add an optional `spark-yarn` profile that submits Spark through YARN. This requires a compatibility check for Spark 3.0.0, Hadoop 3.2.1, Java and the image-provided Hadoop client configuration. Do not reuse the standalone profile's assertions as proof of YARN mode.
- Add a partition-count test across two workers; assert worker hosts from actual input-processing tasks.
- Add deterministic empty-input and multi-file input tests.

### 4. Optional ecosystem components

Only add a component when there is a concrete contract to verify and enough memory budget. Good candidates are:

- **HBase + ZooKeeper**: verify table create/put/get and persistence/restart behavior on HDFS. This adds ZooKeeper plus RegionServer/Master and therefore should be a separate profile.
- **Trino or another SQL query engine over Hive Metastore**: verify it can discover the same Hive table and return equivalent rows; pin compatible versions and keep the profile separate from full stack if memory is tight.
- **Kafka-to-HDFS ingestion (e.g., Kafka Connect or NiFi)**: use a bounded message fixture and assert records arrive in HDFS before running the existing query checks. This is a larger integration and should come after stable scale tests.

Do not add all components to one Compose topology. Separate overlays and independent CI profiles keep failures attributable and resource usage understandable.

## Component extension contract

For each new service/component:

1. Add a dedicated `docker-compose.<component>.yml` overlay. Reuse the base services and service DNS names; do not fork the base cluster.
2. Pin a known compatible image tag and document all related runtime versions and required environment/configuration files.
3. Add a healthcheck for service liveness and a bounded application-level readiness check for actual usability.
4. Write a small deterministic integration fixture and assert semantic results (rows, records, block states or final application state).
5. Ensure the harness uses `set -Eeuo pipefail`, captures useful logs and preserves original failure exit codes.
6. Add fast, no-Docker parser/config regression tests where possible.
7. Add a workflow that uses minimal permissions, uploads diagnostic artifacts with `if: always()`, and always cleans up. Start expensive profiles manually; automate them only after runtime and resource cost are understood.
8. Update `README.md`, this guide, and `CONTRIBUTING.md`; cite upstream component docs and the image maintainer's contract separately.

## CI trigger policy

- Keep `core`, `hive`, and `spark` smoke tests on each relevant push/PR.
- Keep `full` and `distributed` path-filtered.
- Keep `scale` manually dispatchable until its GitHub run has passed and its runtime/cost is known.
- Leave nightly schedules commented out until you deliberately enable them.
- For scheduled tests, retain only concise reports and time-bounded diagnostics; never expose real credentials.
