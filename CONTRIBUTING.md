# Contributing

Thanks for helping improve Hadoop GHA Sandbox. Contributions should keep the PoC reproducible, diagnosable, and honest about which distributed behavior has actually been tested.

## Before opening a pull request

1. Open an issue or describe the problem, expected behavior, and affected profile.
2. Keep a change scoped: one configuration, test, or behavior change per PR where practical.
3. Do not commit real credentials, personal/customer data, private cluster configuration, or logs containing secrets.
4. Use test-only data and credentials. This Compose setup is not a production security boundary.

## Local checks

From the repository root, run:

```bash
# Shell syntax
for f in scripts/*.sh hive/entrypoint.sh; do bash -n "$f"; done

# Fast parser/config regression suite
bash scripts/test-readiness-parsers.sh

# Python syntax (does not require PySpark to be installed)
python3 -m py_compile jobs/wordcount.py

# Validate each Compose overlay (requires Docker Compose v2)
docker compose -f docker-compose.yml config --quiet
docker compose -f docker-compose.yml -f docker-compose.hive.yml config --quiet
docker compose -f docker-compose.yml -f docker-compose.spark.yml config --quiet
docker compose -f docker-compose.yml -f docker-compose.hive.yml -f docker-compose.spark.yml config --quiet
docker compose -f docker-compose.yml -f docker-compose.spark.yml -f docker-compose.distributed.yml config --quiet
docker compose -f docker-compose.yml -f docker-compose.spark.yml -f docker-compose.distributed.yml -f docker-compose.scale.yml config --quiet
```

Then run the narrowest affected profile and its relevant integration dependencies:

```bash
bash scripts/run-profile.sh core
bash scripts/run-profile.sh hive
bash scripts/run-profile.sh spark
bash scripts/run-profile.sh full
bash scripts/run-profile.sh distributed
# Bounded scale and controlled failure/recovery profiles.
SCALE_FILES=24 SCALE_LINES_PER_FILE=800 bash scripts/run-profile.sh scale
bash scripts/run-profile.sh resilience
```

`full` and `distributed` use more resources. Run them when changing overlays, shared Hadoop configuration, resource allocation, Hive configuration, Spark submission, or shared test harnesses.

## Test design guidelines

- Prefer explicit application readiness checks over fixed sleeps or TCP-only checks. Bound every wait and show the last observed state at timeout.
- Preserve error exit codes. If piping a test through `tee`, use `set -Eeuo pipefail`.
- Tests must assert the result: exact word/count pairs, YARN final state, `_SUCCESS`, query rows, live replicas, or distinct worker hosts as appropriate.
- Do not use output directory existence alone as proof of a successful job.
- Clean stale HDFS input/output paths before each run and remove Compose state after it, unless a documented debug mode explicitly retains failed containers.
- Add a fast regression test for every bug that can be tested without starting the whole cluster.
- Use bounded, deterministic fixtures for PR tests. Keep disruptive tests in their own automated, scheduled profile with a controlled failure window and guaranteed cleanup.
- Do not claim that multiple containers prove multi-host fault tolerance. State the exact behavior verified by assertions.

## Image and version changes

When changing a Hadoop, Hive, Spark, Java, Python, or PostgreSQL image:

1. Verify compatibility against the official, version-specific Apache documentation and the image maintainer's entrypoint/environment conventions.
2. Check config paths, generated XML files, JAR locations, launch environments, and Python interpreter selection inside the actual image.
3. Update version-specific constants and tests such as `HADOOP_MAPRED_HOME` and the ApplicationMaster JAR preflight.
4. Run core, Hive, Spark, full-stack, and distributed profiles where applicable. Static YAML validation is not a substitute for an integration run.
5. Prefer immutable image digests for release-grade reproducibility after confirming the intended image works.

## Workflow changes

- Keep `permissions` least-privileged (`contents: read` unless more is required).
- Keep diagnostics and cleanup under `if: always()` so failures remain debuggable.
- Keep `fail-fast: false` in the profile matrix so one profile does not hide the results of others.
- All test tiers must have automatic pull-request/push triggers and a scheduled regression cadence; `workflow_dispatch` may remain as an optional rerun path. The scale defaults are bounded and its tiny HDFS block-size setting is isolated to the synthetic test overlay. The resilience workflow intentionally stops DataNode 2 only inside an ephemeral Compose project, then restarts it and checks replica recovery.
- Make expensive profiles path-filtered where possible, but never suppress a genuine test failure with `continue-on-error`.
- Do not expose service web UIs or ports to an untrusted network.

## Pull request checklist

- [ ] The change and its scope are described, with the affected profiles listed.
- [ ] Shell syntax and `scripts/test-readiness-parsers.sh` pass.
- [ ] Python syntax passes if `jobs/` changed.
- [ ] All affected Compose configurations validate, including `docker-compose.scale.yml` and the distributed overlay used by the resilience workflow.
- [ ] The scale workload verifies multiple HDFS blocks/splits, expected MapReduce record counters/reducer launches, live replica locations, and actual Spark input-worker participation when relevant. The resilience workload verifies a read and MapReduce job during one DataNode outage, then confirms recovery and expected replication.
- [ ] Relevant GitHub Actions runs pass, or the PR clearly explains which runtime check could not be performed and why.
- [ ] Logs/diagnostics do not contain secrets.
- [ ] README or relevant comments describe new configuration, profiles, limitations, and troubleshooting.

## Commit style

Use a short imperative commit subject, optionally prefixed by the area, for example:

- `ci: trigger full-stack integration automatically`
- `spark: assert tasks use both workers`
- `hdfs: verify live block replicas`
- `docs: explain adding a custom overlay`

## Security report

Do not publish credentials or sensitive diagnostics in a public issue. For potentially sensitive security issues, contact the repository maintainer privately before disclosure.
