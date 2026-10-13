# Validation Evidence and Remaining Gates

This report distinguishes existing GitHub Actions evidence from checks run locally against the source tree. A passing earlier profile does not automatically validate new code added afterward.

## Uploaded GitHub Actions diagnostics reviewed

| Profile | Run ID in uploaded artifact | Evidence observed | Assessment |
|---|---:|---|---|
| Core | `38378261106` | One live DataNode and one running NodeManager; HDFS input block had one live replica; YARN application `Final-State: SUCCEEDED`; exact expected WordCount output | Passed the profile assertions in that run |
| Hive | `38378261106` | Core MapReduce prerequisite succeeded; PostgreSQL/Hive services were healthy; Hive query returned all expected word/count pairs | Passed the profile assertions in that run |
| Standalone Spark | `38378261106` | One live Spark worker; Spark wrote output and normalized Spark results matched MapReduce | Passed the profile assertions in that run |
| Full stack | `38378261105` | Shared core job succeeded; Hive returned expected pairs; Spark output matched MapReduce; service health was recorded | Passed the profile assertions in that run |
| Distributed | `38378261082` | Two DataNodes and two NodeManagers registered; `fsck` reported two replicas on two distinct DataNode endpoints; Spark scheduling probe ran on two hosts; Spark and MapReduce outputs matched | Passed the profile assertions in that run |

These runs establish working baseline behavior for the versions and assertions then present in those commits. The uploaded distributed artifact proves two live HDFS replica endpoints and two Spark worker hosts; it does not by itself prove that a large WordCount data set used both Spark workers. That stronger assertion is implemented in the scale profile below.

## Local checks run against this package

- Bash syntax checks: all `scripts/*.sh` and `hive/entrypoint.sh` passed `bash -n`.
- Parser and regression suite: all checks passed, including YARN configuration parsing, `HADOOP_MAPRED_HOME`, Spark serialization and executor-host metadata, Hive initialization invariants, pipeline `pipefail`, HDFS distinct-replica parsing, scale fixture block size, workflow policy, and input bounds.
- Python syntax: `jobs/wordcount.py` and `scripts/assert-hdfs-replication.py` passed `py_compile`.
- YAML syntax: all five Compose files and four workflow files parsed successfully.
- Scale input guards: invalid file count, line count and reducer count returned exit code 2 before any Docker operation.
- Whitespace and ZIP integrity checks were run when packaging.

## Not yet validated by live execution

Docker/Compose is not installed in the authoring environment used for these package checks. The following still require a new GitHub Actions run after this package is committed:

1. `test-scale.yml`, with its multi-file/multi-block workload and actual Spark WordCount worker participation assertion.
2. A semantically resolved `docker compose config --quiet` for the new `docker-compose.scale.yml` overlay and the scale integration path.
3. Any future resilience or new-component profile before it is described as passing.

Run the scale test manually from Actions → **Scalable HDFS and compute integration test**. Start with the defaults (24 files × 800 lines, two reducers); increase only after its runtime and memory use are known. Keep the nightly schedule disabled until the manual scale run is stable.


## Scale workflow failure: run `38382194755`

The first manual scale run failed during fixture upload, before MapReduce or Spark launched. The diagnostic `run-wordcount.log` contains:

```text
put: Specified block size is less than configured minimum value (dfs.namenode.fs-limits.min-block-size): 16384 < 1048576
```

Root cause: `docker-compose.scale.yml` set `dfs.blocksize=16384`, but Hadoop 3.2.1's NameNode default `dfs.namenode.fs-limits.min-block-size` remains 1 MiB. The NameNode therefore rejected each 16 KiB block request. The fix sets the minimum to 16384 **only in the isolated scale overlay** and adds a runtime preflight using `hdfs getconf -confKey` for both settings before the fixture upload. Regression tests assert the environment-variable encoding and preflight are present. This source fix still requires a new live GitHub Actions scale run; the current failed artifact does not validate the updated scale profile.
