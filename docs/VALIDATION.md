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
| Scale | `38383224133` | 24 files × 800 lines = 19,200 records; 48 MapReduce splits; 2 reducers launched; all 48 input blocks had two live replicas on distinct DataNodes; Spark WordCount processed input on two distinct worker hosts; outputs matched | Passed the scale assertions in the uploaded `diagnostics-scale-38383224133-1.zip` artifact |
| Resilience (initial attempt) | `38387453329` | Baseline MapReduce succeeded and DataNode 2 stopped, but `hdfs dfsadmin -report` continued to show 2 live DataNodes for the 240-second wait; the timeout prevented the outage-time read/job assertions from running | Failed because the wait was shorter than Hadoop’s default DataNode expiration interval; this run does not show an HDFS outage-read failure |

These runs establish working baseline behavior for the versions and assertions then present in those commits. The uploaded distributed artifact proves two live HDFS replica endpoints and two Spark worker hosts; it does not by itself prove that a large WordCount data set used both Spark workers. That stronger assertion is implemented in the scale profile below.

## Local checks run against the automation package

- Bash syntax checks: all `scripts/*.sh` and `hive/entrypoint.sh` passed `bash -n`.
- Parser and regression suite: all checks passed, including YARN configuration parsing, `HADOOP_MAPRED_HOME`, Spark serialization and executor-host metadata, Hive initialization invariants, pipeline `pipefail`, HDFS distinct-replica parsing, scale fixture block size, automated workflow triggers, resilience assertions, and input bounds.
- Python syntax: `jobs/wordcount.py` and `scripts/assert-hdfs-replication.py` passed `py_compile`.
- YAML syntax: all Compose files and workflow files passed syntax parsing.
- Scale input guards: invalid file count, line count and reducer count returned exit code 2 before any Docker operation.
- The resilience script is shell-syntax and regression-checked. A dedicated `docker-compose.resilience.yml` sets `dfs.namenode.heartbeat.recheck-interval` to 10,000 ms only for this profile; regular distributed and scale profiles do not inherit this test-only tuning. Docker/Compose is not installed in this authoring environment, so the fix cannot be runtime-tested here.

## Remaining live-validation gate

The uploaded `diagnostics-scale-38383224133-1.zip` confirms the scale profile passed, including the 16 KiB NameNode minimum-block-size correction. The first resilience run (`38387453329`) stopped at a detector timeout, before its outage-time read and MapReduce checks. Apache Hadoop 3.2.x defines `dfs.namenode.heartbeat.recheck-interval` as 300000 ms (five minutes) by default, and the NameNode expiration interval uses this value in its calculation; the harness only waited 240 seconds. The fix adds a resilience-only Compose overlay setting this property to 10000 ms, while keeping normal distributed/scale profiles unchanged. A new live Actions run is still required to prove that the resolved setting takes effect and that reads/job execution/recovery all pass. Static checks cannot establish that runtime behavior.

## Earlier scale workflow failure: run `38382194755`

The first manual scale run failed during fixture upload, before MapReduce or Spark launched. The diagnostic `run-wordcount.log` contains:

```text
put: Specified block size is less than configured minimum value (dfs.namenode.fs-limits.min-block-size): 16384 < 1048576
```

Root cause: `docker-compose.scale.yml` set `dfs.blocksize=16384`, but Hadoop 3.2.1's NameNode default `dfs.namenode.fs-limits.min-block-size` remained 1 MiB. The NameNode rejected each 16 KiB block request. The fix lowers the minimum to 16384 **only in the isolated scale overlay** and adds a preflight using `hdfs getconf -confKey` for both settings. The later successful run `38383224133` verifies that the fix took effect: the log reports both effective values as 16384 bytes and the scale assertions complete successfully.
