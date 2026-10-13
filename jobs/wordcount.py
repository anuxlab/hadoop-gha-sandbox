# -*- coding: utf-8 -*-
"""Deterministic PySpark WordCount with executor-host instrumentation.

Optional positional arguments:
    input_path output_path probe_hosts_path wordcount_hosts_path

The first host list is a scheduling probe; the second records hosts that
processed the actual WordCount input partitions.
"""
import socket
import sys

from pyspark import StorageLevel
from pyspark.sql import SparkSession

DEFAULT_INPUT_PATH = "hdfs://namenode:9000/test/input"
DEFAULT_OUTPUT_PATH = "hdfs://namenode:9000/test/spark-output"
DEFAULT_PROBE_HOSTS_PATH = "hdfs://namenode:9000/test/spark-worker-hosts"
DEFAULT_WORDCOUNT_HOSTS_PATH = "hdfs://namenode:9000/test/spark-wordcount-worker-hosts"


def format_count_pair(item):
    """Serialize a (word, count) pair in Hadoop's tab-separated format."""
    return "{0}\t{1}".format(item[0], item[1])


def tokenize_partition(lines):
    """Emit each word with the executor host that processed its input line."""
    host = socket.gethostname()
    for line in lines:
        for word in line.split():
            yield (word, (1, frozenset((host,))))


def merge_word_count_metadata(left, right):
    """Combine counts while retaining the distinct hosts that processed them."""
    return (left[0] + right[0], left[1] | right[1])


def host_for_partition(_iterator):
    """Report the executing executor container hostname for a scheduling probe."""
    yield socket.gethostname()


def main():
    args = sys.argv[1:]
    if len(args) > 4:
        raise ValueError(
            "Expected at most 4 application arguments: input_path output_path "
            "probe_hosts_path wordcount_hosts_path"
        )
    input_path = args[0] if len(args) > 0 else DEFAULT_INPUT_PATH
    output_path = args[1] if len(args) > 1 else DEFAULT_OUTPUT_PATH
    probe_hosts_path = args[2] if len(args) > 2 else DEFAULT_PROBE_HOSTS_PATH
    wordcount_hosts_path = args[3] if len(args) > 3 else DEFAULT_WORDCOUNT_HOSTS_PATH

    spark = SparkSession.builder.appName("HadoopGHASandboxWordCount").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    try:
        # Host metadata flows through the same map/reduce work as the counts.
        # The scale test can therefore prove actual input partitions ran on
        # multiple workers instead of relying only on an unrelated probe job.
        counts = (
            sc.textFile(input_path)
            .mapPartitions(tokenize_partition)
            .reduceByKey(merge_word_count_metadata)
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        counts.map(
            lambda item: format_count_pair((item[0], item[1][0]))
        ).saveAsTextFile(output_path)
        print("Spark WordCount output written to {0}".format(output_path))

        wordcount_hosts = sorted(
            counts.flatMap(lambda item: list(item[1][1])).distinct().collect()
        )
        if not wordcount_hosts:
            raise RuntimeError("No executor host processed a WordCount input token.")
        sc.parallelize(wordcount_hosts, 1).saveAsTextFile(wordcount_hosts_path)
        print(
            "Spark WordCount input processed on {0} host(s): {1}".format(
                len(wordcount_hosts), ",".join(wordcount_hosts)
            )
        )
        counts.unpersist()

        # Keep a separate scheduling probe for the existing smoke-test contract.
        # It verifies that the cluster can place ordinary Spark tasks on workers;
        # the scale profile additionally checks WordCount's own input workers.
        expected_workers = int(sc.getConf().get("spark.gha.expectedWorkers", "1"))
        probe_partitions = max(2, expected_workers * 2)
        probe_hosts = sorted(
            sc.parallelize(range(probe_partitions), probe_partitions)
            .mapPartitions(host_for_partition)
            .distinct()
            .collect()
        )
        if len(probe_hosts) < expected_workers:
            raise RuntimeError(
                "Expected probe tasks on at least {0} distinct Spark worker hosts, got {1}: {2}".format(
                    expected_workers, len(probe_hosts), probe_hosts
                )
            )
        sc.parallelize(probe_hosts, 1).saveAsTextFile(probe_hosts_path)
        print("Spark probe worker hosts: {0}".format(",".join(probe_hosts)))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
