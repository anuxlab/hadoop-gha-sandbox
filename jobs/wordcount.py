# -*- coding: utf-8 -*-
"""Small deterministic PySpark WordCount for the CI Hadoop sandbox."""
from pyspark.sql import SparkSession

INPUT_PATH = "hdfs://namenode:9000/test/input"
OUTPUT_PATH = "hdfs://namenode:9000/test/spark-output"


def main():
    spark = SparkSession.builder.appName("HadoopGHASandboxWordCount").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    try:
        counts = (
            sc.textFile(INPUT_PATH)
            .flatMap(lambda line: line.split())
            .map(lambda word: (word, 1))
            .reduceByKey(lambda left, right: left + right)
        )
        # Only collect for concise diagnostics; the test fixture is intentionally tiny.
        for word, count in sorted(counts.collect()):
            print("{0}\t{1}".format(word, count))
        counts.saveAsTextFile(OUTPUT_PATH)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
