#!/usr/bin/env python3
"""Round-trip demo: Spark writes to Kafka, then reads the topic back.

Run it inside the Spark master container:

    docker compose exec spark-master /opt/spark/bin/spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3 \
        /opt/examples/01_roundtrip.py

Adjust the connector version if you change SPARK_IMAGE in .env.
"""
from pyspark.sql import SparkSession, functions as F

BROKER = "kafka:19092"          # in-cluster PLAINTEXT listener
TOPIC = "demo-events"

# The Kafka data source lives in the connector jar, which must be added by
# spark-submit itself (--packages); setting spark.jars.packages in the builder
# is ignored by Spark, so keep the flag in the command line above.
spark = SparkSession.builder.appName("spark-kafka-roundtrip").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


def write_sample(n: int = 100) -> None:
    df = (
        spark.range(n)
        .select(
            F.concat(F.lit("user-"), (F.col("id") % 5).cast("string")).alias("key"),
            F.to_json(
                F.struct(
                    F.col("id").alias("event_id"),
                    (F.col("id") % 5).alias("user"),
                    F.lit("click").alias("type"),
                )
            ).alias("value"),
        )
    )
    (
        df.write.format("kafka")
        .option("kafka.bootstrap.servers", BROKER)
        .option("topic", TOPIC)
        .save()
    )
    print(f"[write] sent {n} records to {TOPIC}")


def read_back() -> None:
    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", BROKER)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )
    parsed = (
        raw.select(
            F.col("key").cast("string").alias("key"),
            F.from_json(F.col("value").cast("string"), "event_id long, user long, type string").alias("e"),
            F.col("partition"),
            F.col("offset"),
        )
        .select("key", "e.*", "partition", "offset")
    )
    total = parsed.count()
    print(f"[read] topic {TOPIC} currently holds {total} records")
    parsed.groupBy("key").count().orderBy("key").show(truncate=False)
    print("[ok] Spark <-> Kafka round-trip works")


if __name__ == "__main__":
    write_sample()
    read_back()
    spark.stop()
