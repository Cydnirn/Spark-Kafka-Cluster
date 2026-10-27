#!/usr/bin/env python3
"""Structured Streaming demo: read demo-events, 10s tumbling window counts,
write the result to the demo-aggregates topic.

    docker compose exec spark-master /opt/spark/bin/spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3 \
        /opt/examples/02_streaming.py

While it runs, produce data from the host:

    docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh \
        --bootstrap-server localhost:19092 --topic demo-events
    # type lines like {"event_id":1,"user":1,"type":"click"}, Ctrl-D to stop

Then watch the output topic:

    docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server localhost:19092 --topic demo-aggregates --from-beginning
"""
from pyspark.sql import SparkSession, functions as F

BROKER = "kafka:19092"
IN_TOPIC = "demo-events"
OUT_TOPIC = "demo-aggregates"

# The connector jar has to come from spark-submit's --packages flag (see above).
spark = SparkSession.builder.appName("spark-kafka-streaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

events = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BROKER)
    .option("subscribe", IN_TOPIC)
    .option("startingOffsets", "latest")
    .load()
    .select(
        F.from_json(
            F.col("value").cast("string"), "event_id long, user long, type string"
        ).alias("e"),
        F.col("timestamp").alias("kafka_ts"),
    )
    .select("e.user", "e.type", "kafka_ts")
)

counts = (
    events.withWatermark("kafka_ts", "30 seconds")
    .groupBy(F.window("kafka_ts", "10 seconds"), "user", "type")
    .count()
    .select(
        F.concat(F.lit("user-"), F.col("user").cast("string")).alias("key"),
        F.to_json(
            F.struct(
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                F.col("user"),
                F.col("type"),
                F.col("count"),
            )
        ).alias("value"),
    )
)

query = (
    counts.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", BROKER)
    .option("topic", OUT_TOPIC)
    .option("checkpointLocation", "file:///tmp/spark-events/checkpoints/demo-agg")
    .outputMode("update")
    .trigger(processingTime="5 seconds")
    .start()
)

print(f"[stream] {IN_TOPIC} -> {OUT_TOPIC}, Ctrl-C to stop")
query.awaitTermination()
