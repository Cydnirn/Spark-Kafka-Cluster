#!/usr/bin/env bash
# Post-start smoke test: Spark master/worker up, Kafka reachable, and the
# Spark <-> Kafka round trip works. Exits non-zero on the first failure.
#
#   ./scripts/smoke-test.sh            # docker
#   COMPOSE="podman compose" ./scripts/smoke-test.sh
set -euo pipefail

COMPOSE=${COMPOSE:-docker compose}
CONNECTOR=${CONNECTOR:-org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3}
cd "$(dirname "$0")/.."

step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

step "containers"
$COMPOSE ps

step "spark standalone cluster"
# The jar name carries the Scala version, so resolve it inside the container.
$COMPOSE exec -T spark-master bash -c '
  JAR=$(ls /opt/spark/examples/jars/spark-examples_2.13-*.jar | head -1)
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
    --class org.apache.spark.examples.SparkPi "$JAR" 200 2>/dev/null \
    | grep -o "Pi is roughly [0-9.]*"
'

step "kafka broker"
$COMPOSE exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 --create --if-not-exists \
  --topic smoke --partitions 3 --replication-factor 1
$COMPOSE exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 --describe --topic smoke

step "spark -> kafka -> spark"
$COMPOSE exec -T spark-master /opt/spark/bin/spark-submit \
  --packages "$CONNECTOR" /opt/examples/01_roundtrip.py 2>/dev/null \
  | grep -E '^\[(write|read|ok)\]'

step "jupyterlab -> spark (runs the demo notebook in a kernel)"
$COMPOSE exec -T jupyter jupyter nbconvert --to notebook --execute \
  --output-dir=/tmp --output=smoke.ipynb /opt/examples/spark_kafka_demo.ipynb >/dev/null \
  && echo "demo notebook executed end to end"

printf '\n\033[1msmoke test passed\033[0m\n'
