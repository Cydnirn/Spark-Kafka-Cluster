# Spark + Kafka experiment cluster

A throwaway-but-reusable analytics cluster for experiments: one Spark master, one
Spark worker, one single-node Kafka broker, all on one Compose network.

| Service        | Image                                                  | What it is                            | Host ports          |
| -------------- | ------------------------------------------------------ | ------------------------------------- | ------------------- |
| `spark-master` | `apache/spark:4.1.3-scala2.13-java17-python3-ubuntu`    | standalone master, PySpark + Scala 2.13 | `7077`, `8080` (UI) |
| `spark-worker` | same image                                             | one worker, 2 cores / 2 GB by default | `8081` (UI)         |
| `kafka`        | `apache/kafka:4.3.2`                                   | single-node KRaft broker+controller   | `29092`             |
| `jupyter`      | built from `jupyter/Dockerfile` (same Spark image)     | JupyterLab, PySpark driver in-container | `8888`            |
| `spark-history`| same Spark image (`--profile tools`)                   | Spark History Server                  | `18080`             |
| `kafka-ui`     | `kafbat/kafka-ui:v1.5.0` (`--profile tools`)           | browse topics/messages in a browser   | `8090`              |

Kafka 4.x has no ZooKeeper — the broker runs a single combined `broker,controller`
node (KRaft). Both images publish `amd64` and `arm64`.

```
docker-compose.yml              the stack
.env                            versions, ports, worker size (compose reads it automatically)
spark/conf/spark-defaults.conf  global Spark settings, mounted into every Spark container
jupyter/Dockerfile              JupyterLab image, built from the same Spark image
examples/spark_kafka_demo.ipynb notebook: connect to the cluster, write/read Kafka
examples/01_roundtrip.py        Spark writes 100 records to Kafka, reads them back
examples/02_streaming.py        windowed counts from a topic into another topic
scripts/smoke-test.sh           one-shot end-to-end verification
```

## Start

```bash
docker compose up -d                   # core cluster, includes building the Jupyter image
docker compose --profile tools up -d   # + history server + Kafka UI
docker compose ps
```

The first `up` builds `jupyter/Dockerfile` (pip installs JupyterLab onto the Spark
image): needs network and a minute or two. Skip it with
`docker compose up -d spark-master spark-worker kafka` if you only want the cluster,
and build it later with `docker compose build jupyter`.

Podman works as a drop-in: `podman compose up -d` (needs `podman-compose`).

First run pulls ~1.5 GB. `spark-master` is healthy once its UI answers; the worker
starts after the master is healthy and registers with it.

## Verify it works

One command does all of it (cluster, broker, Spark⇄Kafka), exit code 0 on success:

```bash
./scripts/smoke-test.sh                        # docker
COMPOSE="podman compose" ./scripts/smoke-test.sh
```

By hand — Spark standalone (no Kafka involved):

```bash
docker compose exec spark-master bash -c '
  JAR=$(ls /opt/spark/examples/jars/spark-examples_2.13-*.jar | head -1)
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
    --class org.apache.spark.examples.SparkPi "$JAR" 1000'
```

Spark ⇄ Kafka round trip (needs internet once, to fetch the connector):

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3 \
  /opt/examples/01_roundtrip.py
```

Kafka from the CLI:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list
docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:19092 --topic demo
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic demo --from-beginning
```

Streaming demo (windowed counts written back to Kafka): `examples/02_streaming.py`.

## Two Kafka listeners (the usual Kafka-in-Docker trap)

- `kafka:19092` — `PLAINTEXT`, advertised to everything inside the Compose network
  (Spark jobs use this).
- `localhost:29092` — `PLAINTEXT_HOST`, for clients on the host (CLI, IDE, Python).

A client must use the listener whose advertised address it can actually reach;
mixing them up is what produces "connection to node -1 could not be established".
From the `kafka` container itself `localhost:19092` also works, which is why the CLI
examples above use it.

## Files that must stay world-readable

Containers run as uid 185 (Spark) / 1000 (Kafka), not as you. Anything bind-mounted
into them — `spark/conf/spark-defaults.conf`, `examples/` — must be readable by
other users (mode `644`/`755`, which is what git gives you), and on SELinux hosts
(Fedora/RHEL) the mounts carry `:z` so podman/docker can relabel them. Without `:z`
Spark silently ignores its config: no event logs, default settings.

## JupyterLab

<http://localhost:8888> — no token, no password (see *About security*). Notebooks
live in the `jupyter-work` volume; on first start `jupyter/start.sh` copies the
example notebook there, and `examples/` is also mounted at `/opt/examples`.

The driver runs in the notebook container, so `SparkSession.master` is the cluster
master and the work lands on `spark-worker`:

```python
spark = (SparkSession.builder
         .appName("jupyter-experiment")
         .master("spark://spark-master:7077")
         .getOrCreate())
spark.read.format("kafka").option("kafka.bootstrap.servers", "kafka:19092").load()
```

Two details that make this work:

* the image is built on the cluster's Spark image, so the client version matches the
  master (a mismatch is rejected by the master);
* `PYSPARK_SUBMIT_ARGS` on the container carries `--packages <connector>`, which is
  the only way to get the Kafka data source into a notebook session *and* ship the
  jars to the executors.

`examples/spark_kafka_demo.ipynb` is a runnable walkthrough. Re-run it headlessly:

```bash
docker compose exec jupyter jupyter nbconvert --to notebook --execute \
  --output /tmp/out.ipynb /opt/examples/spark_kafka_demo.ipynb
```

## Submitting your own work

Jobs go through `spark-master` (driver in that container) and the worker picks up
executors. Your code is available at `/opt/examples` (this repo's `examples/`,
mounted read-only) — mount more directories if you need them:

```bash
docker compose exec spark-master /opt/spark/bin/pyspark --master spark://spark-master:7077
docker compose exec -it spark-master /opt/spark/bin/spark-shell --master spark://spark-master:7077
```

`spark/conf/spark-defaults.conf` is mounted into every Spark container, so
`spark.master`, the shared event log and the conservative executor sizing apply to
master, worker and drivers alike. It is the one file to edit for global tuning.
It also sets `spark.jars.ivy=/tmp/spark-ivy` (a volume): the image runs with
`HOME=/nonexistent`, so connectors downloaded by `--packages` have nowhere to live
otherwise.

`--packages` must be on the `spark-submit` command line. Setting
`spark.jars.packages` inside a `SparkSession.builder` in your script does **not**
resolve the connector — Spark only parses that flag in `spark-submit`.

## Tuning knobs

`.env`:

| Variable                    | Default | Meaning                                        |
| --------------------------- | ------- | ---------------------------------------------- |
| `SPARK_WORKER_CORES`        | `2`     | cores the single worker offers                 |
| `SPARK_WORKER_MEMORY`       | `2g`    | memory the worker offers (executors must fit)  |
| `SPARK_MASTER_HOST_PORT`    | `7077`  | host port for the master RPC                   |
| `SPARK_MASTER_UI_HOST_PORT` | `8080`  | host port for the master UI                    |
| `SPARK_WORKER_UI_HOST_PORT` | `8081`  | host port for the worker UI                    |
| `KAFKA_HOST_PORT`           | `29092` | host-side Kafka port                           |
| `JUPYTER_HOST_PORT`         | `8888`  | host port for JupyterLab                       |
| `SPARK_IMAGE` / `KAFKA_IMAGE` | pinned tags | swap versions in one place               |

The ports above are host-side only; inside the Compose network the services always
use 7077 / 8080 / 8081 / 19092, so nothing breaks when a host port is remapped.

To add workers, copy the `spark-worker` block and give it a new `hostname`,
`container_name` and worker UI port.

## Reset / teardown

```bash
docker compose down                       # stop, keep Kafka data + Spark event logs
docker compose --profile tools down -v    # also remove the tooling and the volumes
```

Kafka data, Spark event logs and the ivy/Maven jar cache live in named volumes
(`kafka-data`, `spark-events`, `spark-ivy`), so recreating containers keeps topics
and downloaded connectors. `init-dirs` is a one-shot busybox container that fixes
volume ownership for the non-root `spark` (uid 185) and `appuser` (uid 1000)
users — that is why the Spark and Kafka services wait for it to complete.

## About security

Deliberately open, for laptop experiments only: Kafka uses `PLAINTEXT` (no TLS, no
SASL, auto topic creation on), Spark standalone has no authentication
(`spark.authenticate` off), and UIs are unauthenticated. Do not put this on a
network you do not control. Compose publishes every UI on all host interfaces.

## Troubleshooting

- **Worker shows no executors / `--memory` warning** — the worker's memory must
  exceed `spark.executor.memory` (1g) plus overhead; raise `SPARK_WORKER_MEMORY`.
- **Port already in use** — override the port variable in `.env`.
- **`Could not find or load main class`** — wrong `--packages` Scala suffix; this
  image is Scala 2.13, so connector artifacts must end in `_2.13`.
- **Kafka restarts in a loop after changing `KAFKA_CLUSTER_ID`** — the existing
  volume was formatted with the old id; `docker compose down -v` to reformat.
