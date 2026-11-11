# Spark + Kafka experiment cluster

A throwaway-but-reusable analytics cluster for experiments: one Spark master, one
Spark worker, one single-node Kafka broker. **Every** service runs with host
networking — the containers use the machine's own interfaces, so Spark and Kafka
bind and advertise real addresses and the LAN can reach them without port
publishing or NAT. There is no Compose network in this stack.

| Service        | Image                                                  | What it is                            | Profile  | Ports (on the host)         |
| -------------- | ------------------------------------------------------ | ------------------------------------- | -------- | --------------------------- |
| `spark-master` | `apache/spark:4.1.3-scala2.13-java17-python3-ubuntu`    | standalone master, PySpark + Scala 2.13 | `master` | `7077`, `8080` (UI)        |
| `spark-worker` | same image                                             | one worker, 2 cores / 2 GB by default | `master`, `worker` | `8081` (UI)        |
| `kafka`        | `apache/kafka:4.3.2`                                   | single-node KRaft broker+controller   | `master` | `29092` (raft on `29093`, loopback) |
| `jupyter`      | built from `jupyter/Dockerfile` (same Spark image)     | JupyterLab, PySpark driver in-container | `master` | `8888`                    |
| `spark-history`| same Spark image                                       | Spark History Server                  | `tools`, `master` | `18080`            |
| `kafka-ui`     | `kafbat/kafka-ui:v1.5.0`                               | browse topics/messages in a browser   | `tools`, `master` | `8090`             |

Every service is behind a profile: `master` is the whole node, `worker` is just the
worker (a second machine), `tools` is history server + Kafka UI. A bare
`docker compose up` therefore starts **nothing** — always name a profile.

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

Pick the profile for the machine you are on:

```bash
docker compose --profile master up -d      # this machine runs the whole stack
docker compose --profile master ps
```

`master` brings up `init-dirs`, `spark-master`, `spark-worker`, `kafka` and
`jupyter` (plus `spark-history` and `kafka-ui`); it is what a single host needs.
`--profile worker` starts only `init-dirs` + `spark-worker` and is meant for a
second machine — see *Run a cluster across a LAN*.

Tooling alone, against an already running stack:

```bash
docker compose --profile tools up -d
```

The first `up` builds `jupyter/Dockerfile` (pip installs JupyterLab onto the Spark
image): needs network and a minute or two. Skip it with
`docker compose --profile master up -d spark-master spark-worker kafka` if you only
want the cluster, and build it later with `docker compose build jupyter`.

Podman works as a drop-in: `podman compose --profile master up -d` (needs
`podman-compose`).

First run pulls ~1.5 GB. `spark-master` is healthy once its UI answers; the worker
starts after `init-dirs` finishes and retries registration until the master is up,
so launch order does not matter (that is also why a worker-only node never tries to
start a master of its own).

Because the containers use host networking, their ports are the machine's own:
`7077`/`8080` (master), `8081` (worker), `29092` (Kafka, raft on `29093` only on
loopback), `8090` (Kafka UI), `18080` (history), `8888` (JupyterLab), plus whatever
ports a driver picks. Nothing is remapped, and there is no `networks:` block: Compose
refuses `network_mode: host` together with networks. Two consequences worth knowing
before you start: another service on the host that already owns `8080` collides with
the master UI (move it with `SPARK_MASTER_UI_PORT`), and a second worker on the *same*
host needs its own `SPARK_WORKER_UI_PORT`.

## Run a cluster across a LAN

Master on one machine, workers on another. Same checkout on both, and the same
`.env` with two values per node — where the master is, and what this node is:

```bash
# .env on the MASTER machine
SPARK_MASTER_IP=192.168.0.107      # this machine's LAN IP
SPARK_LOCAL_IP=192.168.0.107       # same value here
KAFKA_ADVERTISE_HOST=192.168.0.107 # same value here
KAFKA_BOOTSTRAP=192.168.0.107:29092

# .env on a WORKER machine
SPARK_MASTER_IP=192.168.0.107      # the master's LAN IP
SPARK_LOCAL_IP=192.168.0.21        # this worker's OWN LAN IP
```

`SPARK_MASTER_IP` is what Spark binds and advertises; `SPARK_LOCAL_IP` is how the
node identifies itself (Spark needs it under host networking, see *Troubleshooting*),
and it is the address the master and the other nodes see for that worker.
`KAFKA_ADVERTISE_HOST` is the broker's equivalent, and `KAFKA_BOOTSTRAP` is what
clients dial, so both have to name the master machine.

1. **Master machine** — `docker compose --profile master up -d`. The master binds
   `192.168.0.107:7077` (and advertises that address) because it shares the host's
   network namespace, so there is nothing to publish; Kafka binds `29092` on every
   interface the same way.
2. **Worker machine** — `docker compose --profile worker up -d`. Only
   `spark-worker` and `init-dirs` start; the worker dials
   `spark://192.168.0.107:7077`.
3. Open TCP `7077` and `29092` on the master machine's firewall. `8080` (master UI),
   `8081` (each worker's UI), `8090` (Kafka UI), `8888` (JupyterLab) and `18080`
   (history server) are for humans and optional.

Drivers get the same treatment: a driver started in `spark-master` or in `jupyter`
binds the host's address (JupyterLab passes `--conf spark.driver.host=$SPARK_MASTER_IP`
via `PYSPARK_SUBMIT_ARGS`), so executors on another machine connect back to it
directly — no `spark.driver.port` pinning, no published ports.

Check it from the master UI (<http://192.168.0.107:8080>) — each worker is listed
under *Workers* with its hostname, or:

```bash
curl -s http://192.168.0.107:8080/json/ | jq -r '.workers[] | "\(.host) \(.state)"'
```

And confirm tasks really land on the other machine: the driver logs
`Starting task ... (<worker-ip>,executor 0, ...)` — if only the master's own worker
appears, executors on the second machine are being rejected (check the firewall for
the driver's ports and that both nodes share the same `SPARK_MASTER_IP`).

**Kafka travels with the broker's address.** The broker is on the master machine and
advertises `KAFKA_ADVERTISE_HOST`; with that set to the LAN IP, clients anywhere can
use `KAFKA_BOOTSTRAP=<master-ip>:29092` — including executors on worker machines and
`kafka-ui` (which dials the same value). With the default `localhost` it is a
single-machine broker: Spark works, other machines cannot follow the metadata.

**Volumes are per node.** `spark-events` (history server), `spark-ivy` (jar cache)
and the Kafka data live in named volumes on the machine that created them. The
history server sees only events written on its own node, and every node that runs a
driver downloads `--packages` jars once on its own; executors get the driver's jars
shipped by Spark, so a worker-only node needs no `--packages` of its own.

## Verify it works

One command does all of it (cluster, broker, Spark⇄Kafka), exit code 0 on success:

```bash
./scripts/smoke-test.sh                        # docker
COMPOSE="podman compose" ./scripts/smoke-test.sh
```

By hand — Spark standalone (no Kafka involved). The master URL comes from the
container's environment, since `spark-defaults.conf` no longer hardcodes one:

```bash
docker compose exec spark-master bash -c '
  JAR=$(ls /opt/spark/examples/jars/spark-examples_2.13-*.jar | head -1)
  /opt/spark/bin/spark-submit --master "spark://$SPARK_MASTER_IP:$SPARK_MASTER_PORT" \
    --class org.apache.spark.examples.SparkPi "$JAR" 1000'
```

Spark ⇄ Kafka round trip (needs internet once, to fetch the connector):

```bash
docker compose exec spark-master bash -c '
  /opt/spark/bin/spark-submit --master "spark://$SPARK_MASTER_IP:$SPARK_MASTER_PORT" \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3 \
    /opt/examples/01_roundtrip.py'
```

Kafka from the CLI:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --list
docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:29092 --topic demo
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic demo --from-beginning
```

Streaming demo (windowed counts written back to Kafka): `examples/02_streaming.py`.

## Kafka on the host (one listener, one address that matters)

There used to be two broker listeners here — `kafka:19092` for the Compose network and
`localhost:29092` for the host — because a bridged container cannot use `localhost`.
With every container in the host's network namespace that split is gone:

- `PLAINTEXT` on `0.0.0.0:29092` is the broker, advertised as
  `KAFKA_ADVERTISE_HOST:KAFKA_PORT`. That advertised value is the only thing clients
  care about: `localhost:29092` on one machine, `<lan-ip>:29092` for a cluster.
- `CONTROLLER` on `127.0.0.1:29093` is the KRaft raft quorum. It is loopback-only and
  never advertised, so it is not exposed to the LAN.

If clients hang or report "connection to node -1 could not be established", the
advertised host is not what they can reach — fix `KAFKA_ADVERTISE_HOST` /
`KAFKA_BOOTSTRAP`, not the listener.

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
master (`PYSPARK_SUBMIT_ARGS` already sets `--master` and
`--conf spark.driver.host=$SPARK_MASTER_IP`, so notebooks need no `.master(...)`
call and their driver is reachable from executors on other machines):

```python
spark = SparkSession.builder.appName("jupyter-experiment").getOrCreate()
spark.read.format("kafka") \
     .option("kafka.bootstrap.servers", os.environ["KAFKA_BOOTSTRAP"]).load()
```

Three details that make this work:

* the image is built on the cluster's Spark image, so the client version matches the
  master (a mismatch is rejected by the master);
* `PYSPARK_SUBMIT_ARGS` on the container carries `--master`, `--conf
  spark.driver.host` and `--packages <connector>` — the only way to get the Kafka
  data source into a notebook session *and* ship the jars to the executors;
* JupyterLab runs with host networking too, so it is simply <http://localhost:8888>
  and the kernel can talk to `localhost:29092` for Kafka.

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
docker compose exec spark-master /opt/spark/bin/pyspark \
  --master "spark://\$SPARK_MASTER_IP:\$SPARK_MASTER_PORT"
docker compose exec -it spark-master /opt/spark/bin/spark-shell \
  --master "spark://\$SPARK_MASTER_IP:\$SPARK_MASTER_PORT"
```

`spark/conf/spark-defaults.conf` is mounted into every Spark container, so the shared
event log and the conservative executor sizing apply to master, worker and drivers
alike. It is the one file to edit for global tuning. It deliberately does **not** set
`spark.master` — that value differs per node now (see *.env* / `SPARK_MASTER_IP`), so
pass `--master spark://$SPARK_MASTER_IP:$SPARK_MASTER_PORT` on the command line.
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
| `SPARK_MASTER_IP`           | `localhost` | address Spark binds and advertises (LAN IP for a cluster) |
| `SPARK_LOCAL_IP`            | `localhost` | this node's own address (LAN IP of the node)  |
| `SPARK_MASTER_PORT`         | `7077`  | master RPC port                                |
| `SPARK_MASTER_UI_PORT`      | `8080`  | master UI port                                 |
| `SPARK_WORKER_UI_PORT`      | `8081`  | worker UI port (one per worker on a host)      |
| `SPARK_WORKER_CORES`        | `2`     | cores the single worker offers                 |
| `SPARK_WORKER_MEMORY`       | `2g`    | memory the worker offers (executors must fit)  |
| `KAFKA_PORT`                | `29092` | broker port on the host                        |
| `KAFKA_ADVERTISE_HOST`      | `localhost` | address the broker advertises to clients   |
| `KAFKA_BOOTSTRAP`           | `localhost:29092` | where Spark jobs and examples look for Kafka |
| `SPARK_HISTORY_UI_PORT`     | `18080` | history server UI                              |
| `KAFKA_UI_PORT`             | `8090`  | Kafka UI (in-container port, on the host now)  |
| `SPARK_IMAGE` / `KAFKA_IMAGE` | pinned tags | swap versions in one place               |

Every port above is the machine's own — host networking, nothing remapped — so
changing one changes where Spark or Kafka itself listens. `KAFKA_ADVERTISE_HOST` and
`KAFKA_BOOTSTRAP` have to agree with each other and with `KAFKA_PORT`; JupyterLab's
port is pinned to `8888` in `jupyter/Dockerfile`'s `CMD`.

To add a worker on the same host, copy the `spark-worker` block and give it a new
`hostname`, `container_name` and a free `--webui-port` (keep it in both profiles). On
another host, just run `docker compose --profile worker up -d` — it needs nothing but
`SPARK_MASTER_IP`.

## Reset / teardown

```bash
docker compose --profile master down          # stop this node, keep Kafka data + Spark event logs
docker compose --profile master down -v       # also remove the volumes
```

Run it on every node (a worker-only machine uses `--profile worker down`).

Kafka data, Spark event logs and the ivy/Maven jar cache live in named volumes
(`kafka-data`, `spark-events`, `spark-ivy`), so recreating containers keeps topics
and downloaded connectors. `init-dirs` is a one-shot busybox container that fixes
volume ownership for the non-root `spark` (uid 185) and `appuser` (uid 1000)
users — that is why the Spark and Kafka services wait for it to complete.

## About security

Deliberately open, for laptop experiments only: Kafka uses `PLAINTEXT` (no TLS, no
SASL, auto topic creation on), Spark standalone has no authentication
(`spark.authenticate` off), and UIs are unauthenticated. Do not put this on a
network you do not control. Every container uses **host networking**, so its ports
(7077/8080/8081/29092/8090/18080/8888 and any driver port) are bound on every
interface of the machine — with the `*_IP` values set to the LAN IP, anyone who can
reach that IP can submit jobs or delete topics. Restrict it with a firewall rather
than expecting Compose to.

## Troubleshooting

- **Worker shows no executors / `--memory` warning** — the worker's memory must
  exceed `spark.executor.memory` (1g) plus overhead; raise `SPARK_WORKER_MEMORY`.
- **`short-name resolution enforced but cannot prompt without a TTY`** — Podman
  refuses bare image names (`apache/spark:...`); qualify them or relax the policy.
  Either set `SPARK_IMAGE=docker.io/apache/spark:...` (same for `KAFKA_IMAGE`,
  `BUSYBOX_IMAGE`, `JUPYTER_IMAGE` base) in `.env`, or add
  `unqualified-search-registries = ["docker.io"]` under `[registries.search]` in
  `/etc/containers/registries.conf`.
- **podman-compose leaves a container in `Created` with `container state improper`** —
  its dependency engine races with the one-shot `init-dirs` container (jupyter is the
  usual victim). Re-run the same `up -d`, or `podman start skc-jupyter`; Docker Compose
  is unaffected.
- **`UnknownHostException: <container-hostname>` at startup** — host networking means
  Spark cannot look up `InetAddress.getLocalHost()`; set `SPARK_LOCAL_IP` to this
  machine's address (`localhost` on a single host). This is the one setting host
  networking cannot infer.
- **`KeyError: 'init-dirs'`** from `podman-compose --profile tools up` — the tools-only
  profile needs `init-dirs` for the event-log volume; it is included in all three
  profiles, so use a recent podman-compose. Docker Compose starts dependencies by
  itself.
- **Port already in use** — every port is the host's; change
  `SPARK_MASTER_PORT` / `SPARK_MASTER_UI_PORT` / `SPARK_WORKER_UI_PORT` /
  `SPARK_HISTORY_UI_PORT` / `KAFKA_PORT` / `KAFKA_UI_PORT`, or move the other
  service. JupyterLab's `8888` is fixed in `jupyter/Dockerfile`.
- **Kafka rejects the new quorum address on an old volume** — the KRaft metadata in
  `kafka-data` still names the previous controller listener. If the broker refuses to
  start, `docker compose --profile master down -v` drops the volume and lets it format
  fresh metadata (a single-node broker normally re-elects itself without this).
- **`Could not parse Master URL: 'null'`** — `spark.master` is no longer set in
  `spark-defaults.conf`; pass `--master spark://$SPARK_MASTER_IP:$SPARK_MASTER_PORT`.
- **`Could not find or load main class`** — wrong `--packages` Scala suffix; this
  image is Scala 2.13, so connector artifacts must end in `_2.13`.
- **Kafka restarts in a loop after changing `KAFKA_CLUSTER_ID`** — the existing
  volume was formatted with the old id; `docker compose down -v` to reformat.
