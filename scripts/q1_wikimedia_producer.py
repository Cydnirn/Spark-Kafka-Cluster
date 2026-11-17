#!/usr/bin/env python3
"""
Q1b — Wikimedia EventStreams -> Apache Kafka producer
=====================================================

Reads the public Wikimedia `recentchange` SSE stream and produces one JSON
message per event into Kafka. Written for the running lab stack
(Spark + Kafka + Jupyter, host networking, broker at 10.8.0.6:29092).

Exam requirements implemented here
----------------------------------
1. Descriptive User-Agent header (Wikimedia policy requires one).
2. Reconnect handling: exponential backoff on disconnects / 5xx / 429, and
   resumption via `Last-Event-ID` so the SSE feed continues where it broke.
3. Only the fields the case study lists are transmitted (projection), with
   `length` kept as a struct so Q3 can compute `length.new - coalesce(length.old, 0)`.
4. Anonymous users (edits whose `user` is an IP address) are hashed with
   SHA-256 + salt before transmission -> `anon:<16 hex chars>`.
5. Hard stop at exactly --minutes (default 12 min) of wall-clock time, then
   flush + close and print a summary that is the evidence for Q1b.

Run from JupyterLab (Terminal) or a notebook cell:

    python3 q1_wikimedia_producer.py

    # cheap smoke test without touching Kafka (parses + prints, sends nothing)
    python3 q1_wikimedia_producer.py --dry-run --minutes 0.5

    # different topic / stop earlier
    python3 q1_wikimedia_producer.py --topic wiki.recentchange.raw --minutes 12

Evidence commands to run afterwards (Q1b) — Kafka 4.x dropped
`kafka.tools.GetOffsetShell`, use `kafka-get-offsets.sh`:

    podman exec skc-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server \
        localhost:29092 --describe --topic wiki.recentchange.raw
    podman exec skc-kafka /opt/kafka/bin/kafka-get-offsets.sh \
        --bootstrap-server localhost:29092 --topic wiki.recentchange.raw --time -1
    podman exec skc-kafka /opt/kafka/bin/kafka-get-offsets.sh \
        --bootstrap-server localhost:29092 --topic wiki.recentchange.raw --time -2
"""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
import signal
import sys
import time
from collections import Counter
from datetime import datetime, timezone

# kafka-python is installed into .pylibs next to this file (kept inside the
# jupyter-work volume so it survives container re-creation).
try:
    _HERE = os.path.dirname(os.path.abspath(__file__))
except NameError:  # pasted into a notebook cell
    _HERE = "/opt/notebooks"
_LIBS = os.path.join(_HERE, ".pylibs")
if os.path.isdir(_LIBS):
    sys.path.insert(0, _LIBS)

import requests  # noqa: E402
from kafka import KafkaProducer  # noqa: E402

# --------------------------------------------------------------------------- #
# Configuration (everything overridable from the command line)
# --------------------------------------------------------------------------- #
STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

# Wikimedia asks for a descriptive UA with a contact address; a bare python
# UA gets throttled with HTTP 429.
USER_AGENT = "AmikomST153-WikiEditMonitor/1.0 (senoaji@students.amikom.ac.id)"

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = "wiki.recentchange.raw"
CONSUMER_GROUP = "wiki-monitor-spark"  # documented here, used by Q2-Q4

# Anonymisation salt. Change it and the same IP yields a different hash;
# keep it fixed for one run campaign so repeat editors collapse into one
# pseudonym (needed for "unique editors" in Q3b-4).
ANON_SALT = os.environ.get("ANON_SALT", "st153-amikom-2026-odd")

# Fields the case study lists. Everything else ($schema, meta, revision,
# title_url, parsedcomment, patrolled, notify_url, server_url, ...) is dropped.
KEEP_FIELDS = (
    "id",
    "type",        # edit | new | log | categorize
    "wiki",        # idwiki, enwiki, wikidatawiki, ...
    "server_name",
    "title",
    "namespace",
    "user",        # hashed below when it is an IP address
    "bot",
    "minor",
    "timestamp",   # Unix epoch seconds
    "comment",
)

PROGRESS_EVERY_S = 15.0
READ_TIMEOUT = (10.0, 20.0)   # connect, read — Wikimedia sends ":ok" heartbeats
MAX_BACKOFF_S = 60.0


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def anonymize_user(user):
    """Hash IP addresses (anonymous editors), leave usernames untouched."""
    if not isinstance(user, str):
        return user
    candidate = user.strip()
    try:
        ipaddress.ip_address(candidate.strip("[]"))
    except ValueError:
        return user  # registered account (or something odd) — keep as is
    digest = hashlib.sha256((ANON_SALT + candidate).encode("utf-8")).hexdigest()[:16]
    return f"anon:{digest}"


def project(event: dict) -> dict:
    """Keep only the required fields and normalise their types."""
    rec = {k: event.get(k) for k in KEEP_FIELDS}

    # length is a struct: {"old": int|null, "new": int|null}. `old` is null for
    # pages created by this event; `length` itself is missing for log/categorize.
    length = event.get("length") or {}
    rec["length"] = {"old": length.get("old"), "new": length.get("new")}

    rec["timestamp"] = int(rec["timestamp"]) if rec.get("timestamp") is not None else None
    rec["bot"] = bool(rec.get("bot"))
    rec["minor"] = bool(rec.get("minor"))
    rec["user"] = anonymize_user(rec.get("user"))
    return rec


def message_key(rec: dict, buckets: int) -> bytes:
    """Partition key. buckets=0 -> plain `wiki` (per-wiki ordering, sees skew)."""
    wiki = rec.get("wiki") or "unknown"
    if buckets <= 0:
        return str(wiki).encode("utf-8")
    # Salted key: spread one hot wiki over `buckets` partitions while keeping
    # ordering within (wiki, bucket).
    return f"{wiki}#{(rec.get('id') or 0) % buckets + 1}".encode("utf-8")


def build_producer(bootstrap: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap.split(","),
        client_id="q1-wikimedia-producer",
        acks="all",                 # durability; on a single broker == acks=1
        retries=5,
        linger_ms=200,              # small batching window -> fewer requests
        batch_size=32 * 1024,
        compression_type="gzip",
        request_timeout_ms=30000,
        max_block_ms=30000,
    )


# --------------------------------------------------------------------------- #
# Main loop
# --------------------------------------------------------------------------- #
stats = {
    "lines": 0, "events": 0, "sent": 0, "skipped": 0,
    "malformed": 0, "reconnects": 0, "send_errors": 0,
    "bytes": 0, "anonymized": 0,
}
per_wiki = Counter()


def _on_delivered(_meta):
    stats["sent"] += 1


def _on_failed(exc):
    stats["send_errors"] += 1
    log(f"delivery failed: {type(exc).__name__}: {exc}")


def ingest(session, producer, args, deadline, opts) -> None:
    """One connection lifetime: read SSE until the deadline or the stream ends."""
    headers = {"User-Agent": USER_AGENT, "Accept": "text/event-stream"}
    if opts["last_event_id"]:
        headers["Last-Event-ID"] = opts["last_event_id"]

    with session.get(
        STREAM_URL, headers=headers, stream=True, timeout=READ_TIMEOUT
    ) as resp:
        if resp.status_code != 200:
            raise RuntimeError(f"HTTP {resp.status_code} from EventStreams")
        log(f"connected (HTTP 200, Last-Event-ID={opts['last_event_id'] or '-'})")

        for raw in resp.iter_lines(decode_unicode=False):
            now = time.monotonic()
            if now >= deadline:
                return "deadline"
            if not raw:
                continue
            line = raw.decode("utf-8", "replace")
            stats["lines"] += 1

            if line.startswith(":"):          # ":ok" heartbeat
                continue
            if line.startswith("id:"):
                opts["last_event_id"] = line[3:].strip()
                continue
            if not line.startswith("data:"):
                continue

            payload = line[5:].strip()
            if not payload.startswith("{"):
                stats["skipped"] += 1
                continue
            try:
                event = json.loads(payload)
            except json.JSONDecodeError:
                stats["malformed"] += 1
                continue
            if not isinstance(event, dict):
                stats["malformed"] += 1
                continue

            stats["events"] += 1
            rec = project(event)
            if str(rec.get("user", "")).startswith("anon:"):
                stats["anonymized"] += 1
            per_wiki[rec.get("wiki")] += 1

            if args.dry_run:
                if stats["events"] % 200 == 1:
                    log(f"dry-run sample: {json.dumps(rec, ensure_ascii=False)[:220]}")
            else:
                key = message_key(rec, args.key_buckets)
                value = json.dumps(rec, separators=(",", ":"),
                                   ensure_ascii=False).encode("utf-8")
                try:
                    (producer.send(args.topic, key=key, value=value)
                     .add_callback(_on_delivered)
                     .add_errback(_on_failed))
                    stats["bytes"] += len(payload)
                except Exception as exc:                      # noqa: BLE001
                    stats["send_errors"] += 1
                    log(f"send failed: {type(exc).__name__}: {exc}")
        return "eof"


def main() -> int:
    ap = argparse.ArgumentParser(description="Wikimedia SSE -> Kafka producer (Q1b)")
    ap.add_argument("--minutes", type=float, default=12.0,
                    help="hard wall-clock stop, in minutes (default: 12)")
    ap.add_argument("--topic", default=TOPIC)
    ap.add_argument("--bootstrap", default=BOOTSTRAP)
    ap.add_argument("--key-buckets", type=int, default=0,
                    help="0 = key on wiki; N = salted wiki#(id%%N) over N partitions")
    ap.add_argument("--dry-run", action="store_true",
                    help="parse and print, do not send anything to Kafka")
    ap.add_argument("--limit", type=int, default=0,
                    help="stop after N events (0 = only the time limit applies)")
    args = ap.parse_args()

    minutes = max(args.minutes, 0.02)
    started = time.monotonic()
    deadline = started + minutes * 60.0
    stop = {"flag": False}

    def _shutdown(signum, _frame):
        stop["flag"] = True
        log(f"signal {signum} received — stopping early, flushing")

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    log(f"target={STREAM_URL}")
    log(f"kafka={args.bootstrap} topic={args.topic} "
        f"key={'wiki' if args.key_buckets == 0 else f'wiki#id%{args.key_buckets}'}"
        f"{' [DRY RUN]' if args.dry_run else ''}")
    log(f"window={minutes:.2f} min, hard stop at "
        f"{datetime.fromtimestamp(time.time() + minutes * 60).strftime('%H:%M:%S')}")

    producer = None if args.dry_run else build_producer(args.bootstrap)
    session = requests.Session()
    opts = {"last_event_id": None}
    backoff = 1.0
    last_progress = started

    try:
        while time.monotonic() < deadline and not stop["flag"]:
            remaining = deadline - time.monotonic()
            if remaining <= 0.5:
                break
            try:
                reason = ingest(session, producer, args, deadline, opts)
                if reason == "deadline" or time.monotonic() >= deadline:
                    break
                # iter_lines ended normally (server closed the stream) — reconnect
                stats["reconnects"] += 1
                log("stream closed by server — reconnecting")
                time.sleep(min(2.0, max(0.0, deadline - time.monotonic())))
                backoff = 1.0
            except (requests.RequestException, RuntimeError) as exc:
                if time.monotonic() >= deadline or stop["flag"]:
                    break
                stats["reconnects"] += 1
                wait = min(backoff, MAX_BACKOFF_S, max(0.0, deadline - time.monotonic()))
                log(f"connection problem ({type(exc).__name__}: {exc}); "
                    f"retry in {wait:.0f}s")
                time.sleep(wait)
                backoff = min(backoff * 2, MAX_BACKOFF_S)
            finally:
                now = time.monotonic()
                if now - last_progress >= PROGRESS_EVERY_S:
                    last_progress = now
                    log(f"elapsed={now - started:6.1f}s "
                        f"events={stats['events']} sent={stats['sent']} "
                        f"rate={stats['events'] / max(now - started, 1e-9):5.1f}/s")
            if args.limit and stats["events"] >= args.limit:
                break
    finally:
        session.close()
        if producer is not None:
            log("flushing producer…")
            try:
                producer.flush(timeout=60)
            except Exception as exc:                          # noqa: BLE001
                log(f"flush error: {type(exc).__name__}: {exc}")
            producer.close(timeout=30)

    elapsed = time.monotonic() - started
    delivered = stats["sent"] if not args.dry_run else stats["events"]
    log("=" * 68)
    log(f"STOPPED after {elapsed / 60:.2f} min ({elapsed:.1f}s) — target was {minutes:.2f} min")
    log(f"  events parsed      : {stats['events']}")
    log(f"  messages delivered : {delivered}"
        f"{' (dry-run: not sent)' if args.dry_run else ''}")
    log(f"  throughput         : {delivered / max(elapsed, 1e-9):.1f} msg/s")
    log(f"  bytes sent         : {stats['bytes'] / 1024 / 1024:.2f} MiB")
    log(f"  anonymised IPs     : {stats['anonymized']}")
    log(f"  malformed JSON     : {stats['malformed']}  skipped lines: {stats['skipped']}")
    log(f"  send errors        : {stats['send_errors']}")
    log(f"  reconnects         : {stats['reconnects']}")
    log(f"  top wikis          : {per_wiki.most_common(5)}")
    if not args.dry_run:
        log(f"  next: kafka-get-offsets.sh --bootstrap-server {args.bootstrap} "
            f"--topic {args.topic} --time -1")
    return 0


if __name__ == "__main__":
    sys.exit(main())
