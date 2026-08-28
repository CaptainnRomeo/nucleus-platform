"""ledger-worker - consumes payment events, writes ledger entries.

Demonstrates the consumer-side concerns that actually matter in production:
  - manual offset commits (at-least-once, not at-most-once)
  - tunable processing time so lag can be manufactured on demand
  - graceful shutdown that closes the consumer to trigger a clean rebalance
"""
import json
import logging
import os
import signal
import time

from confluent_kafka import Consumer, KafkaError
from prometheus_client import Counter, Gauge, Histogram, start_http_server

logging.basicConfig(level=logging.INFO, format='{"ts":"%(asctime)s","msg":"%(message)s"}')
log = logging.getLogger("ledger-worker")

BROKER = os.getenv("KAFKA_BOOTSTRAP", "kafka.kafka.svc.cluster.local:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "payments.events")
GROUP = os.getenv("KAFKA_GROUP", "ledger-workers")
# Seconds per message. Raise this to manufacture lag without touching Kafka.
PROC_TIME = float(os.getenv("PROCESSING_TIME_S", "0.05"))

PROCESSED = Counter("ledger_messages_processed_total", "Messages processed", ["partition"])
FAILED = Counter("ledger_messages_failed_total", "Processing failures")
PROC_LATENCY = Histogram("ledger_processing_seconds", "Per-message processing time")
ASSIGNED = Gauge("ledger_assigned_partitions", "Partitions assigned to this consumer")

_running = True


def _shutdown(signum, frame):
    global _running
    log.info("SIGTERM received, finishing current message then leaving group")
    _running = False


signal.signal(signal.SIGTERM, _shutdown)
signal.signal(signal.SIGINT, _shutdown)


def on_assign(consumer, partitions):
    ASSIGNED.set(len(partitions))
    log.info(f"assigned partitions: {[p.partition for p in partitions]}")


def on_revoke(consumer, partitions):
    ASSIGNED.set(0)
    log.info(f"revoked partitions: {[p.partition for p in partitions]}")


def main():
    start_http_server(8080)

    consumer = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": GROUP,
        "auto.offset.reset": "earliest",
        # Manual commits. With enable.auto.commit=true the offset can be
        # committed BEFORE processing finishes - so a crash loses the message.
        # That is at-most-once. Payments need at-least-once.
        "enable.auto.commit": False,
        # Cooperative rebalancing: only the partitions that actually move get
        # revoked, instead of every consumer dropping everything and
        # re-acquiring. The old eager protocol caused stop-the-world pauses.
        "partition.assignment.strategy": "cooperative-sticky",
        # If a poll loop takes longer than this, the broker assumes the
        # consumer is dead and rebalances - causing a rebalance storm.
        # Must exceed worst-case batch processing time.
        "max.poll.interval.ms": 300000,
        "session.timeout.ms": 45000,
    })

    consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)
    log.info(f"consuming {TOPIC} group={GROUP} proc_time={PROC_TIME}s")

    try:
        while _running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error(f"consume error: {msg.error()}")
                    FAILED.inc()
                continue

            with PROC_LATENCY.time():
                try:
                    json.loads(msg.value().decode())
                except Exception:
                    FAILED.inc()
                time.sleep(PROC_TIME)

            # Commit AFTER processing. At-least-once: a crash between
            # processing and commit replays the message, so downstream
            # writes must be idempotent - same reason payment-api uses
            # idempotency keys.
            consumer.commit(msg, asynchronous=False)
            PROCESSED.labels(partition=str(msg.partition())).inc()
    finally:
        # close() triggers a clean group-leave so the rebalance happens
        # immediately instead of after session.timeout.ms expires.
        consumer.close()
        log.info("consumer closed")


if __name__ == "__main__":
    main()
