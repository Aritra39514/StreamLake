"""
Fake order event producer — generates realistic e-commerce orders and
publishes them to a Kafka topic at a configurable rate.

Demonstrates:
  - Idempotent producer (enable.idempotence=True)  → exactly-once delivery
  - Key-based partitioning (customer_id as key)     → related events in order
  - Snappy compression                              → ~3x smaller on the wire
  - AdminClient topic auto-creation
"""
import argparse
import json
import logging
import random
import sys
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("producer")

PRODUCTS = {
    "prod-001": 9.99,
    "prod-002": 24.99,
    "prod-003": 49.99,
    "prod-004": 99.99,
    "prod-005": 199.99,
}
STATUSES = ["PLACED", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"]


# ── topic helpers ──────────────────────────────────────────────────────────────

def ensure_topic(servers: str, topic: str, partitions: int = 3):
    admin = AdminClient({"bootstrap.servers": servers})
    existing = admin.list_topics(timeout=10).topics
    if topic in existing:
        logger.info("Topic '%s' already exists (%d partitions)", topic, partitions)
        return
    logger.info("Creating topic '%s' with %d partitions…", topic, partitions)
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=partitions, replication_factor=1)]
    )
    for t, fut in futures.items():
        try:
            fut.result()
            logger.info("Topic '%s' created", t)
        except Exception as exc:
            logger.warning("Could not create topic '%s': %s", t, exc)


# ── order factory ──────────────────────────────────────────────────────────────

def make_order() -> dict:
    product_id = random.choice(list(PRODUCTS))
    quantity = random.randint(1, 5)
    return {
        "order_id":    str(uuid.uuid4()),
        "customer_id": f"cust-{random.randint(1000, 9999)}",
        "product_id":  product_id,
        "quantity":    quantity,
        "price":       round(PRODUCTS[product_id] * quantity, 2),
        "status":      random.choice(STATUSES),
        "event_time":  datetime.now(timezone.utc).isoformat(),
    }


# ── delivery callback ──────────────────────────────────────────────────────────

def _on_delivery(err, msg):
    if err:
        logger.error("Delivery failed for offset %s: %s", msg.offset(), err)


# ── main ───────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="StreamLake order event producer")
    p.add_argument("--servers", default="localhost:9092", help="Kafka bootstrap servers")
    p.add_argument("--topic",   default="orders",         help="Target topic name")
    p.add_argument("--rate",    type=float, default=2.0,  help="Events per second")
    p.add_argument("--count",   type=int,   default=0,    help="Total events (0 = infinite)")
    args = p.parse_args()

    ensure_topic(args.servers, args.topic)

    producer = Producer({
        "bootstrap.servers":     args.servers,
        "acks":                  "all",        # wait for all replicas
        "enable.idempotence":    True,         # exactly-once to the broker
        "compression.type":      "snappy",
        "linger.ms":             5,            # micro-batching
        "batch.size":            65536,
    })

    interval = 1.0 / args.rate
    sent = 0

    logger.info(
        "Producing to topic='%s' at %.1f events/sec  (Ctrl-C to stop)",
        args.topic, args.rate,
    )

    try:
        while args.count == 0 or sent < args.count:
            order = make_order()
            producer.produce(
                topic=args.topic,
                key=order["customer_id"].encode(),
                value=json.dumps(order).encode(),
                callback=_on_delivery,
            )
            producer.poll(0)  # trigger delivery callbacks without blocking
            sent += 1
            if sent % 100 == 0:
                logger.info("Produced %d events", sent)
            time.sleep(interval)

    except KeyboardInterrupt:
        logger.info("\nStopping — flushing remaining messages…")

    finally:
        producer.flush(timeout=15)
        logger.info("Done. Total produced: %d", sent)


if __name__ == "__main__":
    main()
