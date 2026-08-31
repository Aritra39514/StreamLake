"""
Data plane worker — one thread per Kafka topic → Iceberg table mapping.

Exactly-once guarantee approach
────────────────────────────────
True cross-system atomic commits require a 2PC coordinator. Here we use
at-least-once with idempotent Iceberg appends instead:

  1. Buffer Kafka messages in memory.
  2. Append the batch to Iceberg (creates a new immutable snapshot).
  3. Only after the Iceberg write succeeds, commit the Kafka offset back
     to both the broker (consumer.commit) and our local SQLite state.
  4. On restart the worker reads the last committed offset from SQLite
     and seeks the Kafka consumer to exactly that position — so any
     un-committed messages are re-consumed and re-written (at-least-once).

Because Iceberg snapshots are immutable and identified by sequence number,
duplicate rows from a retry are detectable via the kafka_offset column.
"""
import json
import logging
import time
import threading
from typing import Dict

from confluent_kafka import Consumer, KafkaError, TopicPartition

from .config import settings
from .database import (
    get_committed_offsets,
    save_committed_offsets,
    update_table_status,
)
from .iceberg_writer import IcebergWriter
from .metrics import ACTIVE_WORKERS, COMMIT_LATENCY, RECORDS_PROCESSED

logger = logging.getLogger(__name__)


class TableWorker(threading.Thread):
    """Reads from one Kafka topic and materialises it as one Iceberg table."""

    def __init__(self, table_id: str, topic: str, table_name: str):
        super().__init__(daemon=True, name=f"worker-{table_id}")
        self.table_id = table_id
        self.topic = topic
        self.table_name = table_name
        self._stop = threading.Event()
        self.records_written = 0
        self._committed_offsets: Dict[int, int] = {}

    def stop(self):
        self._stop.set()

    # ── main loop ──────────────────────────────────────────────────────────────

    def run(self):
        logger.info("[%s] Starting — topic=%s table=%s", self.table_id, self.topic, self.table_name)
        update_table_status(self.table_id, "running")
        ACTIVE_WORKERS.inc()

        writer = consumer = None
        try:
            writer = IcebergWriter(self.table_name)
            consumer = self._build_consumer()
            self._assign_with_resume(consumer)
            self._poll_loop(writer, consumer)
        except Exception as exc:
            logger.error("[%s] Fatal error: %s", self.table_id, exc, exc_info=True)
            update_table_status(self.table_id, "error", self.records_written, str(exc))
        finally:
            ACTIVE_WORKERS.dec()
            if consumer:
                consumer.close()
            if self._stop.is_set():
                update_table_status(self.table_id, "stopped", self.records_written)
                logger.info("[%s] Stopped. Total records: %d", self.table_id, self.records_written)

    # ── consumer setup ─────────────────────────────────────────────────────────

    def _build_consumer(self) -> Consumer:
        return Consumer({
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": f"streamlake-{self.table_id}",
            "enable.auto.commit": False,   # we commit manually after Iceberg write
            "auto.offset.reset": "earliest",
            "session.timeout.ms": 30_000,
            "max.poll.interval.ms": 300_000,
        })

    def _assign_with_resume(self, consumer: Consumer):
        """
        Assign partitions explicitly and seek to the last committed offset.
        This bypasses the broker-side consumer group offsets entirely — our
        SQLite table is the single source of truth for committed positions.
        """
        meta = consumer.list_topics(self.topic, timeout=15)
        if self.topic not in meta.topics:
            raise RuntimeError(f"Topic '{self.topic}' not found in Kafka")

        saved = get_committed_offsets(self.table_id, self.topic)
        partitions = list(meta.topics[self.topic].partitions.keys())
        assignments = [
            TopicPartition(self.topic, p, saved.get(p, 0))
            for p in partitions
        ]
        consumer.assign(assignments)
        logger.info(
            "[%s] Assigned %d partition(s), resuming from offsets: %s",
            self.table_id, len(assignments), saved,
        )

    # ── poll loop ──────────────────────────────────────────────────────────────

    def _poll_loop(self, writer: IcebergWriter, consumer: Consumer):
        buffer: list = []
        last_flush = time.monotonic()

        while not self._stop.is_set():
            msg = consumer.poll(timeout=0.3)

            if msg is None:
                pass
            elif msg.error():
                code = msg.error().code()
                if code != KafkaError._PARTITION_EOF:
                    raise RuntimeError(f"Kafka consumer error: {msg.error()}")
            else:
                try:
                    record = json.loads(msg.value().decode("utf-8"))
                    record["kafka_offset"] = msg.offset()
                    record["kafka_partition"] = msg.partition()
                    buffer.append(record)
                    self._committed_offsets[msg.partition()] = msg.offset()
                except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                    logger.warning(
                        "[%s] Skipping unreadable message offset=%d: %s",
                        self.table_id, msg.offset(), exc,
                    )

            elapsed = time.monotonic() - last_flush
            if buffer and (
                len(buffer) >= settings.batch_size
                or elapsed >= settings.batch_timeout_seconds
            ):
                self._flush(writer, consumer, buffer)
                buffer.clear()
                last_flush = time.monotonic()
                update_table_status(self.table_id, "running", self.records_written)

        # graceful drain on stop
        if buffer:
            self._flush(writer, consumer, buffer)

    # ── flush ──────────────────────────────────────────────────────────────────

    def _flush(self, writer: IcebergWriter, consumer: Consumer, buffer: list):
        t0 = time.monotonic()

        # Step 1 — write to Iceberg (creates an immutable snapshot)
        count = writer.append(buffer)

        # Step 2 — commit Kafka offsets only after the Iceberg write succeeds
        tps = [
            TopicPartition(self.topic, p, o + 1)
            for p, o in self._committed_offsets.items()
        ]
        consumer.commit(offsets=tps, asynchronous=False)

        # Step 3 — persist offsets to SQLite so we can resume after a crash
        save_committed_offsets(self.table_id, self.topic, self._committed_offsets)

        self.records_written += count
        elapsed = time.monotonic() - t0
        RECORDS_PROCESSED.labels(table=self.table_name).inc(count)
        COMMIT_LATENCY.labels(table=self.table_name).observe(elapsed)

        logger.info(
            "[%s] Flushed %d records to Iceberg in %.2fs  (total: %d)",
            self.table_id, count, elapsed, self.records_written,
        )
