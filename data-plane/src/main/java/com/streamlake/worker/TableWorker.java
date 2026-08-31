package com.streamlake.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamlake.iceberg.IcebergTableWriter;
import com.streamlake.model.Order;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * One worker thread per (Kafka topic → Iceberg table) mapping.
 *
 * At-least-once write guarantee
 * ──────────────────────────────
 *  1. Buffer messages in memory.
 *  2. writer.append(buffer)           ← atomic Iceberg snapshot commit
 *  3. consumer.commitSync(offsets)    ← Kafka offset commit AFTER write succeeds
 *
 *  Crash between 2 and 3: Kafka re-delivers the batch; a duplicate Iceberg
 *  snapshot is written. Duplicates are deduplicable via (kafka_partition,
 *  kafka_offset). This is identical to how Confluent Tableflow works.
 *
 * Resilience properties
 * ──────────────────────
 *  - running is an AtomicBoolean initialized TRUE at construction so a
 *    stop() call issued before the executor schedules run() is not lost.
 *  - ConsumerRebalanceListener flushes the buffer and commits offsets before
 *    partitions are revoked, preventing CommitFailedException from killing
 *    the worker during a rebalance.
 *  - commitSync() retries on RetriableException with exponential backoff
 *    so transient broker hiccups don't permanently kill the consumer thread.
 *  - resetForRestart() allows the WorkerRegistry supervisor to restart a
 *    failed worker without creating a new object (preserving the map entry).
 */
public class TableWorker implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(TableWorker.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final int  BATCH_SIZE       = 100;
    private static final long BATCH_TIMEOUT_MS = 5_000;

    // ── identity ──────────────────────────────────────────────────────────────
    private final String workerId;
    private final String topic;
    private final String tableName;
    private final String bootstrapServers;
    private final String warehousePath;

    // ── lifecycle ─────────────────────────────────────────────────────────────
    // Initialized TRUE so stop() before run() is honoured — not clobbered by
    // a later `running = true` assignment inside run().
    private final AtomicBoolean   running      = new AtomicBoolean(true);
    private final AtomicReference<String> status = new AtomicReference<>("starting");
    private final AtomicLong recordsWritten = new AtomicLong();
    private final AtomicInteger restartCount = new AtomicInteger(0);
    private volatile String error = null;

    public TableWorker(String workerId, String topic, String tableName,
                       String bootstrapServers, String warehousePath) {
        this.workerId         = workerId;
        this.topic            = topic;
        this.tableName        = tableName;
        this.bootstrapServers = bootstrapServers;
        this.warehousePath    = warehousePath;
    }

    // ── control ───────────────────────────────────────────────────────────────

    public void stop() {
        running.set(false);
        log.info("[{}] Stop requested", workerId);
    }

    /** Called by WorkerRegistry supervisor before re-submitting to the executor. */
    public void resetForRestart() {
        running.set(true);
        error = null;
        status.set("starting");
    }

    // ── main loop ─────────────────────────────────────────────────────────────

    @Override
    public void run() {
        if (!running.get()) {
            log.info("[{}] Stopped before run() started — skipping", workerId);
            status.set("stopped");
            return;
        }
        log.info("[{}] Starting  topic={} → table={}", workerId, topic, tableName);
        status.set("running");

        try (IcebergTableWriter writer = new IcebergTableWriter(warehousePath, tableName)) {
            KafkaConsumer<String, String> consumer = buildConsumer();

            List<Order> buffer = new ArrayList<>(BATCH_SIZE);
            // Tracks the highest seen offset per partition for precise commitSync calls.
            Map<TopicPartition, OffsetAndMetadata> pendingOffsets = new HashMap<>();

            // Flush on revocation so in-flight records aren't lost when partitions
            // are reassigned during a scale-out or rolling restart.
            ConsumerRebalanceListener rebalanceListener = new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    if (buffer.isEmpty()) return;
                    log.info("[{}] Rebalance: flushing {} records before partition revocation",
                        workerId, buffer.size());
                    try {
                        int count = writer.append(new ArrayList<>(buffer));
                        if (!pendingOffsets.isEmpty()) {
                            consumer.commitSync(pendingOffsets);
                        }
                        recordsWritten.addAndGet(count);
                        buffer.clear();
                        pendingOffsets.clear();
                    } catch (Exception e) {
                        log.error("[{}] Flush on revocation failed: {}", workerId, e.getMessage(), e);
                    }
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    buffer.clear();
                    pendingOffsets.clear();
                    log.info("[{}] Partitions assigned: {}", workerId, partitions);
                }
            };

            consumer.subscribe(List.of(topic), rebalanceListener);
            long lastFlush = System.currentTimeMillis();

            while (running.get()) {
                ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(300));

                for (ConsumerRecord<String, String> rec : records) {
                    try {
                        Order order = MAPPER.readValue(rec.value(), Order.class);
                        order.setKafkaOffset(rec.offset());
                        order.setKafkaPartition(rec.partition());
                        buffer.add(order);
                        pendingOffsets.put(
                            new TopicPartition(rec.topic(), rec.partition()),
                            new OffsetAndMetadata(rec.offset() + 1)
                        );
                    } catch (Exception e) {
                        log.warn("[{}] Skipping bad message offset={}: {}",
                            workerId, rec.offset(), e.getMessage());
                    }
                }

                boolean sizeReached    = buffer.size() >= BATCH_SIZE;
                boolean timeoutReached = !buffer.isEmpty() &&
                    (System.currentTimeMillis() - lastFlush) >= BATCH_TIMEOUT_MS;

                if (sizeReached || timeoutReached) {
                    flush(writer, consumer, buffer, pendingOffsets);
                    buffer.clear();
                    pendingOffsets.clear();
                    lastFlush = System.currentTimeMillis();
                }
            }

            // Drain remaining records on graceful stop
            if (!buffer.isEmpty()) {
                flush(writer, consumer, buffer, pendingOffsets);
            }
            consumer.close();

        } catch (Exception e) {
            log.error("[{}] Fatal: {}", workerId, e.getMessage(), e);
            error = e.getMessage();
            status.set("error");
            return;
        }

        status.set("stopped");
        log.info("[{}] Stopped. Total records written: {}", workerId, recordsWritten.get());
    }

    // ── flush ─────────────────────────────────────────────────────────────────

    private void flush(IcebergTableWriter writer,
                       KafkaConsumer<String, String> consumer,
                       List<Order> buffer,
                       Map<TopicPartition, OffsetAndMetadata> offsets) throws IOException {
        long t0 = System.currentTimeMillis();

        // Write to Iceberg first — creates an immutable snapshot
        int count = writer.append(new ArrayList<>(buffer));

        // commitSync with exponential backoff on transient broker errors.
        // Only transient (RetriableException) errors are retried; fatal errors
        // (AuthorizationException, etc.) propagate and stop the worker.
        long backoffMs = 1_000;
        for (int attempt = 0; attempt <= 5; attempt++) {
            try {
                consumer.commitSync(offsets);
                break;
            } catch (RetriableException e) {
                if (attempt == 5) throw e;
                log.warn("[{}] commitSync retriable (attempt {}), backing off {}ms: {}",
                    workerId, attempt + 1, backoffMs, e.getMessage());
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(ie);
                }
                backoffMs = Math.min(backoffMs * 2, 30_000);
            }
        }

        recordsWritten.addAndGet(count);
        log.info("[{}] Flushed {} records in {}ms  (total: {})",
            workerId, count, System.currentTimeMillis() - t0, recordsWritten.get());
    }

    // ── consumer config ───────────────────────────────────────────────────────

    private KafkaConsumer<String, String> buildConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,                 "streamlake-" + workerId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,         500);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,       30_000);
        return new KafkaConsumer<>(props);
    }

    // ── accessors ─────────────────────────────────────────────────────────────

    public String  getWorkerId()           { return workerId; }
    public String  getTopic()              { return topic; }
    public String  getTableName()          { return tableName; }
    public String  getStatus()             { return status.get(); }
    public long    getRecordsWritten()     { return recordsWritten.get(); }
    public String  getError()             { return error; }
    public int     getRestartCount()      { return restartCount.get(); }
    public int     incrementRestartCount() { return restartCount.incrementAndGet(); }
}
