package com.streamlake.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Thread-safe registry that owns the lifecycle of all TableWorkers.
 *
 * Key distributed-systems properties:
 *  - TOCTOU-free: putIfAbsent atomically guards against duplicate workers
 *    for the same ID under concurrent POST /workers requests.
 *  - Supervisor pattern: a background watchdog restarts workers that exit
 *    with status='error' (transient Kafka/Iceberg failures) up to MAX_RESTARTS.
 *  - Bounded thread pool: 200-thread ceiling avoids unbounded native memory
 *    growth. Use virtual threads on Java 21+ to remove the ceiling entirely.
 */
public class WorkerRegistry {

    private static final Logger log = LoggerFactory.getLogger(WorkerRegistry.class);
    private static final int MAX_RESTARTS = 5;

    private final Map<String, TableWorker> workers = new ConcurrentHashMap<>();

    // Bounded pool: each worker occupies one thread. 200 is a pragmatic ceiling;
    // CallerRunsPolicy turns start() into a blocking call instead of silently
    // dropping the 201st worker.
    private final ExecutorService executor = new ThreadPoolExecutor(
        0, 200,
        60L, TimeUnit.SECONDS,
        new SynchronousQueue<>(),
        r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        },
        new ThreadPoolExecutor.CallerRunsPolicy()
    );

    // Single-threaded watchdog — runs supervise() every 10 s.
    private final ScheduledExecutorService watchdog =
        Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "worker-watchdog");
            t.setDaemon(true);
            return t;
        });

    private final String bootstrapServers;
    private final String warehousePath;

    public WorkerRegistry(String bootstrapServers, String warehousePath) {
        this.bootstrapServers = bootstrapServers;
        this.warehousePath    = warehousePath;
        watchdog.scheduleAtFixedRate(this::supervise, 10, 10, TimeUnit.SECONDS);
    }

    /**
     * Starts a new worker for the given (workerId, topic, tableName) triple.
     * Atomically guards against duplicate registrations using putIfAbsent —
     * eliminates the containsKey + put TOCTOU race present in a naive guard.
     */
    public TableWorker start(String workerId, String topic, String tableName) {
        TableWorker worker = new TableWorker(
            workerId, topic, tableName, bootstrapServers, warehousePath);
        TableWorker prev = workers.putIfAbsent(workerId, worker);
        if (prev != null) {
            throw new IllegalStateException("Worker " + workerId + " already exists");
        }
        executor.submit(worker);
        log.info("Started worker {}  ({} → {})", workerId, topic, tableName);
        return worker;
    }

    /**
     * Signals the worker to stop. The worker's run() loop will drain and exit.
     * The map entry is removed by the supervisor once status reaches 'stopped'.
     */
    public boolean stop(String workerId) {
        TableWorker worker = workers.get(workerId);
        if (worker == null) return false;
        worker.stop();
        log.info("Stop signalled for worker {}", workerId);
        return true;
    }

    public TableWorker get(String workerId) {
        return workers.get(workerId);
    }

    public Collection<TableWorker> all() {
        return workers.values();
    }

    public void stopAll() {
        workers.values().forEach(TableWorker::stop);
        watchdog.shutdownNow();
        executor.shutdown();
        try {
            if (!executor.awaitTermination(15, TimeUnit.SECONDS)) {
                log.warn("Some workers did not stop cleanly within 15s");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ── supervisor ────────────────────────────────────────────────────────────

    private void supervise() {
        for (Map.Entry<String, TableWorker> entry : workers.entrySet()) {
            TableWorker w = entry.getValue();
            String s = w.getStatus();

            if ("error".equals(s)) {
                int restarts = w.getRestartCount();
                if (restarts >= MAX_RESTARTS) {
                    log.error("[{}] Max restarts ({}) reached — giving up",
                        w.getWorkerId(), MAX_RESTARTS);
                    continue;
                }
                int attempt = w.incrementRestartCount();
                log.warn("[{}] Worker in error state — restarting (attempt {}/{})",
                    w.getWorkerId(), attempt, MAX_RESTARTS);
                w.resetForRestart();
                executor.submit(w);

            } else if ("stopped".equals(s)) {
                workers.remove(entry.getKey());
                log.info("[{}] Worker stopped cleanly — removed from registry", w.getWorkerId());
            }
        }
    }
}
