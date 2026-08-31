package com.streamlake;

import com.streamlake.producer.OrderProducer;
import com.streamlake.server.DataPlaneServer;
import com.streamlake.streams.OrderMetricsStream;
import com.streamlake.worker.WorkerRegistry;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for the Java data plane.
 *
 * Usage:
 *   java -jar data-plane-1.0.0.jar server    ← data plane HTTP API on :9090
 *   java -jar data-plane-1.0.0.jar producer  ← fake order event producer
 *   java -jar data-plane-1.0.0.jar streams   ← Kafka Streams metrics app
 *
 * Environment variables:
 *   KAFKA_BOOTSTRAP_SERVERS  default: localhost:9092
 *   WAREHOUSE_PATH           default: warehouse/
 *   DATA_PLANE_PORT          default: 9090
 *   METRICS_PORT             default: 8001
 */
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        String command          = args.length > 0 ? args[0] : "server";
        String bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String warehousePath    = env("WAREHOUSE_PATH",           "warehouse");
        int    dataPlanePort    = Integer.parseInt(env("DATA_PLANE_PORT", "9090"));
        int    metricsPort      = Integer.parseInt(env("METRICS_PORT",    "8001"));

        switch (command) {
            case "server"   -> runServer(bootstrapServers, warehousePath, dataPlanePort, metricsPort);
            case "producer" -> runProducer(bootstrapServers);
            case "streams"  -> runStreams(bootstrapServers);
            default -> {
                System.err.println("Usage: java -jar data-plane.jar [server|producer|streams]");
                System.exit(1);
            }
        }
    }

    // ── server ─────────────────────────────────────────────────────────────────

    private static void runServer(String bootstrapServers,
                                  String warehousePath,
                                  int port,
                                  int metricsPort) throws Exception {
        // Export JVM stats (GC pause, heap, thread counts) to Prometheus
        DefaultExports.initialize();
        HTTPServer metricsServer = new HTTPServer(metricsPort);

        WorkerRegistry registry = new WorkerRegistry(bootstrapServers, warehousePath);
        DataPlaneServer server  = new DataPlaneServer(port, registry);
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received — stopping workers…");
            registry.stopAll();
            try {
                server.stop();
                metricsServer.close();
            } catch (Exception e) {
                log.error("Error during shutdown", e);
            }
        }));

        log.info("╔══════════════════════════════════════════════╗");
        log.info("║  StreamLake  Data Plane  (Java)              ║");
        log.info("╠══════════════════════════════════════════════╣");
        log.info("║  Workers API  →  http://localhost:{}       ║", port);
        log.info("║  Metrics      →  http://localhost:{}/metrics ║", metricsPort);
        log.info("╚══════════════════════════════════════════════╝");

        // Park the main thread — workers run on daemon threads
        Thread.currentThread().join();
    }

    // ── producer ───────────────────────────────────────────────────────────────

    private static void runProducer(String bootstrapServers) throws Exception {
        double ratePerSec  = Double.parseDouble(env("PRODUCER_RATE",  "5.0"));
        long   totalEvents = Long.parseLong(env("PRODUCER_COUNT", "0"));   // 0 = infinite
        new OrderProducer(bootstrapServers, "orders").produce(ratePerSec, totalEvents);
    }

    // ── streams ────────────────────────────────────────────────────────────────

    private static void runStreams(String bootstrapServers) throws Exception {
        OrderMetricsStream app = new OrderMetricsStream(bootstrapServers);
        app.start();
        Runtime.getRuntime().addShutdownHook(new Thread(app::stop));
        Thread.currentThread().join();
    }

    // ── helper ─────────────────────────────────────────────────────────────────

    private static String env(String key, String fallback) {
        String v = System.getenv(key);
        return (v != null && !v.isBlank()) ? v : fallback;
    }
}
