package com.streamlake.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamlake.worker.TableWorker;
import com.streamlake.worker.WorkerRegistry;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Lightweight HTTP server (Java built-in HttpServer, zero extra deps)
 * that exposes the data plane's internal management API.
 *
 *  POST   /workers              start a new TableWorker
 *  GET    /workers              list all workers + live status
 *  GET    /workers/{id}         get one worker
 *  DELETE /workers/{id}         stop a worker
 *  GET    /health               liveness probe
 *
 * The Go control plane calls these endpoints to orchestrate workers.
 * This is the exact split Confluent uses: Go for the user-facing API and
 * lifecycle coordination, Java for the Kafka + Iceberg data path.
 */
public class DataPlaneServer {

    private static final Logger log = LoggerFactory.getLogger(DataPlaneServer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final HttpServer server;
    private final WorkerRegistry registry;

    public DataPlaneServer(int port, WorkerRegistry registry) throws IOException {
        this.registry = registry;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.server.createContext("/workers", new WorkersHandler());
        this.server.createContext("/health",  new HealthHandler());
        // Explicit thread pool — setExecutor(null) uses a serial inline executor,
        // not a pool, so any blocking handler would stall all other requests.
        int threads = Math.max(4, Runtime.getRuntime().availableProcessors() * 2);
        this.server.setExecutor(Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r, "http-handler");
            t.setDaemon(true);
            return t;
        }));
    }

    public void start() {
        server.start();
        log.info("Data plane HTTP server listening on :{}", server.getAddress().getPort());
    }

    public void stop() {
        server.stop(2);
    }

    // ── /workers handler ──────────────────────────────────────────────────────

    private class WorkersHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange ex) throws IOException {
            String method   = ex.getRequestMethod();
            String pathInfo = ex.getRequestURI().getPath().replaceFirst("^/workers", "");

            try {
                switch (method) {
                    case "POST"   -> handlePost(ex);
                    case "GET"    -> handleGet(ex, pathInfo);
                    case "DELETE" -> handleDelete(ex, pathInfo);
                    default       -> send(ex, 405, "{\"error\":\"method not allowed\"}");
                }
            } catch (Exception e) {
                log.error("Handler error", e);
                send(ex, 500, "{\"error\":\"" + e.getMessage() + "\"}");
            }
        }

        private void handlePost(HttpExchange ex) throws IOException {
            @SuppressWarnings("unchecked")
            Map<String, String> body = MAPPER.readValue(ex.getRequestBody(), Map.class);
            String workerId  = body.get("worker_id");
            String topic     = body.get("topic");
            String tableName = body.get("table_name");

            if (workerId == null || topic == null || tableName == null) {
                send(ex, 400, "{\"error\":\"worker_id, topic, table_name required\"}");
                return;
            }

            try {
                TableWorker worker = registry.start(workerId, topic, tableName);
                send(ex, 201, MAPPER.writeValueAsString(toMap(worker)));
            } catch (IllegalStateException e) {
                // Worker already exists — return the existing one (idempotent)
                TableWorker existing = registry.get(workerId);
                if (existing != null) {
                    send(ex, 409, MAPPER.writeValueAsString(toMap(existing)));
                } else {
                    send(ex, 409, "{\"error\":\"" + e.getMessage() + "\"}");
                }
            }
        }

        private void handleGet(HttpExchange ex, String pathInfo) throws IOException {
            if (pathInfo == null || pathInfo.isEmpty() || pathInfo.equals("/")) {
                List<Map<String, Object>> all = registry.all().stream()
                    .map(DataPlaneServer.this::toMap)
                    .collect(Collectors.toList());
                send(ex, 200, MAPPER.writeValueAsString(all));
            } else {
                String id = pathInfo.substring(1); // strip leading /
                TableWorker worker = registry.get(id);
                if (worker == null) {
                    send(ex, 404, "{\"error\":\"worker not found\"}");
                } else {
                    send(ex, 200, MAPPER.writeValueAsString(toMap(worker)));
                }
            }
        }

        private void handleDelete(HttpExchange ex, String pathInfo) throws IOException {
            if (pathInfo == null || pathInfo.isEmpty() || pathInfo.equals("/")) {
                send(ex, 400, "{\"error\":\"worker id required\"}");
                return;
            }
            String id = pathInfo.substring(1);
            boolean stopped = registry.stop(id);
            send(ex, stopped ? 204 : 404, "");
        }
    }

    // ── /health handler ───────────────────────────────────────────────────────

    private class HealthHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange ex) throws IOException {
            String body = String.format(
                "{\"status\":\"ok\",\"workers\":%d}", registry.all().size());
            send(ex, 200, body);
        }
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private Map<String, Object> toMap(TableWorker w) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id",              w.getWorkerId());
        m.put("topic",           w.getTopic());
        m.put("table_name",      w.getTableName());
        m.put("status",          w.getStatus());
        m.put("records_written", w.getRecordsWritten());
        m.put("error",           w.getError());
        return m;
    }

    private static void send(HttpExchange ex, int code, String body) throws IOException {
        byte[] bytes = body.getBytes();
        ex.getResponseHeaders().add("Content-Type", "application/json");
        ex.sendResponseHeaders(code, bytes.length == 0 ? -1 : bytes.length);
        if (bytes.length > 0) {
            try (OutputStream os = ex.getResponseBody()) {
                os.write(bytes);
            }
        }
    }
}
