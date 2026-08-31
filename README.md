# StreamLake — Kafka → Apache Iceberg Materialization Platform

A production-grade reference implementation of **Confluent Tableflow** architecture —
continuously materializes Apache Kafka topics into Apache Iceberg tables so query engines
(Spark, Trino, DuckDB, Flink) can read streaming data using standard SQL.

![Java](https://img.shields.io/badge/Java-21-orange?logo=openjdk)
![Go](https://img.shields.io/badge/Go-1.22-blue?logo=go)
![Kafka](https://img.shields.io/badge/Apache_Kafka-3.7-231F20?logo=apachekafka)
![Iceberg](https://img.shields.io/badge/Apache_Iceberg-1.5.2-3D6FBE)
![Confluent](https://img.shields.io/badge/Confluent_Platform-7.6-blue)
![Docker](https://img.shields.io/badge/Docker_Compose-ready-2496ED?logo=docker)

---

## What is StreamLake?

Kafka is a **write-optimised log** — great for real-time ingestion, not queryable by SQL engines.
Iceberg is a **read-optimised open table format** with ACID transactions, schema evolution, and
time-travel — but has no built-in streaming ingest. StreamLake is the bridge.

```
OrderProducer ──► Kafka (KRaft) ──► TableWorker ──► Iceberg (Parquet)
                       │                                    │
                  Schema Registry                   warehouse/streamlake/
                  Kafka Connect                     metadata.json (atomic)
                  ksqlDB                            data/*.parquet
                  REST Proxy
```

---

## Architecture

| Plane | Language | Responsibility |
|---|---|---|
| **Control Plane** | Go 1.22 + chi | REST API, SQLite state, worker lifecycle, split-brain reconciliation |
| **Data Plane** | Java 21 + Kafka 3.7 + Iceberg 1.5 | Kafka consumers, Iceberg writes, Kafka Streams pipelines |

### Confluent Ecosystem (Docker Compose)

| Service | Port | Role |
|---|---|---|
| Kafka Broker (KRaft) | 9092 | Event backbone — no ZooKeeper |
| Schema Registry | 8081 | Avro/JSON/Protobuf schema management |
| Kafka Connect | 8083 | Source & sink connectors |
| ksqlDB | 8088 | SQL streaming over Kafka |
| REST Proxy | 8082 | HTTP interface to Kafka |
| Kafka UI | 8080 | Visual topic/schema browser |

---

## Quick Start (Local)

### Prerequisites
- Docker Desktop
- Java 17+ and Maven 3.8+
- Go 1.22+

### 1. Start the Confluent Stack

```bash
docker compose up -d
# Wait ~30s for all services to become healthy
docker compose ps   # broker, schema-registry, connect, ksqldb-server, rest-proxy → healthy
```

### 2. Build the Java Data Plane

```bash
cd data-plane
mvn clean package -q -DskipTests
# Produces: target/data-plane-1.0.0.jar (~161 MB fat jar)
```

### 3. Start the Data Plane (terminal 1)

```bash
cd data-plane
java -jar target/data-plane-1.0.0.jar server
# Workers API  →  http://localhost:9090
# Metrics      →  http://localhost:8001/metrics
```

### 4. Start the Order Producer (terminal 2)

```bash
cd data-plane
java -jar target/data-plane-1.0.0.jar producer
# Produces 5 orders/second to the 'orders' topic
```

### 5. Build and Start the Go Control Plane (terminal 3)

```bash
cd control-plane
go build -o bin/control-plane ./...
DATA_PLANE_URL=http://localhost:9090 ./bin/control-plane
# Control plane API  →  http://localhost:8000
```

### 6. Register an Iceberg Pipeline

```bash
curl -s -X POST http://localhost:8000/api/v1/tables \
  -H 'Content-Type: application/json' \
  -d '{"topic":"orders","table_name":"orders"}' | python -m json.tool
```

### 7. Watch Data Flow

```bash
# Check pipeline status
curl -s http://localhost:8000/api/v1/tables | python -m json.tool

# Browse topics in Kafka UI
open http://localhost:8080

# Iceberg Parquet files
ls warehouse/streamlake/orders/data/
```

### 8. Start Kafka Streams Analytics (optional, terminal 4)

```bash
cd data-plane
java -jar target/data-plane-1.0.0.jar streams
# Pipeline A: orders → filter PLACED → orders-placed
# Pipeline B: orders → 1-min window → SUM(price) → product-revenue-metrics
```

---

## API Reference

### Control Plane `:8000`

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/tables` | Register a new Kafka→Iceberg pipeline |
| `GET` | `/api/v1/tables` | List all pipelines with live status |
| `GET` | `/api/v1/tables/{id}` | Get one pipeline |
| `DELETE` | `/api/v1/tables/{id}` | Stop a pipeline (graceful drain) |
| `GET` | `/health` | Liveness probe |
| `GET` | `/metrics` | Prometheus metrics |

### Data Plane `:9090`

| Method | Path | Description |
|---|---|---|
| `POST` | `/workers` | Start a new TableWorker |
| `GET` | `/workers` | List all workers + live status |
| `DELETE` | `/workers/{id}` | Stop a worker |
| `GET` | `/health` | `{"status":"ok","workers":N}` |

---

## Distributed Systems Guarantees

Eight critical properties explicitly implemented:

| Property | Implementation |
|---|---|
| **At-least-once delivery** | Iceberg write first, Kafka offset commit second |
| **Rebalance flush** | `ConsumerRebalanceListener` flushes buffer before partition revocation |
| **TOCTOU-free registry** | `ConcurrentHashMap.putIfAbsent()` — single atomic CAS |
| **Split-brain recovery** | Startup + 30s background reconciliation between Go and Java |
| **Supervisor/watchdog** | `ScheduledExecutorService` restarts error workers up to 5× |
| **Idempotent create** | `UNIQUE(topic)` DB constraint + `GetByTopic` check + 409 response |
| **Concurrent HTTP** | `newFixedThreadPool(max(4, cores×2))` — no serial inline executor |
| **SQLite WAL** | WAL mode + `busy_timeout=5s` — concurrent readers never blocked |

---

## Environment Variables

| Variable | Default | Service | Description |
|---|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Java | Kafka broker address |
| `WAREHOUSE_PATH` | `warehouse` | Java | Iceberg HadoopCatalog root |
| `DATA_PLANE_PORT` | `9090` | Java | Data plane HTTP port |
| `METRICS_PORT` | `8001` | Java | Prometheus metrics port |
| `PRODUCER_RATE` | `5.0` | Java | Events per second |
| `DATA_PLANE_URL` | `http://localhost:9090` | Go | Java data plane URL |
| `PORT` | `8000` | Go | Control plane HTTP port |
| `DB_PATH` | `streamlake.db` | Go | SQLite database path |

---

## AWS Production Deployment

Swap local components for AWS managed services — application code changes are minimal:

| Local | AWS Equivalent |
|---|---|
| Kafka (Docker) | Amazon MSK (Managed Kafka) |
| HadoopCatalog (local FS) | AWS Glue Data Catalog + S3 |
| SQLite | Amazon Aurora Serverless v2 (PostgreSQL) |
| Docker Compose | Amazon ECS Fargate |
| Prometheus | Amazon Managed Prometheus (AMP) + Grafana |

**Only one code change for AWS** — swap the catalog in `IcebergTableWriter.java`:

```java
// Local
catalog = new HadoopCatalog(conf, "warehouse/");

// AWS — everything else (append, schema, commit) is identical
catalog = new GlueCatalog();
catalog.initialize("glue", Map.of(
    CatalogProperties.WAREHOUSE_LOCATION, "s3://my-bucket/warehouse/",
    CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO"
));
```

See [STREAMLAKE_SYSTEM_DESIGN.html](STREAMLAKE_SYSTEM_DESIGN.html) for the full AWS architecture,
Terraform concepts, Dockerfiles, and cost estimate (~$560/mo for a production setup).

---

## Project Structure

```
StreamLake/
├── data-plane/                         # Java — Kafka consumer + Iceberg writer
│   ├── pom.xml                         # Maven: kafka-clients, iceberg, parquet, hadoop
│   └── src/main/java/com/streamlake/
│       ├── Main.java                   # Entry point (server|producer|streams)
│       ├── server/DataPlaneServer.java # HTTP API :9090
│       ├── worker/
│       │   ├── WorkerRegistry.java     # Thread-safe registry + supervisor watchdog
│       │   └── TableWorker.java        # Kafka poll loop + Iceberg flush
│       ├── iceberg/IcebergTableWriter.java  # Parquet write + atomic snapshot commit
│       ├── producer/OrderProducer.java # Idempotent fake order generator
│       ├── streams/OrderMetricsStream.java  # Kafka Streams: filter + windowed agg
│       └── model/Order.java
├── control-plane/                      # Go — lifecycle API + state management
│   ├── main.go                         # chi router + startup reconciliation
│   └── internal/
│       ├── api/handler.go              # REST handlers (idempotent, retry, fan-out)
│       └── store/sqlite.go             # SQLite WAL store
├── docker-compose.yml                  # Full Confluent Platform 7.6 stack
├── Makefile                            # up / down / build-java / server / producer
├── STREAMLAKE_SYSTEM_DESIGN.html       # Full system design doc (print to PDF)
└── .env.example                        # Environment variable reference
```

---

## Reset

```bash
docker compose down -v
rm -rf warehouse/ streamlake.db
```

---

## Documentation

Open [STREAMLAKE_SYSTEM_DESIGN.html](STREAMLAKE_SYSTEM_DESIGN.html) in a browser and
print → Save as PDF for the full system design document covering architecture diagrams,
tech stack rationale, distributed systems deep-dives, API reference, and AWS deployment guide.
