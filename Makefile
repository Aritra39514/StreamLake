.PHONY: up down install-java install-go build-java build-go producer server streams run-go reset

# ── Infrastructure ─────────────────────────────────────────────────────────────
up:
	docker compose up -d
	@echo ""
	@echo "Waiting 30s for all services to be ready..."
	@sleep 30
	@echo ""
	@echo "  Kafka Broker    ->  localhost:9092"
	@echo "  Schema Registry ->  http://localhost:8081"
	@echo "  Kafka Connect   ->  http://localhost:8083/connectors"
	@echo "  ksqlDB          ->  http://localhost:8088"
	@echo "  REST Proxy      ->  http://localhost:8082"
	@echo "  Kafka UI        ->  http://localhost:8080"
	@echo "  Data Plane      ->  http://localhost:9090  (after: make server)"
	@echo "  Control Plane   ->  http://localhost:8000  (after: make run-go)"
	@echo "  Metrics         ->  http://localhost:8001/metrics"

down:
	docker compose down -v

# ── Java data-plane ────────────────────────────────────────────────────────────
build-java:
	cd data-plane && mvn clean package -q -DskipTests

producer: build-java
	cd data-plane && java -jar target/data-plane-1.0.0.jar producer

server: build-java
	cd data-plane && java -jar target/data-plane-1.0.0.jar server

streams: build-java
	cd data-plane && java -jar target/data-plane-1.0.0.jar streams

# ── Go control-plane ───────────────────────────────────────────────────────────
build-go:
	cd control-plane && go build -o bin/control-plane ./...

run-go: build-go
	cd control-plane && DATA_PLANE_URL=http://localhost:9090 ./bin/control-plane

# ── Reset ──────────────────────────────────────────────────────────────────────
reset:
	docker compose down -v
	rm -rf warehouse/ streamlake.db
	@echo "Reset complete."
