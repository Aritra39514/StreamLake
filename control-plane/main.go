package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/streamlake/control-plane/internal/api"
	"github.com/streamlake/control-plane/internal/store"
)

/*
StreamLake Control Plane (Go)

Why Go for this layer?
──────────────────────
Confluent writes control plane services in Go because:
  • Fast startup — Kubernetes probes pass in milliseconds
  • Low memory baseline — ~10 MB vs ~200 MB for a JVM service
  • Goroutine concurrency model maps cleanly to lifecycle management
  • cobra / chi give you a production CLI + HTTP stack in ~50 lines

This service:
  1. Accepts user-facing REST calls (create/list/stop Iceberg tables)
  2. Persists state to SQLite
  3. Forwards worker start/stop commands to the Java data plane on :9090
  4. Exposes Prometheus metrics + Swagger-style health endpoint

Environment variables:
  DATA_PLANE_URL   default: http://localhost:9090
  PORT             default: 8000
*/

func main() {
	db, err := store.New(getenv("DB_PATH", "streamlake.db"))
	if err != nil {
		log.Fatalf("cannot open database: %v", err)
	}
	defer db.Close()

	dataPlaneURL := getenv("DATA_PLANE_URL", "http://localhost:9090")
	client := &http.Client{Timeout: 10 * time.Second}

	// On startup, reconcile SQLite state with the live Java data plane.
	// This re-registers workers that were running before a Go control-plane
	// restart, preventing split-brain where SQLite shows 'running' but Java
	// has no active worker.
	reconcileWorkers(db, client, dataPlaneURL)

	h := api.NewHandler(db, dataPlaneURL)

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	// Observability
	r.Get("/health",  h.Health)
	r.Get("/metrics", promhttp.Handler().ServeHTTP)

	// User-facing API
	r.Route("/api/v1", func(r chi.Router) {
		r.Post("/tables",        h.CreateTable)
		r.Get("/tables",         h.ListTables)
		r.Get("/tables/{id}",    h.GetTable)
		r.Delete("/tables/{id}", h.StopTable)
	})

	port := getenv("PORT", "8000")
	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	// Background reconciler keeps SQLite fresh between user requests.
	// Without it a crashed Java worker would show 'running' in the API
	// until the next ListTables or GetTable call.
	go backgroundReconcile(db, client, dataPlaneURL)

	go func() {
		log.Printf("Control plane listening on :%s  (data-plane → %s)", port, dataPlaneURL)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen error: %v", err)
		}
	}()

	// Graceful shutdown on SIGINT / SIGTERM
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutdown signal received…")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("Shutdown error: %v", err)
	}
	log.Println("Control plane stopped")
}

// reconcileWorkers aligns SQLite state with the live Java WorkerRegistry:
//  1. Re-registers any SQLite 'starting'/'running' rows not present in Java
//     (e.g. after a Go restart while Java kept running, or vice versa).
//  2. Stops any Java workers whose IDs are absent from SQLite
//     (orphans from a previous Go crash that created a worker but lost the row).
func reconcileWorkers(db *store.DB, client *http.Client, dataPlaneURL string) {
	rows, err := db.List()
	if err != nil {
		log.Printf("reconcile: db.List error: %v", err)
		return
	}

	// Snapshot of workers currently alive in Java
	liveWorkers := map[string]bool{}
	if resp, err := client.Get(dataPlaneURL + "/workers"); err == nil && resp.StatusCode == 200 {
		var workers []map[string]any
		if json.NewDecoder(resp.Body).Decode(&workers) == nil {
			for _, w := range workers {
				if id, ok := w["id"].(string); ok {
					liveWorkers[id] = true
				}
			}
		}
		resp.Body.Close()
	}

	// Re-register SQLite rows that Java has lost
	for _, row := range rows {
		if row.Status != "starting" && row.Status != "running" {
			continue
		}
		if liveWorkers[row.ID] {
			continue // already alive in Java
		}
		payload, _ := json.Marshal(map[string]string{
			"worker_id":  row.ID,
			"topic":      row.Topic,
			"table_name": row.TableName,
		})
		r, err := client.Post(dataPlaneURL+"/workers", "application/json",
			bytes.NewReader(payload))
		if err != nil || (r.StatusCode >= 400 && r.StatusCode != 409) {
			msg := "failed to re-register on startup"
			_ = db.UpdateStatus(row.ID, "error", row.RecordsWritten, &msg)
			log.Printf("reconcile: could not re-register worker %s: %v", row.ID, err)
		} else {
			if r != nil {
				r.Body.Close()
			}
			log.Printf("reconcile: re-registered worker %s (%s → %s)", row.ID, row.Topic, row.TableName)
		}
	}

	// Stop Java workers that have no corresponding SQLite row (orphans)
	for id := range liveWorkers {
		if _, err := db.Get(id); err != nil {
			req, _ := http.NewRequest("DELETE", dataPlaneURL+"/workers/"+id, nil)
			client.Do(req) //nolint:errcheck
			log.Printf("reconcile: stopped orphan worker %s", id)
		}
	}
}

// backgroundReconcile runs reconcileWorkers every 30 s so the SQLite store
// never stays stale for more than one interval after a Java-side failure.
func backgroundReconcile(db *store.DB, client *http.Client, dataPlaneURL string) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		reconcileWorkers(db, client, dataPlaneURL)
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
