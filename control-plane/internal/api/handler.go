package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/streamlake/control-plane/internal/store"
)

// Handler holds the dependencies for all HTTP route handlers.
type Handler struct {
	db           *store.DB
	dataPlaneURL string
	client       *http.Client
}

func NewHandler(db *store.DB, dataPlaneURL string) *Handler {
	return &Handler{
		db:           db,
		dataPlaneURL: dataPlaneURL,
		client:       &http.Client{Timeout: 10 * time.Second},
	}
}

// ── request / response types ──────────────────────────────────────────────────

type CreateTableRequest struct {
	Topic     string `json:"topic"`
	TableName string `json:"table_name,omitempty"`
}

// workerStartRequest is sent to the Java data plane.
type workerStartRequest struct {
	WorkerID  string `json:"worker_id"`
	Topic     string `json:"topic"`
	TableName string `json:"table_name"`
}

// ── POST /api/v1/tables ───────────────────────────────────────────────────────

func (h *Handler) CreateTable(w http.ResponseWriter, r *http.Request) {
	var req CreateTableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "invalid JSON body")
		return
	}
	if req.Topic == "" {
		writeErr(w, 400, "topic is required")
		return
	}

	// Idempotency: if the topic is already registered return the existing row.
	// UNIQUE(topic) in the DDL enforces this at the DB layer too; we surface it
	// as a 409 here so callers can distinguish "already exists" from 500.
	if existing, err := h.db.GetByTopic(req.Topic); err == nil {
		writeJSON(w, 409, map[string]any{
			"error": "topic already registered",
			"table": existing,
		})
		return
	}

	id := randomID(4)
	tableName := req.TableName
	if tableName == "" {
		rep := strings.NewReplacer(".", "_", "-", "_")
		tableName = rep.Replace(req.Topic)
	}

	t := store.Table{
		ID: id, Topic: req.Topic, TableName: tableName, Status: "starting",
	}
	if err := h.db.Upsert(t); err != nil {
		writeErr(w, 500, fmt.Sprintf("db error: %v", err))
		return
	}

	payload, _ := json.Marshal(workerStartRequest{
		WorkerID: id, Topic: req.Topic, TableName: tableName,
	})

	// Retry the data-plane POST with exponential backoff (100ms, 200ms, 400ms).
	// Only retry on network errors and 5xx — a 4xx is a caller mistake, not transient.
	var dpErr error
	var dpResp *http.Response
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(100*(1<<attempt)) * time.Millisecond)
		}
		dpResp, dpErr = h.client.Post(
			h.dataPlaneURL+"/workers", "application/json", bytes.NewReader(payload))
		if dpErr == nil && dpResp.StatusCode < 500 {
			break
		}
	}

	if dpErr != nil || dpResp.StatusCode >= 400 {
		msg := "data plane unreachable — start Java server with: make server"
		if dpErr == nil {
			msg = fmt.Sprintf("data plane returned %d", dpResp.StatusCode)
		}
		errStr := msg
		_ = h.db.UpdateStatus(id, "error", 0, &errStr)
		writeErr(w, 502, msg)
		return
	}
	defer dpResp.Body.Close()

	t.Status = "running"
	_ = h.db.Upsert(t)
	writeJSON(w, 201, t)
}

// ── GET /api/v1/tables ────────────────────────────────────────────────────────

func (h *Handler) ListTables(w http.ResponseWriter, r *http.Request) {
	tables, err := h.db.List()
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if tables == nil {
		tables = []store.Table{}
	}

	// Fan out enrichment in parallel. Without this, N tables × 10 s timeout =
	// O(N) response time on a slow data plane. Semaphore caps goroutine fan-out.
	sem := make(chan struct{}, 20)
	var wg sync.WaitGroup
	for i := range tables {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			h.enrich(r.Context(), &tables[i])
		}(i)
	}
	wg.Wait()

	writeJSON(w, 200, tables)
}

// ── GET /api/v1/tables/{id} ───────────────────────────────────────────────────

func (h *Handler) GetTable(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	t, err := h.db.Get(id)
	if errors.Is(err, sql.ErrNoRows) {
		writeErr(w, 404, "table not found")
		return
	}
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	h.enrich(r.Context(), t)
	writeJSON(w, 200, t)
}

// ── DELETE /api/v1/tables/{id} ────────────────────────────────────────────────

func (h *Handler) StopTable(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if _, err := h.db.Get(id); errors.Is(err, sql.ErrNoRows) {
		writeErr(w, 404, "table not found")
		return
	}

	// Mark as 'stopping' before calling the data plane so the state is always
	// consistent: if we crash mid-stop, reconciliation sees 'stopping' and retries.
	_ = h.db.UpdateStatus(id, "stopping", 0, nil)

	req, _ := http.NewRequestWithContext(r.Context(),
		"DELETE", h.dataPlaneURL+"/workers/"+id, nil)
	resp, err := h.client.Do(req)
	if err != nil {
		errMsg := "data plane unreachable during stop"
		_ = h.db.UpdateStatus(id, "error", 0, &errMsg)
		writeErr(w, 502, errMsg)
		return
	}
	// 404 from the data plane means the worker was already gone — that's fine.
	if resp.StatusCode != 204 && resp.StatusCode != 404 {
		errMsg := fmt.Sprintf("data plane returned %d during stop", resp.StatusCode)
		_ = h.db.UpdateStatus(id, "error", 0, &errMsg)
		writeErr(w, 502, errMsg)
		return
	}

	// Soft-delete: keep the row as 'stopped' for audit trail.
	_ = h.db.UpdateStatus(id, "stopped", 0, nil)
	w.WriteHeader(204)
}

// ── GET /health ───────────────────────────────────────────────────────────────

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]string{"status": "ok"})
}

// ── helpers ───────────────────────────────────────────────────────────────────

// enrich fetches live worker state from Java, persists it back to SQLite,
// and overlays it on the local DB row. Per-call timeout prevents one slow
// data-plane call from holding up the entire ListTables response.
func (h *Handler) enrich(ctx context.Context, t *store.Table) {
	ctx2, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx2, "GET",
		h.dataPlaneURL+"/workers/"+t.ID, nil)
	resp, err := h.client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	// If the data plane says the worker doesn't exist but SQLite says it's running,
	// persist the discrepancy so the next ListTables call reflects reality.
	if resp.StatusCode == 404 && (t.Status == "running" || t.Status == "starting") {
		errMsg := "worker not found in data plane"
		_ = h.db.UpdateStatus(t.ID, "error", t.RecordsWritten, &errMsg)
		t.Status = "error"
		t.Error = &errMsg
		return
	}
	if resp.StatusCode != 200 {
		return
	}

	var m map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return
	}
	newStatus := t.Status
	if status, ok := m["status"].(string); ok {
		newStatus = status
		t.Status = status
	}
	if rw, ok := m["records_written"].(float64); ok {
		t.RecordsWritten = int64(rw)
	}
	if errMsg, ok := m["error"].(string); ok && errMsg != "" {
		t.Error = &errMsg
	}
	// Persist the freshest state back so SQLite never goes stale indefinitely.
	_ = h.db.UpdateStatus(t.ID, newStatus, t.RecordsWritten, t.Error)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func randomID(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}
