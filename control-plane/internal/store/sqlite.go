package store

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

// DB wraps a SQLite connection and owns the table configuration state.
type DB struct {
	conn *sql.DB
}

// Table represents one Kafka topic → Iceberg table mapping.
type Table struct {
	ID             string  `json:"id"`
	Topic          string  `json:"topic"`
	TableName      string  `json:"table_name"`
	Status         string  `json:"status"`
	RecordsWritten int64   `json:"records_written"`
	Error          *string `json:"error,omitempty"`
	CreatedAt      string  `json:"created_at"`
}

func New(path string) (*DB, error) {
	// WAL mode: allows one writer + many readers concurrently; busy_timeout retries
	// before returning SQLITE_BUSY so concurrent HTTP goroutines don't fail immediately.
	dsn := path + "?_journal_mode=WAL&_busy_timeout=5000&_foreign_keys=on"
	conn, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// Single connection serialises all writes; safe with WAL for reads too.
	conn.SetMaxOpenConns(1)
	db := &DB{conn: conn}
	if err := db.migrate(); err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return db, nil
}

func (db *DB) migrate() error {
	// UNIQUE(topic) prevents two workers for the same topic (idempotency guard).
	// CHECK on status rejects invalid transitions at the DB layer.
	// NOT NULL on created_at prevents null-timestamp ORDER BY surprises.
	_, err := db.conn.Exec(`
		CREATE TABLE IF NOT EXISTS tables (
			id              TEXT PRIMARY KEY,
			topic           TEXT    NOT NULL UNIQUE,
			table_name      TEXT    NOT NULL,
			status          TEXT    NOT NULL DEFAULT 'stopped'
				CHECK (status IN ('starting','running','stopping','stopped','error')),
			records_written INTEGER NOT NULL DEFAULT 0,
			error           TEXT,
			created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		return err
	}
	// Covering index satisfies ORDER BY created_at DESC in List() without a sort step.
	if _, err = db.conn.Exec(
		`CREATE INDEX IF NOT EXISTS idx_tables_created_at ON tables(created_at DESC)`); err != nil {
		return err
	}
	_, err = db.conn.Exec(
		`CREATE INDEX IF NOT EXISTS idx_tables_status ON tables(status)`)
	return err
}

// Upsert inserts a new row or updates only the mutable fields on conflict,
// preserving the original created_at timestamp.
func (db *DB) Upsert(t Table) error {
	_, err := db.conn.Exec(`
		INSERT INTO tables (id, topic, table_name, status, records_written, error)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			status          = excluded.status,
			records_written = excluded.records_written,
			error           = excluded.error`,
		t.ID, t.Topic, t.TableName, t.Status, t.RecordsWritten, t.Error,
	)
	return err
}

// GetByTopic looks up a row by its Kafka topic — used for idempotency checks.
func (db *DB) GetByTopic(topic string) (*Table, error) {
	row := db.conn.QueryRow(
		`SELECT id, topic, table_name, status, records_written, error, created_at
		 FROM tables WHERE topic = ?`, topic)
	var t Table
	if err := row.Scan(&t.ID, &t.Topic, &t.TableName, &t.Status,
		&t.RecordsWritten, &t.Error, &t.CreatedAt); err != nil {
		return nil, err
	}
	return &t, nil
}

func (db *DB) Get(id string) (*Table, error) {
	row := db.conn.QueryRow(
		`SELECT id, topic, table_name, status, records_written, error, created_at
		 FROM tables WHERE id = ?`, id)
	var t Table
	if err := row.Scan(&t.ID, &t.Topic, &t.TableName, &t.Status,
		&t.RecordsWritten, &t.Error, &t.CreatedAt); err != nil {
		return nil, err
	}
	return &t, nil
}

func (db *DB) List() ([]Table, error) {
	rows, err := db.conn.Query(
		`SELECT id, topic, table_name, status, records_written, error, created_at
		 FROM tables ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []Table
	for rows.Next() {
		var t Table
		if err := rows.Scan(&t.ID, &t.Topic, &t.TableName, &t.Status,
			&t.RecordsWritten, &t.Error, &t.CreatedAt); err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}
	return tables, rows.Err()
}

func (db *DB) UpdateStatus(id, status string, records int64, errMsg *string) error {
	_, err := db.conn.Exec(
		`UPDATE tables SET status=?, records_written=?, error=? WHERE id=?`,
		status, records, errMsg, id,
	)
	return err
}

func (db *DB) Delete(id string) error {
	_, err := db.conn.Exec(`DELETE FROM tables WHERE id = ?`, id)
	return err
}

func (db *DB) Close() error {
	return db.conn.Close()
}
