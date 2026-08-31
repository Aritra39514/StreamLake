import sqlite3
from contextlib import contextmanager
from typing import Dict, List, Optional

from .config import settings


@contextmanager
def get_db():
    conn = sqlite3.connect(settings.state_db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tables (
                id            TEXT PRIMARY KEY,
                topic         TEXT NOT NULL,
                table_name    TEXT NOT NULL,
                status        TEXT NOT NULL DEFAULT 'stopped',
                records_written INTEGER NOT NULL DEFAULT 0,
                error         TEXT,
                created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS committed_offsets (
                table_id  TEXT    NOT NULL,
                topic     TEXT    NOT NULL,
                partition INTEGER NOT NULL,
                kafka_offset INTEGER NOT NULL,
                PRIMARY KEY (table_id, topic, partition)
            )
        """)


# ── Tables ─────────────────────────────────────────────────────────────────────

def upsert_table(id: str, topic: str, table_name: str):
    with get_db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO tables (id, topic, table_name, status, records_written)"
            " VALUES (?, ?, ?, 'starting', 0)",
            (id, topic, table_name),
        )


def update_table_status(
    id: str, status: str, records_written: int = 0, error: Optional[str] = None
):
    with get_db() as conn:
        conn.execute(
            "UPDATE tables SET status=?, records_written=?, error=?,"
            " updated_at=CURRENT_TIMESTAMP WHERE id=?",
            (status, records_written, error, id),
        )


def get_table(id: str) -> Optional[dict]:
    with get_db() as conn:
        row = conn.execute("SELECT * FROM tables WHERE id=?", (id,)).fetchone()
        return dict(row) if row else None


def list_tables() -> List[dict]:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM tables ORDER BY created_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]


def delete_table(id: str):
    with get_db() as conn:
        conn.execute("DELETE FROM tables WHERE id=?", (id,))
        conn.execute("DELETE FROM committed_offsets WHERE table_id=?", (id,))


# ── Offsets ────────────────────────────────────────────────────────────────────

def get_committed_offsets(table_id: str, topic: str) -> Dict[int, int]:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT partition, kafka_offset FROM committed_offsets"
            " WHERE table_id=? AND topic=?",
            (table_id, topic),
        ).fetchall()
        return {r["partition"]: r["kafka_offset"] for r in rows}


def save_committed_offsets(table_id: str, topic: str, offsets: Dict[int, int]):
    with get_db() as conn:
        for partition, offset in offsets.items():
            conn.execute(
                "INSERT OR REPLACE INTO committed_offsets"
                " (table_id, topic, partition, kafka_offset) VALUES (?,?,?,?)",
                (table_id, topic, partition, offset),
            )
