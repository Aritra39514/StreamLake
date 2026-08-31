"""
Control plane — REST API that manages the lifecycle of TableWorkers.

Endpoints
─────────
POST   /api/v1/tables          create a table (starts a worker)
GET    /api/v1/tables          list all tables + live status
GET    /api/v1/tables/{id}     get one table
DELETE /api/v1/tables/{id}     stop worker + remove table record
GET    /health                 liveness probe
GET    /docs                   Swagger UI (built-in)
"""
import threading
import uuid
import logging
from contextlib import asynccontextmanager
from typing import Dict, Optional

from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel

from .config import settings
from .database import (
    delete_table,
    get_table,
    init_db,
    list_tables,
    update_table_status,
    upsert_table,
)
from .metrics import ACTIVE_WORKERS, start_metrics_server
from .worker import TableWorker

logger = logging.getLogger(__name__)

_workers: Dict[str, TableWorker] = {}
_lock = threading.Lock()


# ── lifespan ───────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    start_metrics_server()
    logger.info("StreamLake API ready")
    yield
    with _lock:
        for w in list(_workers.values()):
            w.stop()
        for w in list(_workers.values()):
            w.join(timeout=10)
    logger.info("All workers stopped")


# ── app ────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="StreamLake — Control Plane",
    description="Manages Kafka → Apache Iceberg materialisation workers",
    version="1.0.0",
    lifespan=lifespan,
)


# ── schemas ────────────────────────────────────────────────────────────────────

class CreateTableRequest(BaseModel):
    topic: str
    table_name: Optional[str] = None


class TableResponse(BaseModel):
    id: str
    topic: str
    table_name: str
    status: str
    records_written: int
    error: Optional[str] = None


# ── helpers ────────────────────────────────────────────────────────────────────

def _enrich(row: dict) -> dict:
    """Overlay live worker stats onto the DB row."""
    with _lock:
        w = _workers.get(row["id"])
        if w and w.is_alive():
            row["records_written"] = w.records_written
    return row


# ── routes ─────────────────────────────────────────────────────────────────────

@app.post("/api/v1/tables", response_model=TableResponse, status_code=201)
def create_table(req: CreateTableRequest):
    table_id = str(uuid.uuid4())[:8]
    table_name = (
        req.table_name
        or req.topic.replace(".", "_").replace("-", "_")
    )

    upsert_table(table_id, req.topic, table_name)

    worker = TableWorker(table_id, req.topic, table_name)
    with _lock:
        _workers[table_id] = worker
    worker.start()

    logger.info("Created worker %s  topic=%s  table=%s", table_id, req.topic, table_name)
    return TableResponse(
        id=table_id, topic=req.topic, table_name=table_name,
        status="starting", records_written=0,
    )


@app.get("/api/v1/tables", response_model=list[TableResponse])
def list_all_tables():
    return [TableResponse(**_enrich(r)) for r in list_tables()]


@app.get("/api/v1/tables/{table_id}", response_model=TableResponse)
def get_single_table(table_id: str):
    row = get_table(table_id)
    if not row:
        raise HTTPException(status_code=404, detail="Table not found")
    return TableResponse(**_enrich(row))


@app.delete("/api/v1/tables/{table_id}", status_code=204)
def stop_table(table_id: str):
    row = get_table(table_id)
    if not row:
        raise HTTPException(status_code=404, detail="Table not found")

    with _lock:
        worker = _workers.pop(table_id, None)

    if worker:
        worker.stop()
        worker.join(timeout=15)

    update_table_status(table_id, "stopped", row.get("records_written", 0))
    delete_table(table_id)
    return Response(status_code=204)


@app.get("/health")
def health():
    with _lock:
        alive = sum(1 for w in _workers.values() if w.is_alive())
    return {"status": "ok", "active_workers": alive}
