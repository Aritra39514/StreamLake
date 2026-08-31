import logging
from pathlib import Path
from typing import Any, Dict, List
from datetime import datetime, timezone

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError

from .config import settings

logger = logging.getLogger(__name__)

# ── Arrow schema for the orders topic ─────────────────────────────────────────
# kafka_offset / kafka_partition are appended by the worker so we can audit
# exactly which Kafka messages landed in each Iceberg snapshot.

ORDERS_SCHEMA = pa.schema([
    pa.field("order_id",         pa.string()),
    pa.field("customer_id",      pa.string()),
    pa.field("product_id",       pa.string()),
    pa.field("quantity",         pa.int32()),
    pa.field("price",            pa.float64()),
    pa.field("status",           pa.string()),
    pa.field("event_time",       pa.timestamp("us", tz="UTC")),
    pa.field("kafka_offset",     pa.int64()),
    pa.field("kafka_partition",  pa.int32()),
])

NAMESPACE = "streamlake"


def _catalog():
    Path(settings.warehouse_path).mkdir(parents=True, exist_ok=True)
    return load_catalog("streamlake", **{
        "type": "sql",
        "uri": settings.catalog_db_uri,
        "warehouse": settings.warehouse_path,
    })


class IcebergWriter:
    """Wraps a single Iceberg table and provides an append-only write path."""

    def __init__(self, table_name: str):
        self.table_name = table_name
        self.identifier = (NAMESPACE, table_name)
        self.catalog = _catalog()
        self.table = self._get_or_create_table()

    # ── lifecycle ──────────────────────────────────────────────────────────────

    def _get_or_create_table(self):
        try:
            self.catalog.create_namespace(NAMESPACE)
        except Exception:
            pass  # namespace already exists
        try:
            return self.catalog.load_table(self.identifier)
        except NoSuchTableError:
            logger.info("Creating Iceberg table %s", self.identifier)
            return self.catalog.create_table(
                identifier=self.identifier,
                schema=ORDERS_SCHEMA,
            )

    # ── write ──────────────────────────────────────────────────────────────────

    def append(self, records: List[Dict[str, Any]]) -> int:
        """Convert a list of dicts to a PyArrow table and append to Iceberg."""
        if not records:
            return 0

        cols: Dict[str, list] = {f.name: [] for f in ORDERS_SCHEMA}
        for rec in records:
            cols["order_id"].append(str(rec.get("order_id", "")))
            cols["customer_id"].append(str(rec.get("customer_id", "")))
            cols["product_id"].append(str(rec.get("product_id", "")))
            cols["quantity"].append(int(rec.get("quantity", 0)))
            cols["price"].append(float(rec.get("price", 0.0)))
            cols["status"].append(str(rec.get("status", "")))
            raw_ts = rec.get("event_time")
            if isinstance(raw_ts, str):
                from dateutil.parser import parse as _parse
                cols["event_time"].append(_parse(raw_ts).replace(tzinfo=timezone.utc))
            elif isinstance(raw_ts, datetime):
                cols["event_time"].append(raw_ts)
            else:
                cols["event_time"].append(datetime.now(timezone.utc))
            cols["kafka_offset"].append(int(rec.get("kafka_offset", -1)))
            cols["kafka_partition"].append(int(rec.get("kafka_partition", 0)))

        arrow_table = pa.table(cols, schema=ORDERS_SCHEMA)
        self.table.append(arrow_table)
        return len(records)

    # ── query ──────────────────────────────────────────────────────────────────

    def row_count(self) -> int:
        try:
            self.table.refresh()
            snap = self.table.current_snapshot()
            if snap and snap.summary:
                return int(snap.summary.get("total-records", 0))
        except Exception:
            pass
        return 0

    def scan(self, limit: int = 20):
        """Return the latest rows as a pandas DataFrame."""
        return self.table.scan(limit=limit).to_pandas()
