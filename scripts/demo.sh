#!/usr/bin/env bash
# scripts/demo.sh
# Full end-to-end demo — run from the repo root: bash scripts/demo.sh
set -euo pipefail

BASE="http://localhost:8000"

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║          StreamLake  —  End-to-End Demo              ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

# ── 1. Create a table (starts the worker) ─────────────────────────────────────
echo "▶  Step 1 — Creating table 'orders' from topic 'orders'…"
RESPONSE=$(curl -s -X POST "$BASE/api/v1/tables" \
  -H "Content-Type: application/json" \
  -d '{"topic": "orders", "table_name": "orders"}')
echo "   $RESPONSE"
TABLE_ID=$(echo "$RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
echo ""

# ── 2. Watch it stream ────────────────────────────────────────────────────────
echo "▶  Step 2 — Polling status every 5s for 30s…"
for i in $(seq 1 6); do
  sleep 5
  STATUS=$(curl -s "$BASE/api/v1/tables/$TABLE_ID")
  RECORDS=$(echo "$STATUS" | python3 -c "import sys,json; print(json.load(sys.stdin)['records_written'])")
  echo "   [${i}0s] records_written = $RECORDS"
done
echo ""

# ── 3. List all tables ────────────────────────────────────────────────────────
echo "▶  Step 3 — All tables:"
curl -s "$BASE/api/v1/tables" | python3 -m json.tool
echo ""

# ── 4. Query Iceberg directly ─────────────────────────────────────────────────
echo "▶  Step 4 — Scanning Iceberg table (latest 10 rows)…"
python3 run.py query orders --limit 10
echo ""

# ── 5. Stop worker ────────────────────────────────────────────────────────────
echo "▶  Step 5 — Stopping worker $TABLE_ID…"
curl -s -X DELETE "$BASE/api/v1/tables/$TABLE_ID" -o /dev/null -w "HTTP %{http_code}\n"
echo ""

echo "✓  Demo complete. Check http://localhost:8080 (Kafka UI) for topic lag."
echo "✓  Check http://localhost:8001/metrics for Prometheus metrics."
