#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# start_airflow.sh — Start Airflow for Cricket Analytics Pipeline
#
# Usage:
#   bash scripts/start_airflow.sh              # Start Airflow standalone
#   bash scripts/start_airflow.sh --trigger     # Start + trigger the DAG
#   bash scripts/start_airflow.sh --stop        # Stop Airflow
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
AIRFLOW_HOME="${AIRFLOW_HOME:-$HOME/airflow}"
DAG_ID="cricket_analytics_batch_pipeline"
DAG_FILE="$PROJECT_DIR/airflow/dags/cricket_batch_pipeline.py"
SYMLINK_TARGET="$AIRFLOW_HOME/dags/cricket_batch_pipeline.py"

# ── Handle --stop ─────────────────────────────────────────────────────────────
if [[ "${1:-}" == "--stop" ]]; then
    echo "🛑 Stopping Airflow..."
    pkill -f "airflow" 2>/dev/null || true
    echo "✅ Airflow stopped"
    exit 0
fi

echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║  🏏 Cricket Analytics — Airflow Setup                           ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""

# ── Step 1: Symlink DAG ──────────────────────────────────────────────────────
echo "━━━ Step 1: Linking DAG to Airflow ━━━"
mkdir -p "$AIRFLOW_HOME/dags"
if [ ! -L "$SYMLINK_TARGET" ] || [ "$(readlink "$SYMLINK_TARGET")" != "$DAG_FILE" ]; then
    ln -sf "$DAG_FILE" "$SYMLINK_TARGET"
    echo "  ✅ Symlinked: $SYMLINK_TARGET → $DAG_FILE"
else
    echo "  ✅ Symlink already exists"
fi

# ── Step 2: Check Airflow ────────────────────────────────────────────────────
echo ""
echo "━━━ Step 2: Checking Airflow installation ━━━"
if ! command -v airflow &> /dev/null; then
    echo "  ⚠️  Airflow not found in PATH"
    echo ""
    echo "  Install Airflow with:"
    echo "    pip install 'apache-airflow==2.10.4' \\"
    echo "      --constraint 'https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.12.txt'"
    echo ""
    echo "  Then initialize:"
    echo "    airflow db init"
    echo "    airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com"
    echo ""
    exit 1
fi
echo "  ✅ Airflow found: $(airflow version 2>/dev/null || echo 'installed')"

# ── Step 3: Validate DAG ─────────────────────────────────────────────────────
echo ""
echo "━━━ Step 3: Validating DAG ━━━"
python3 -m py_compile "$DAG_FILE"
echo "  ✅ DAG syntax valid"

# ── Step 4: Start Airflow ────────────────────────────────────────────────────
echo ""
echo "━━━ Step 4: Starting Airflow ━━━"
echo ""
echo "  Starting Airflow standalone (webserver + scheduler)..."
echo "  Dashboard: http://localhost:8080"
echo "  Username:  admin"
echo "  Password:  (see ~/airflow/simple_auth_manager_passwords.json.generated)"
echo ""
echo "  Press Ctrl+C to stop Airflow"
echo ""

# Start Airflow
export AIRFLOW_HOME
airflow standalone &
AIRFLOW_PID=$!

# Wait for Airflow to come up
echo "  ⏳ Waiting for Airflow to start..."
sleep 10

# ── Step 5: Optional trigger ─────────────────────────────────────────────────
if [[ "${1:-}" == "--trigger" ]]; then
    echo ""
    echo "━━━ Step 5: Triggering DAG ━━━"
    sleep 5  # Give a bit more time
    airflow dags unpause "$DAG_ID" 2>/dev/null || true
    airflow dags trigger "$DAG_ID" 2>/dev/null && \
        echo "  ✅ DAG triggered: $DAG_ID" || \
        echo "  ⚠️  Could not trigger DAG yet — try manually from http://localhost:8080"
fi

# Keep running
wait $AIRFLOW_PID

