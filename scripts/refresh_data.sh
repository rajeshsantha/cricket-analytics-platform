#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# refresh_data.sh — Incremental data refresh for T20 World Cup 2026
#
# Downloads latest Cricsheet data, detects new matches, runs the Spark
# pipeline for new matches only, re-computes Gold KPIs, exports Parquet,
# and optionally commits + pushes to Git for Streamlit Cloud auto-deploy.
#
# Usage:
#   bash scripts/refresh_data.sh                  # refresh only
#   bash scripts/refresh_data.sh --deploy         # refresh + git push
#   bash scripts/refresh_data.sh --dry-run        # show what would be added
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
STREAMLIT_DIR="$PROJECT_DIR/visualization/streamlit"

# Configurable paths
CRICSHEET_DOWNLOAD_URL="https://cricsheet.org/downloads/t20s_male_json.zip"
RAW_DATA_DIR="${RAW_DATA_DIR:-$HOME/Datasets/t20_wc_2026_json}"
STAGING_DIR="/tmp/cricsheet-refresh-staging"
DELTA_BASE="${DELTA_BASE:-/tmp/cricket-delta}"
JAR="$PROJECT_DIR/target/cricket-analytics-platform-1.0.0-SNAPSHOT-jar-with-dependencies.jar"

# Flags
DEPLOY=false
DRY_RUN=false
for arg in "$@"; do
  case "$arg" in
    --deploy)  DEPLOY=true ;;
    --dry-run) DRY_RUN=true ;;
  esac
done

echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║  🏏 T20 World Cup 2026 — Data Refresh Pipeline                  ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""
echo "  📅 $(date '+%Y-%m-%d %H:%M:%S')"
echo "  📂 Raw data:   $RAW_DATA_DIR"
echo "  🗄️  Delta base: $DELTA_BASE"
echo ""

# ── Step 1: Download latest Cricsheet data ──────────────────────────────────
echo "━━━ Step 1/7: Downloading latest data from Cricsheet ━━━"
rm -rf "$STAGING_DIR"
mkdir -p "$STAGING_DIR"

curl -sL "$CRICSHEET_DOWNLOAD_URL" -o "$STAGING_DIR/t20wc.zip"
unzip -q -o "$STAGING_DIR/t20wc.zip" -d "$STAGING_DIR/all"
echo "  Downloaded $(ls -1 "$STAGING_DIR/all/"*.json 2>/dev/null | wc -l | tr -d ' ') total match files"

# ── Step 2: Filter to 2026 matches only ─────────────────────────────────────
echo ""
echo "━━━ Step 2/7: Filtering 2026 T20 World Cup matches ━━━"
mkdir -p "$STAGING_DIR/2026"
for f in "$STAGING_DIR/all/"*.json; do
  if grep -q '"dates".*"2026-' "$f" 2>/dev/null && grep -q '"T20 World Cup"' "$f" 2>/dev/null; then
    cp "$f" "$STAGING_DIR/2026/"
  fi
done
TOTAL_2026=$(ls -1 "$STAGING_DIR/2026/"*.json 2>/dev/null | wc -l | tr -d ' ')
echo "  Found $TOTAL_2026 matches from 2026"

# ── Step 3: Detect new matches ──────────────────────────────────────────────
echo ""
echo "━━━ Step 3/7: Detecting new matches ━━━"
mkdir -p "$RAW_DATA_DIR"
NEW_COUNT=0
NEW_FILES=""
for f in "$STAGING_DIR/2026/"*.json; do
  fname=$(basename "$f")
  if [ ! -f "$RAW_DATA_DIR/$fname" ]; then
    NEW_COUNT=$((NEW_COUNT + 1))
    NEW_FILES="$NEW_FILES $fname"
    if [ "$DRY_RUN" = false ]; then
      cp "$f" "$RAW_DATA_DIR/"
    fi
  fi
done

EXISTING=$(ls -1 "$RAW_DATA_DIR/"*.json 2>/dev/null | wc -l | tr -d ' ')

if [ "$NEW_COUNT" -eq 0 ]; then
  echo "  ✅ No new matches found. $EXISTING matches already up to date."
  if [ "$DRY_RUN" = true ]; then
    echo "  (dry-run complete)"
  fi
  rm -rf "$STAGING_DIR"
  echo ""
  echo "Done! No pipeline run needed."
  exit 0
fi

echo "  📊 Existing: $EXISTING matches"
echo "  🆕 New:      $NEW_COUNT match(es)"
for nf in $NEW_FILES; do
  echo "     + $nf"
done

if [ "$DRY_RUN" = true ]; then
  echo ""
  echo "  (dry-run complete — no changes made)"
  rm -rf "$STAGING_DIR"
  exit 0
fi

# ── Step 4: Rebuild JAR if needed ───────────────────────────────────────────
echo ""
echo "━━━ Step 4/7: Checking build ━━━"
if [ ! -f "$JAR" ]; then
  echo "  Building project..."
  cd "$PROJECT_DIR" && mvn clean package -DskipTests -q
  echo "  ✅ Build complete"
else
  echo "  ✅ JAR exists: $(basename "$JAR")"
fi

# ── Step 5: Run Spark pipeline (full reprocess) ────────────────────────────
echo ""
echo "━━━ Step 5/7: Running Spark batch pipeline ━━━"
echo "  Clearing Delta tables for full recompute..."
rm -rf "$DELTA_BASE"

echo "  Processing $((EXISTING + NEW_COUNT)) matches..."
spark-submit \
  --class com.rajesh.cricket.Main \
  --master "${SPARK_MASTER:-local[*]}" \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.driver.memory=4g" \
  --conf "spark.executor.memory=4g" \
  "$JAR" --mode batch --data-path "$RAW_DATA_DIR" 2>&1 | \
  grep -E "INFO Main|INFO Gold|INFO Bronze|INFO Silver|ERROR|Exception" || true

echo "  ✅ Spark pipeline complete"

# ── Step 6: Export Parquet + rebuild player map ─────────────────────────────
echo ""
echo "━━━ Step 6/7: Exporting data for Streamlit dashboard ━━━"

# Rebuild player-team mapping
cd "$STREAMLIT_DIR"
python3 build_player_map.py 2>&1 | sed 's/^/  /'

# Export Parquet
python3 export_data.py 2>&1 | tail -5 | sed 's/^/  /'

echo "  ✅ Export complete"

# ── Step 7: Deploy (optional) ──────────────────────────────────────────────
echo ""
if [ "$DEPLOY" = true ]; then
  echo "━━━ Step 7/7: Deploying to Streamlit Cloud ━━━"
  cd "$PROJECT_DIR"
  git add visualization/streamlit/data/
  git add visualization/streamlit/data/player_teams.json

  TIMESTAMP=$(date '+%Y-%m-%d %H:%M')
  TOTAL_NOW=$(ls -1 "$RAW_DATA_DIR/"*.json | wc -l | tr -d ' ')
  git commit -m "Data refresh: $TOTAL_NOW matches (+$NEW_COUNT new) — $TIMESTAMP" || {
    echo "  ℹ️  No changes to commit"
  }
  git push origin HEAD
  echo "  ✅ Pushed to Git — Streamlit Cloud will auto-deploy"
else
  echo "━━━ Step 7/7: Skipping deploy (use --deploy to push) ━━━"
  echo "  To deploy manually:"
  echo "    cd $PROJECT_DIR"
  echo "    git add visualization/streamlit/data/"
  echo "    git commit -m 'Data refresh'"
  echo "    git push origin HEAD"
fi

# Cleanup
rm -rf "$STAGING_DIR"

echo ""
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║  ✅ Refresh complete!                                            ║"
echo "║  📊 Total matches: $((EXISTING + NEW_COUNT))                                          ║"
echo "║  🆕 New matches:   $NEW_COUNT                                                ║"
echo "╚══════════════════════════════════════════════════════════════════╝"



