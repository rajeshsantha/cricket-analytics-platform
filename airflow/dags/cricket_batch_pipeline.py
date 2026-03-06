"""
🏏 Cricket Analytics Platform — Airflow DAG
============================================

Orchestrates the full batch pipeline:
  Task 1: Build JAR (mvn package) — skipped if JAR already exists
  Task 2: Spark-submit batch job  (Cricsheet JSON → Bronze → Silver → Gold)
  Task 3: Export Parquet          (Gold Delta → Streamlit Parquet files)
  Task 4: Rebuild player map      (player → team JSON mapping)

Schedule: Daily at 07:00 UTC (12:30 PM IST — after morning matches)
          Can also be triggered manually from the Airflow UI.

Setup:
  1. Copy (or symlink) this file to ~/airflow/dags/
  2. Start Airflow:  airflow standalone
  3. Open http://localhost:8080 → Enable the DAG → Trigger
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator

# ─── Configuration — update these paths for your environment ─────────────────
PROJECT_DIR = "/Users/rajeshsantha/IdeaProjects/cricket-analytics-platform"
SPARK_HOME = "/Users/rajeshsantha/spark411"
SPARK_SUBMIT = f"{SPARK_HOME}/bin/spark-submit"
JAR_PATH = f"{PROJECT_DIR}/target/cricket-analytics-platform-1.0.0-SNAPSHOT-jar-with-dependencies.jar"
DATA_PATH = "/Users/rajeshsantha/Datasets/t20_wc_2026_json"   # Cricsheet JSON (primary)
DATA_PATH_ALT = f"{STREAMLIT_DIR}/data/raw_json"              # Fallback: bundled raw JSON
DELTA_BASE = "/tmp/cricket-delta"
STREAMLIT_DIR = f"{PROJECT_DIR}/visualization/streamlit"
JAVA_HOME = "/opt/homebrew/opt/openjdk@21"

# Environment variables passed to all tasks
ENV_VARS = {
    "JAVA_HOME": JAVA_HOME,
    "SPARK_HOME": SPARK_HOME,
    "PATH": f"{JAVA_HOME}/bin:{SPARK_HOME}/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin",
    "DELTA_BASE_PATH": DELTA_BASE,
}

# ─── DAG definition ──────────────────────────────────────────────────────────

default_args = {
    "owner": "rajesh",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=30),
    "email_on_failure": False,
}

with DAG(
    dag_id="cricket_analytics_batch_pipeline",
    default_args=default_args,
    description="🏏 Cricket Analytics: Cricsheet → Spark → Delta Lake → Dashboard",
    schedule="0 7 * * *",            # Daily at 07:00 UTC
    catchup=False,
    max_active_runs=1,               # Only one pipeline run at a time
    tags=["cricket", "spark", "etl", "delta-lake"],
) as dag:

    # ── Task 1: Build JAR if not present ──────────────────────────────────
    build_jar = BashOperator(
        task_id="build_jar",
        bash_command=f"""
            set -e
            if [ -f "{JAR_PATH}" ]; then
                echo "✅ JAR already exists: {JAR_PATH}"
                echo "   Size: $(du -h '{JAR_PATH}' | cut -f1)"
            else
                echo "🔨 Building project..."
                cd {PROJECT_DIR}
                mvn clean package -DskipTests -q
                echo "✅ Build complete: $(du -h '{JAR_PATH}' | cut -f1)"
            fi
        """,
        env=ENV_VARS,
    )

    # ── Task 2: Run Spark batch pipeline ──────────────────────────────────
    spark_batch = BashOperator(
        task_id="spark_submit_batch",
        bash_command=f"""
            set -e
            # Use primary data path, fall back to bundled raw_json
            DATA="{DATA_PATH}"
            if [ ! -d "$DATA" ] || [ -z "$(ls -A "$DATA"/*.json 2>/dev/null)" ]; then
                DATA="{DATA_PATH_ALT}"
            fi
            echo "🏏 Starting Spark batch pipeline"
            echo "   Data path:  $DATA"
            echo "   Delta base: {DELTA_BASE}"
            echo "   JAR:        {JAR_PATH}"

            {SPARK_SUBMIT} \\
              --class com.rajesh.cricket.Main \\
              --master "local[*]" \\
              --packages io.delta:delta-spark_2.13:4.0.0 \\
              --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \\
              --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \\
              --conf "spark.driver.memory=4g" \\
              --conf "spark.executor.memory=4g" \\
              --conf "spark.sql.session.timeZone=UTC" \\
              "{JAR_PATH}" \\
              --mode batch \\
              --data-path "$DATA"

            echo "✅ Spark batch pipeline completed"
        """,
        env=ENV_VARS,
        execution_timeout=timedelta(minutes=20),
    )

    # ── Task 3: Export Gold KPIs to Parquet for Streamlit ─────────────────
    export_parquet = BashOperator(
        task_id="export_parquet",
        bash_command=f"""
            set -e
            echo "📦 Exporting Gold KPIs to Parquet..."
            cd {STREAMLIT_DIR}
            python3 export_data.py
            TOTAL=$(ls -1 data/*.parquet 2>/dev/null | wc -l | tr -d ' ')
            echo "✅ Exported $TOTAL Parquet files to {STREAMLIT_DIR}/data/"
        """,
        env=ENV_VARS,
    )

    # ── Task 4: Rebuild player-team mapping ───────────────────────────────
    rebuild_player_map = BashOperator(
        task_id="rebuild_player_map",
        bash_command=f"""
            set -e
            echo "🗺️ Rebuilding player-team mapping..."
            cd {STREAMLIT_DIR}
            python3 build_player_map.py
            echo "✅ Player map rebuilt"
        """,
        env=ENV_VARS,
    )

    # ── Task 5: Recompute lightweight KPIs (Python fallback) ─────────────
    compute_kpis_lightweight = BashOperator(
        task_id="compute_kpis_lightweight",
        bash_command=f"""
            set -e
            echo "📊 Recomputing KPIs (Python/Pandas)..."
            cd {PROJECT_DIR}
            python3 scripts/compute_kpis_lightweight.py
            echo "✅ Lightweight KPIs recomputed"
        """,
        env=ENV_VARS,
    )

    # ── Task 6 (optional): Git commit & push for Streamlit Cloud deploy ──
    deploy_to_cloud = BashOperator(
        task_id="deploy_to_streamlit_cloud",
        bash_command=f"""
            set -e
            cd {PROJECT_DIR}
            git add visualization/streamlit/data/
            TIMESTAMP=$(date '+%Y-%m-%d %H:%M')
            TOTAL=$(ls -1 {STREAMLIT_DIR}/data/*.parquet | wc -l | tr -d ' ')
            git diff --cached --quiet && echo "ℹ️ No changes to commit" && exit 0
            git commit -m "🏏 Airflow auto-refresh: $TOTAL KPIs updated — $TIMESTAMP [skip ci]"
            git push origin HEAD
            echo "✅ Pushed to Git — Streamlit Cloud will auto-deploy"
        """,
        env=ENV_VARS,
        trigger_rule="all_success",
    )

    # ── Pipeline DAG: task dependencies ───────────────────────────────────
    #
    #  build_jar → spark_batch → export_parquet → deploy_to_cloud
    #                               ↓
    #                          rebuild_player_map
    #                               ↓
    #                      compute_kpis_lightweight
    #
    build_jar >> spark_batch >> export_parquet
    export_parquet >> rebuild_player_map >> compute_kpis_lightweight >> deploy_to_cloud

