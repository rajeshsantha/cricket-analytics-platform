# 🏏 Cricket Analytics Platform

> **A production-grade cricket analytics system** built with Scala, Apache Spark, Delta Lake,
> and Streamlit. Ingests ball-by-ball data from Cricsheet and CricAPI, processes it through a
> medallion architecture (Bronze → Silver → Gold), and serves 30 KPIs via an interactive dashboard.

**Live Dashboard:** [cricket-insights.streamlit.app](https://cricket-insights.streamlit.app)

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tournament Modes (Branch Strategy)](#-tournament-modes-branch-strategy)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start-macos)
- [Running Each Mode](#-running-each-mode)
- [Data Refresh Pipeline](#-data-refresh--when-new-matches-finish)
- [Dashboard Features](#-streamlit-dashboard)
- [Gold KPIs (30 Queries)](#-gold-kpis-30-queries)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [Kafka & Docker](#kafka--docker)
- [Tests](#tests)
- [Tech Stack](#tech-stack)
- [Adding a New Tournament](#-adding-a-new-tournament)
- [Author](#-author)
- [License](#license)

---

## Overview

The Cricket Analytics Platform is an end-to-end data engineering project that demonstrates:

- **Batch ingestion** of Cricsheet JSON data through a Spark-powered medallion pipeline
- **Real-time streaming** of live match data via CricAPI → Kafka → Spark Structured Streaming
- **30 Gold-layer KPIs** covering batting, bowling, team, venue/toss, and match trend analytics
- **Interactive dashboard** with country flags, ICC-style tables, and team filtering
- **Multiple deployment modes** — from zero-dependency Streamlit Cloud to full Spark + Kafka
- **Airflow orchestration** for automated daily pipeline runs
- **Multi-tournament support** via isolated Git branches (IPL, T20 World Cup, etc.)

| Component | Technology |
|-----------|-----------|
| Data Processing | Apache Spark 4.1.1 (Scala 2.13) |
| Storage | Delta Lake 4.1.0 (Medallion Architecture) |
| Streaming | Spark Structured Streaming + Apache Kafka |
| Dashboard | Streamlit + Plotly |
| Orchestration | Apache Airflow (local) + GitHub Actions (CI/CD) |
| Live Scores | CricAPI REST API |

---

## Architecture

### Batch Pipeline (Cricsheet → Delta Lake → Dashboard)

```
Cricsheet (JSON files)
        ↓
CricsheetIngestionJob     → parse JSON, add metadata
        ↓
BronzeBatchJob            → Delta: bronze/cricsheet      (raw records)
        ↓
SilverBatchJob            → Delta: silver/deliveries     (flattened, cleaned)
        ↓
GoldBatchKPIs             → Delta: gold/batch_kpis       (30 KPIs via SQL)
        ↓
Streamlit Dashboard       → reads Gold Delta or bundled Parquet
```

### Streaming Pipeline (CricAPI → Kafka → Delta Lake)

```
CricAPI (REST poll every 2s)
        ↓
CricApiPoller             → HTTP GET, JSON parse
        ↓
KafkaProducer             → Topic: cricket-live-balls
        ↓
KafkaStreamReader         → Spark readStream from Kafka
        ↓
BronzeStreamingJob        → Delta: bronze/live_balls     (raw JSON)
        ↓
SilverStreamingJob        → Delta: silver/live_balls     (flattened, watermarked)
        ↓
GoldStreamingKPIs         → Delta: gold/live_kpis        (windowed aggregations)
        ↓
Live Scorecard            → Streamlit with auto-refresh
```

### Lightweight Pipeline (Python-only, for CI/Cloud)

```
Cricsheet (JSON files)
        ↓
compute_kpis_lightweight.py   → Pandas: flatten + compute 30 KPIs
        ↓
Parquet files                 → visualization/streamlit/data/*.parquet
        ↓
Streamlit Cloud               → no Spark, no Kafka, no Delta needed
```

### Airflow-Orchestrated Pipeline

```
Airflow Scheduler (daily @ 07:00 UTC)
        ↓
build_jar         → mvn package (skipped if JAR exists)
        ↓
spark_batch       → spark-submit (Bronze → Silver → Gold)
        ↓
export_parquet    → Gold Delta → Parquet for Streamlit
        ↓
rebuild_map       → player → team JSON mapping
        ↓
compute_kpis      → Python fallback KPI recomputation
        ↓
deploy            → git commit + push → Streamlit Cloud auto-deploy
```

---

## 🏟️ Tournament Modes (Branch Strategy)

Each tournament is maintained on its own Git branch with its own data, thresholds, and deployment.

| Branch | Tournament | Data | Matches | Status |
|--------|-----------|------|---------|--------|
| `main` | IPL (original) | Cricsheet IPL JSON | All IPL seasons | ✅ Stable |
| `tournament/ipl` | IPL (frozen copy) | Same as main | All IPL seasons | ✅ Frozen |
| `t20-worldcup-2026` | T20 World Cup 2026 | Cricsheet T20 WC JSON | 48+ matches | ✅ Active |
| `feature/auto-refresh` | T20 WC + Auto Refresh | Same + GitHub Actions CI | 48+ matches | ✅ Active |
| `feature/live-score` | T20 WC + Live Scores | CricAPI REST (real-time) | Live matches | ✅ Active |

```bash
# Switch between tournaments
git checkout t20-worldcup-2026        # T20 World Cup 2026
git checkout tournament/ipl           # IPL
git checkout feature/auto-refresh     # T20 WC with auto-refresh
git checkout feature/live-score       # T20 WC with live scores + Airflow
```

### Branch Highlights

| Branch | Key Features |
|--------|-------------|
| **`t20-worldcup-2026`** | 48 matches, 11,306 deliveries, country flags, ICC-style team filtering |
| **`feature/auto-refresh`** | + GitHub Actions daily refresh, `refresh_data.sh`, auto-deploy |
| **`feature/live-score`** | + Live Score page (CricAPI), CLI tool, Airflow DAG, match ID deep-links |

---

## Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Java | 21 | `brew install openjdk@21` |
| Scala | 2.13 | `brew install scala` |
| Maven | 3.8+ | `brew install maven` |
| Spark | 4.1.1 | `brew install apache-spark` |
| Docker | Latest | `brew install --cask docker` |
| Python | 3.9+ | `brew install python` |

> **Tip:** For the dashboard-only or Python-only modes, only Python 3.9+ is required.

---

## Quick Start (macOS)

```bash
# 1. Clone and set up
git clone https://github.com/rajeshsantha/cricket-analytics-platform.git
cd cricket-analytics-platform
bash scripts/setup_mac.sh

# 2. Choose a tournament
git checkout feature/live-score       # recommended (latest features)

# 3. Launch the dashboard (pre-computed data included)
cd visualization/streamlit
pip install -r requirements.txt
streamlit run app.py                  # opens http://localhost:8501
```

---

## 🚀 Running Each Mode

### Mode 1: Streamlit Dashboard Only (no Spark/Kafka needed)

Uses pre-computed Parquet files bundled in the repository.

```bash
cd visualization/streamlit
pip install -r requirements.txt
streamlit run app.py
```

### Mode 2: Python-Only KPI Recomputation (no Spark needed)

Recompute all 30 KPIs from raw JSON using Pandas (~3 seconds).

```bash
python3 scripts/compute_kpis_lightweight.py
python3 visualization/streamlit/build_player_map.py
cd visualization/streamlit && streamlit run app.py
```

### Mode 3: Full Spark Batch Pipeline

Complete Scala/Spark pipeline with Delta Lake.

```bash
# Build
mvn clean package -DskipTests

# Run
spark-submit \
  --class com.rajesh.cricket.Main \
  --master "local[*]" \
  --packages io.delta:delta-spark_2.13:4.0.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.driver.memory=4g" \
  target/cricket-analytics-platform-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  --mode batch --data-path /path/to/cricsheet/json

# Or use the convenience script:
bash scripts/run_batch.sh /path/to/cricsheet/json
```

### Mode 4: Streaming Pipeline (Live CricAPI → Kafka → Dashboard)

Real-time ball-by-ball analytics during live matches.

```bash
# 1. Start Kafka
cd docker && docker-compose up -d

# 2. Create topics
bash scripts/create_kafka_topics.sh

# 3. Start streaming pipeline
bash scripts/run_streaming.sh

# 4. Start CricAPI poller (separate terminal)
spark-submit \
  --class com.rajesh.cricket.Main \
  target/cricket-analytics-platform-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  --mode poller

# 5. Open dashboard → Live Scorecard page
cd visualization/streamlit && streamlit run app.py
```

### Mode 5: Gold KPI Recomputation Only (Spark)

Recompute Gold KPIs without re-ingesting Bronze/Silver.

```bash
spark-submit \
  --class com.rajesh.cricket.Main \
  --packages io.delta:delta-spark_2.13:4.0.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  target/cricket-analytics-platform-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  --mode gold
```

### Mode 6: Live Match Scores (Python-only, no Spark/Kafka)

Real-time scores via CricAPI. No infrastructure needed.

**Dashboard:**
```bash
export CRICAPI_KEY="your-api-key"
cd visualization/streamlit && streamlit run app.py
# Navigate to "📡 Live Score" → enter a Match ID or browse live matches
```

**Deep-link:** `http://localhost:8501/?match_id=<your-match-id>`

**CLI:**
```bash
python3 scripts/live_score.py --list                                 # browse matches
python3 scripts/live_score.py --match-id <id>                        # show score
python3 scripts/live_score.py --match-id <id> --watch --interval 10  # auto-refresh
python3 scripts/live_score.py --match-id <id> --json                 # JSON output
```

> Get a free CricAPI key at [cricapi.com](https://cricapi.com) (100 requests/day).

### Mode 7: Airflow-Orchestrated Batch Pipeline

Automate the full batch pipeline with Apache Airflow.

```bash
# 1. Start Airflow
airflow standalone
# Open http://localhost:8080 (check ~/airflow/ for credentials)

# 2. The DAG is auto-discovered via symlink
bash scripts/start_airflow.sh          # start + symlink in one step
bash scripts/start_airflow.sh --trigger # start + immediately trigger

# 3. Enable & trigger from the Airflow UI:
#    DAG name: cricket_analytics_batch_pipeline
#    Schedule: daily at 07:00 UTC
```

**DAG tasks:** `build_jar → spark_batch → export_parquet → rebuild_player_map → compute_kpis → deploy`

### Spark Submit Modes Summary

```bash
spark-submit ... Main --mode batch --data-path /path   # Cricsheet → Bronze → Silver → Gold
spark-submit ... Main --mode streaming                  # Kafka → Bronze → Silver → Gold
spark-submit ... Main --mode poller                     # CricAPI → Kafka
spark-submit ... Main --mode gold                       # Recompute Gold only
```

---

## 🔄 Data Refresh — When New Matches Finish

### Option A: One-Command Local Refresh

```bash
bash scripts/refresh_data.sh --dry-run   # preview new matches
bash scripts/refresh_data.sh             # download + reprocess
bash scripts/refresh_data.sh --deploy    # reprocess + git push (auto-deploy)
```

### Option B: Python-Only Refresh (no Spark)

```bash
python3 scripts/refresh_data_ci.py               # detect & download new matches
python3 scripts/compute_kpis_lightweight.py      # recompute 30 KPIs (~3s)
python3 visualization/streamlit/build_player_map.py  # rebuild player map
git add visualization/streamlit/data/ && git commit -m "Data refresh" && git push
```

### Option C: Automatic (GitHub Actions)

A workflow runs daily at 06:00 UTC on `t20-worldcup-2026` and `feature/auto-refresh` branches.
It downloads new matches, recomputes KPIs, and auto-commits for Streamlit Cloud deploy.

**Manual trigger:** GitHub → Actions → "🏏 Refresh T20 WC Data" → Run workflow

---

## 📊 Streamlit Dashboard

### Live Demo: [cricket-insights.streamlit.app](https://cricket-insights.streamlit.app)

### Pages (8 pages, 30+ KPIs)

| Page | Features |
|------|----------|
| 🏠 **Overview** | Headline metrics, top scorers/wicket-takers, run-rate chart |
| 📡 **Live Score** | CricAPI live scores, batting/bowling cards, auto-refresh, deep-links |
| 🏏 **Batting** | 9 tabs — Top Scorers, Avg, Strike Rate, Highest Scores, 6s & 4s, Partnerships, Consistency |
| 🎳 **Bowling** | 6 tabs — Wicket Takers, Avg, Economy, Death Overs, Dot Ball %, Best Spells |
| 👥 **Team Analytics** | 7 tabs — Powerplay, Totals, Chases, Wins, Runs/Wicket, Extras, Head-to-Head |
| 🏟️ **Venue & Toss** | 4 tabs — Toss Impact, Bat First vs Chase, Venue Scores, Home/Away |
| 📈 **Match Trends** | 3 tabs — Boundary Analysis, Pressure Index, Run Rate Progression |
| 📊 **Live Scorecard** | Spark Structured Streaming metrics (requires Kafka pipeline) |

### Key Features

- 🏳️ **Country flags** next to every player name (🇮🇳 V Kohli, 🇦🇺 SPD Smith)
- 🔍 **Team filtering** on all stat pages (ICC-style dropdown)
- 📊 **ICC-style tables** with Matches, Innings, Avg, SR, Highest Score
- 📈 **Interactive Plotly charts** with hover tooltips
- ☁️ **Cloud-ready** — ships with pre-computed Parquet; deploys without Spark

---

## 🏆 Gold KPIs (30 Queries)

Computed by both `GoldBatchKPIs.scala` (Spark SQL) and `compute_kpis_lightweight.py` (Pandas).

| # | KPI | Category |
|---|-----|----------|
| 1 | Top run scorers (with strike rate & highest score) | Batting |
| 2 | Top wicket takers | Bowling |
| 3 | Best batting average (min 3 innings) | Batting |
| 4 | Best bowling average (min 3 wickets) | Bowling |
| 5 | Best strike rate (min 30 balls) | Batting |
| 6 | Best economy rate (min 5 overs) | Bowling |
| 7 | Highest individual scores | Batting |
| 8 | Most sixes hit | Batting |
| 9 | Most fours hit | Batting |
| 10 | Best powerplay run rate by team | Team |
| 11 | Best death over economy | Bowling |
| 12 | Win % by toss decision | Venue/Toss |
| 13 | Win % batting first vs chasing | Venue/Toss |
| 14 | Average score by venue | Venue/Toss |
| 15 | Highest team totals | Team |
| 16 | Lowest successful chases | Team |
| 17 | Most matches won by team | Team |
| 18 | Player performance by match type | Batting |
| 19 | Partnership analysis (top pairs) | Batting |
| 20 | Dot ball % by bowler | Bowling |
| 21 | Boundary % per over phase (PP/Middle/Death) | Trends |
| 22 | Average runs per wicket by innings | Team |
| 23 | Pressure index per over | Trends |
| 24 | Run rate progression over-by-over | Trends |
| 25 | Extras analysis by team | Team |
| 26 | Home vs away win percentage | Venue/Toss |
| 27 | Head-to-head team records | Team |
| 28 | Player consistency (score std deviation) | Batting |
| 29 | Best bowling spells (5-over window) | Bowling |
| 30 | Match-winning contribution index | Batting |

---

## Project Structure

```
cricket-analytics-platform/
├── pom.xml                              # Maven build (Scala 2.13, Spark 4.1.1)
├── airflow/
│   └── dags/
│       └── cricket_batch_pipeline.py    # Airflow DAG (6-task pipeline)
├── docker/
│   ├── docker-compose.yml               # Kafka + Zookeeper + Kafka UI
│   └── kafka-ui/application.yml
├── scripts/
│   ├── setup_mac.sh                     # One-command macOS setup
│   ├── run_batch.sh                     # Spark batch pipeline
│   ├── run_streaming.sh                 # Spark streaming pipeline
│   ├── refresh_data.sh                  # One-command data refresh
│   ├── refresh_data_ci.py               # CI: detect & download new matches
│   ├── compute_kpis_lightweight.py      # Python KPI computation (no Spark)
│   ├── live_score.py                    # CLI: live match scores
│   ├── start_airflow.sh                 # Start Airflow + link DAG
│   ├── create_kafka_topics.sh           # Kafka topic setup
│   └── reset_kafka.sh                   # Reset Kafka state
├── src/main/scala/com/rajesh/cricket/
│   ├── Main.scala                       # Entry point (batch/streaming/poller/gold)
│   ├── config/                          # Configuration loading
│   ├── model/                           # Case classes (Cricsheet + Streaming)
│   ├── ingestion/                       # Data ingestion (batch + streaming)
│   ├── bronze/                          # Raw data layer
│   ├── silver/                          # Cleaned data layer
│   ├── gold/                            # Analytics layer (30 KPIs)
│   ├── analytics/                       # Window functions, pressure index
│   └── utils/                           # Delta, HTTP, Kafka, Schema helpers
├── src/test/scala/                      # Unit tests (4 test classes)
├── visualization/streamlit/
│   ├── app.py                           # Dashboard entry point (8-page router)
│   ├── components/                      # Live score poller (CricAPI)
│   ├── dashboard/                       # 8 page modules + helpers
│   └── data/                            # Raw JSON + Parquet KPIs
└── notebooks/                           # Exploratory analysis notebooks
```

---

## Configuration

### Environment Variables

```bash
CRICAPI_KEY=your_api_key_here           # CricAPI key (for live scores / poller)
CRICAPI_MATCH_ID=your_match_id          # Match ID for poller mode
KAFKA_BOOTSTRAP_SERVERS=localhost:9092   # Kafka (for streaming mode)
DELTA_BASE_PATH=/tmp/cricket-delta      # Delta Lake root path
```

### Typesafe Config Files

| File | Purpose |
|------|---------|
| `conf/application.conf` | Main config (includes the others) |
| `conf/kafka.conf` | Kafka bootstrap servers and topics |
| `conf/delta.conf` | Delta Lake table paths |
| `conf/cricapi.conf` | CricAPI URL, key, poll interval |

---

## Kafka & Docker

```bash
# Start Kafka + Zookeeper + Kafka UI
cd docker && docker-compose up -d

# Create topics
bash scripts/create_kafka_topics.sh

# Kafka UI: http://localhost:8080
```

| Topic | Partitions | Purpose |
|-------|-----------|---------|
| `cricket-live-balls` | 3 | Ball-by-ball events from CricAPI |
| `cricket-live-matches` | 3 | Match metadata |
| `cricket-batch` | 3 | Batch processing events |

---

## Tests

```bash
mvn test
```

| Test Class | Coverage |
|------------|----------|
| `BronzeJobSpec` | File reading, Delta writing, schema validation |
| `SilverJobSpec` | Data quality filters, null handling, type coercion |
| `GoldKPISpec` | 5 KPI SQL queries against in-memory data |
| `CricApiPollerSpec` | JSON parsing from mock CricAPI responses |

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Scala 2.13 |
| Build | Maven 3.8+ |
| Processing | Apache Spark 4.1.1 |
| Streaming | Spark Structured Streaming |
| Message Queue | Apache Kafka |
| Storage | Delta Lake 4.1.0 |
| HTTP Client | sttp 3.9.0 |
| JSON | Circe 0.14.6 |
| Config | Typesafe Config 1.4.2 |
| Testing | ScalaTest 3.2.17 |
| Dashboard | Streamlit 1.32+ |
| Charts | Plotly 5.18+ |
| Orchestration | Apache Airflow |
| CI/CD | GitHub Actions |
| Cloud | Streamlit Cloud |

---

## 🛠️ Adding a New Tournament

```bash
# 1. Create branch from main
git checkout -b tournament/odi-wc-2027 main

# 2. Add Cricsheet JSON files to visualization/streamlit/data/raw_json/

# 3. Compute KPIs + player map
python3 scripts/compute_kpis_lightweight.py
python3 visualization/streamlit/build_player_map.py

# 4. Update app.py sidebar branding for the tournament

# 5. Deploy
git add -A && git commit -m "Add ODI WC 2027" && git push -u origin tournament/odi-wc-2027
```

---

## 👤 Author

**Rajesh Santha**

Built as a portfolio project demonstrating end-to-end data engineering with
Apache Spark, Delta Lake, Kafka, Airflow, and Streamlit.

- GitHub: [github.com/rajeshsantha](https://github.com/rajeshsantha)

---

## License

This project uses publicly available cricket data from
[Cricsheet](https://cricsheet.org) (CC BY 4.0) and
[CricAPI](https://cricapi.com) for live scores.

