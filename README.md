# 🏏 Cricket Analytics Platform

A **production-ready Cricket Analytics Platform** built in Scala (Maven) for macOS development.
Ingests live ball-by-ball cricket data from CricAPI (REST), streams it through Kafka, processes it
with Spark Structured Streaming, and stores it in a Delta Lake medallion architecture (Bronze → Silver → Gold),
with a Streamlit visualization layer.

Supports **multiple tournament modes** via Git branches — each tournament is an isolated, independently
deployable analytics dashboard.

---

## 🌐 Live Dashboards

| Tournament | Branch | Live URL |
|------------|--------|----------|
| 🏆 **T20 World Cup 2026** | `t20-worldcup-2026` / `feature/auto-refresh` | [cricket-insights.streamlit.app](https://cricket-insights.streamlit.app) |
| 🏏 **IPL (all seasons)** | `tournament/ipl` | _(deploy from branch)_ |

---

## 📋 Table of Contents

- [Architecture](#architecture)
- [Tournament Modes (Branch Strategy)](#-tournament-modes-branch-strategy)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start-macos)
- [Running Each Mode](#-running-each-mode)
- [Data Refresh Pipeline](#-data-refresh--when-new-matches-finish)
- [Dashboard Features](#-streamlit-dashboard)
- [Gold KPIs (30 queries)](#-gold-kpis-30-queries)
- [Project Structure](#project-structure)
- [Configuration Guide](#configuration-guide)
- [Kafka & Docker](#kafka--docker)
- [Running Tests](#running-tests)
- [Tech Stack](#tech-stack)

---

## Architecture

### Batch Pipeline (Cricsheet JSON → Delta Lake → Dashboard)
```
Cricsheet (JSON files)
        ↓
CricsheetIngestionJob.scala   (parse JSON, add metadata)
        ↓
BronzeBatchJob.scala          → Delta: /data/bronze/cricsheet  (raw JSON)
        ↓
SilverBatchJob.scala          → Delta: /data/silver/cricsheet  (flattened, cleaned)
        ↓
GoldBatchKPIs.scala           → Delta: /data/gold/batch_kpis   (30 KPIs via SQL)
        ↓
Streamlit Dashboard           (reads Gold Delta or bundled Parquet)
```

### Streaming Pipeline (Live CricAPI → Kafka → Delta Lake)
```
CricAPI (REST Poll every 2s)
        ↓
CricApiPoller.scala           (HTTP GET → JSON parse)
        ↓
KafkaProducer.scala           → Kafka Topic: cricket-live-balls
        ↓
KafkaStreamReader.scala       (Spark readStream from Kafka)
        ↓
BronzeStreamingJob.scala      → Delta: /data/bronze/live_balls  (raw JSON)
        ↓  (watermark: 10 min, late data tolerated)
SilverStreamingJob.scala      → Delta: /data/silver/live_balls  (flattened)
        ↓  (stateful aggregations: window, groupBy)
GoldStreamingKPIs.scala       → Delta: /data/gold/live_kpis     (aggregated KPIs)
        ↓
Streamlit Dashboard           (Live Scorecard page with auto-refresh)
```

### Lightweight Pipeline (Python-only, for CI/Cloud)
```
Cricsheet (JSON files)
        ↓
compute_kpis_lightweight.py   (Pandas: flatten + compute 30 KPIs)
        ↓
Parquet files                 → visualization/streamlit/data/*.parquet
        ↓
Streamlit Cloud               (no Spark, no Kafka, no Delta needed)
```

---

## 🏟️ Tournament Modes (Branch Strategy)

Each tournament is maintained on its own Git branch with its own data, thresholds, and deployment.
Branches are isolated — a PR or push to one branch never affects another tournament's dashboard.

| Branch | Tournament | Data Source | Matches | Status |
|--------|-----------|-------------|---------|--------|
| `main` | IPL (original) | Cricsheet IPL JSON | All IPL seasons | ✅ Stable |
| `tournament/ipl` | IPL (frozen copy) | Same as main | All IPL seasons | ✅ Frozen |
| `t20-worldcup-2026` | ICC T20 World Cup 2026 | Cricsheet T20 WC 2026 JSON | 48+ matches | ✅ Active |
| `feature/auto-refresh` | T20 WC 2026 + Auto Refresh | Same + GitHub Actions CI | 48+ matches | ✅ Active |
| `feature/live-score` | T20 WC 2026 + Live Scores | CricAPI REST (real-time) | Live matches | ✅ Active |

### Switching Between Tournaments

```bash
# Work on T20 World Cup 2026
git checkout t20-worldcup-2026

# Work on IPL
git checkout tournament/ipl

# Work on auto-refresh (T20 WC 2026 with incremental pipeline)
git checkout feature/auto-refresh

# Create a new tournament branch (e.g., ODI World Cup 2027)
git checkout -b tournament/odi-wc-2027 main
```

### Branch Details

#### `main` — IPL Analytics
- Original IPL data across all seasons
- Full Spark batch pipeline support
- Baseline for creating new tournament branches

#### `tournament/ipl` — IPL (Frozen Copy)
- Exact copy of `main` at time of creation
- Protected from any T20 WC changes
- Deploy separately to Streamlit Cloud if needed

#### `t20-worldcup-2026` — T20 World Cup 2026
- 48 matches from ICC Men's T20 World Cup 2026 (India & Sri Lanka)
- 11,306+ ball-by-ball deliveries
- Tournament-optimized KPI thresholds (3 innings, 3 wickets, 30 balls, 5 overs)
- Country flags 🇮🇳 🇦🇺 🏴 next to player names
- Team-based filtering on all stats pages (like the ICC website)
- Dashboard: [cricket-insights.streamlit.app](https://cricket-insights.streamlit.app)

#### `feature/auto-refresh` — T20 WC 2026 + Incremental Refresh
- Everything from `t20-worldcup-2026` plus:
- GitHub Actions workflow for daily auto-refresh
- `refresh_data.sh` script for local one-command refresh
- `refresh_data_ci.py` for Python-only CI refresh
- Automatic detection of new matches from Cricsheet
- Auto-commit + push for Streamlit Cloud redeployment

#### `feature/live-score` — T20 WC 2026 + Live Match Scores
- Everything from `feature/auto-refresh` plus:
- **📡 Live Score page**: Real-time match scores via CricAPI (no Kafka/Spark required)
- **Match ID parameter**: Pass `?match_id=abc123` in the URL to deep-link to a match
- **Browse Live Matches**: Lists all currently live/recent cricket matches
- **Full Scorecard**: Batting + bowling breakdowns with run charts
- **Auto-refresh**: Configurable 5–60 second polling interval
- **CLI tool**: `python3 scripts/live_score.py --match-id abc123 --watch`
- **Country flags** on live match teams

---

## Prerequisites

| Tool       | Version    | Install                            |
|------------|------------|------------------------------------|
| Java       | 21         | `brew install openjdk@21`          |
| Scala      | 2.13.17    | `brew install scala`               |
| Maven      | 3.8+       | `brew install maven`               |
| Docker     | Latest     | `brew install --cask docker`       |
| Python     | 3.9+       | `brew install python`              |
| Spark      | 4.1.1      | `brew install apache-spark`        |

> **Note:** For the lightweight Python-only pipeline (CI/Streamlit Cloud), only Python 3.9+ is required.
> Spark, Kafka, and Delta Lake are not needed.

---

## Quick Start (macOS)

```bash
# 1. Clone the repository
git clone https://github.com/rajeshsantha/cricket-analytics-platform.git
cd cricket-analytics-platform

# 2. Run the automated macOS setup script
bash scripts/setup_mac.sh

# 3. Configure your API keys (for streaming/live mode only)
cp .env.example .env
# Edit .env: set CRICAPI_KEY and CRICAPI_MATCH_ID

# 4. Choose your tournament
git checkout t20-worldcup-2026    # T20 World Cup 2026
# OR
git checkout tournament/ipl       # IPL
# OR
git checkout feature/auto-refresh # T20 WC with auto-refresh

# 5. Run the dashboard immediately (pre-computed data included)
cd visualization/streamlit
pip install -r requirements.txt
streamlit run app.py
```

---

## 🚀 Running Each Mode

### Mode 1: Streamlit Dashboard Only (no Spark/Kafka needed)

The fastest way to see the dashboard — uses pre-computed Parquet files bundled in the repo.

```bash
cd visualization/streamlit
pip install -r requirements.txt
streamlit run app.py
# Opens at http://localhost:8501
```

### Mode 2: Python-Only KPI Recomputation (no Spark needed)

Recompute all 30 KPIs from raw JSON files using pure Python/Pandas (~3 seconds).

```bash
# Ensure raw JSON match files are in visualization/streamlit/data/raw_json/
python3 scripts/compute_kpis_lightweight.py

# Rebuild player→team mapping
python3 visualization/streamlit/build_player_map.py

# View updated dashboard
cd visualization/streamlit && streamlit run app.py
```

### Mode 3: Full Spark Batch Pipeline

Run the complete Scala/Spark pipeline for full control and Delta Lake support.

```bash
# 1. Build the project
mvn clean package -DskipTests

# 2. Run batch pipeline
spark-submit \
  --class com.rajesh.cricket.Main \
  --master "local[*]" \
  --packages io.delta:delta-spark_2.13:4.0.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.driver.memory=4g" \
  target/cricket-analytics-platform-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  --mode batch --data-path /path/to/cricsheet/json

# OR use the convenience script:
bash scripts/run_batch.sh /path/to/cricsheet/json
```

### Mode 4: Streaming Pipeline (Live CricAPI → Kafka → Dashboard)

For real-time ball-by-ball analytics during live matches.

```bash
# 1. Start Kafka (Docker)
cd docker && docker-compose up -d

# 2. Create Kafka topics
bash scripts/create_kafka_topics.sh

# 3. Start streaming pipeline (Kafka → Bronze → Silver → Gold)
bash scripts/run_streaming.sh

# 4. In a separate terminal, start the CricAPI poller
spark-submit \
  --class com.rajesh.cricket.Main \
  target/cricket-analytics-platform-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  --mode poller

# 5. Open the Live Scorecard page in the Streamlit dashboard
cd visualization/streamlit && streamlit run app.py
```

### Mode 5: Gold KPI Recomputation Only (Spark)

If Bronze/Silver Delta tables already exist, recompute only the Gold KPIs.

```bash
spark-submit \
  --class com.rajesh.cricket.Main \
  --packages io.delta:delta-spark_2.13:4.0.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  target/cricket-analytics-platform-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  --mode gold
```

### All Spark Submit Modes Summary

```bash
# Batch pipeline (Cricsheet → Bronze → Silver → Gold)
spark-submit ... Main --mode batch --data-path /path/to/cricsheet

# Streaming pipeline (Kafka → Bronze → Silver → Gold)
spark-submit ... Main --mode streaming

# CricAPI poller only (publishes to Kafka)
spark-submit ... Main --mode poller

# Gold KPI recomputation only
spark-submit ... Main --mode gold
```

### Mode 6: Live Match Scores (Python-only, no Spark/Kafka)

Get real-time scores for any live cricket match via CricAPI. No Spark, Kafka, or Delta Lake required.

**Dashboard (Streamlit):**
```bash
# Set your API key
export CRICAPI_KEY="your-api-key-here"

# Start the dashboard
cd visualization/streamlit && streamlit run app.py
# Navigate to "📡 Live Score" → Enter a Match ID or browse live matches
```

**Deep-link to a specific match:**
```
http://localhost:8501/?match_id=d9032b36-d872-4011-b96c-73a9137e7ced
```

**CLI (terminal):**
```bash
# List all current live/recent matches
python3 scripts/live_score.py --list

# Show live score for a specific match
python3 scripts/live_score.py --match-id d9032b36-d872-4011-b96c-73a9137e7ced

# Watch mode — auto-refresh every 10 seconds
python3 scripts/live_score.py --match-id abc123 --watch --interval 10

# Output as JSON (for piping/processing)
python3 scripts/live_score.py --match-id abc123 --json
```

**Get a free CricAPI key:** [cricapi.com](https://cricapi.com) (100 requests/day free tier)

---

## 🔄 Data Refresh — When New Matches Finish

When a new cricket match finishes and Cricsheet publishes the data, use one of these options
to update all statistics and KPIs:

### Option A: One-Command Local Refresh (recommended for dev)

```bash
# Dry run — see what new matches are available without changing anything
bash scripts/refresh_data.sh --dry-run

# Download new matches, run Spark pipeline, re-export Parquet
bash scripts/refresh_data.sh

# Download, reprocess, AND auto-commit + push for Streamlit Cloud deploy
bash scripts/refresh_data.sh --deploy
```

**What it does (7 steps):**
1. Downloads latest Cricsheet data (ZIP)
2. Filters to 2026 T20 World Cup matches only
3. Detects new matches not yet in local raw data
4. Builds the Spark JAR if needed
5. Runs the full Spark batch pipeline (all matches)
6. Exports Parquet + rebuilds player-team mapping
7. (with `--deploy`) Commits & pushes to Git for auto-deploy

### Option B: Python-Only Refresh (no Spark required)

```bash
# Step 1: Detect & download new matches from Cricsheet
python3 scripts/refresh_data_ci.py

# Step 2: Recompute all 30 KPIs (pure Python/Pandas, ~3 seconds)
python3 scripts/compute_kpis_lightweight.py

# Step 3: Rebuild player-team mapping
python3 visualization/streamlit/build_player_map.py

# Step 4: Commit & push (for Streamlit Cloud)
git add visualization/streamlit/data/
git commit -m "Data refresh: +N new matches"
git push
```

### Option C: Automatic Daily Refresh (GitHub Actions)

A GitHub Actions workflow (`.github/workflows/refresh_data.yml`) runs **daily at 06:00 UTC**
(11:30 AM IST — after day matches finish):

1. Downloads latest Cricsheet data
2. Detects new 2026 T20 World Cup matches
3. Recomputes all 30 KPIs (Python-only, no Spark)
4. Rebuilds the player-team mapping
5. Commits updated Parquet files back to the branch
6. Streamlit Cloud auto-deploys on push

**Active on branches:** `t20-worldcup-2026`, `feature/auto-refresh`

**Manual trigger:** GitHub → Actions → 🏏 Refresh T20 WC Data → Run workflow

You can also force a full re-export even if no new matches are detected:
- Check the **"Force full re-export"** checkbox when triggering manually

### How the Pipeline Handles Incremental Data

| Layer | Strategy | Why |
|-------|----------|-----|
| **Raw JSON** | Append-only (`data/raw_json/`) | New match files are added; existing files never change |
| **KPI Parquet** | Full recompute | All 30 KPIs are recomputed from all matches (~3s in Python) |
| **Player Map** | Full rebuild | Ensures new players from new matches get team mappings |
| **Delta Lake** (local Spark only) | Drop & recreate | Spark pipeline clears Delta, reprocesses all matches |

---

## 📊 Streamlit Dashboard

### Live Demo: [cricket-insights.streamlit.app](https://cricket-insights.streamlit.app)

### Run Locally
```bash
cd visualization/streamlit
pip install -r requirements.txt
streamlit run app.py
# Opens at http://localhost:8501
```

### Dashboard Pages (8 pages, 30 KPIs)

| Page | KPIs | Features |
|------|------|----------|
| 🏠 **Overview** | 1, 2, 8, 9, 17, 24 | Headline metrics, bar charts, run-rate line chart |
| 📡 **Live Score** | Real-time | CricAPI live scores, batting/bowling scorecards, auto-refresh |
| 🏏 **Batting** | 1, 3, 5, 7, 8, 9, 18, 19, 28, 30 | 9 tabs — Top Scorers, Batting Avg, Strike Rate, Highest Scores, Sixes & Fours, By Match Type, Partnerships, Consistency, Win Contribution |
| 🎳 **Bowling** | 2, 4, 6, 11, 20, 29 | 6 tabs — Top Wicket Takers, Bowling Avg, Economy Rate, Death Overs, Dot Ball %, Best Spells |
| 👥 **Team Analytics** | 10, 15, 16, 17, 22, 25, 27 | 7 tabs — Powerplay, Team Totals, Chases, Most Wins, Runs/Wicket, Extras, Head-to-Head |
| 🏟️ **Venue & Toss** | 12, 13, 14, 26 | 4 tabs — Toss Impact, Bat First vs Chase, Venue Scores, Home/Away |
| 📈 **Match Trends** | 21, 23, 24 | 3 tabs — Boundary Analysis, Pressure Index, Run Rate Progression |
| 📊 **Live Scorecard (Spark)** | Streaming KPIs | Spark Structured Streaming via Kafka (requires full pipeline) |

### Key Dashboard Features

- **🏳️ Country Flags**: Every player name shows their country flag emoji (🇮🇳 V Kohli, 🇦🇺 SPD Smith)
- **🔍 Team Filtering**: Filter by team on all stat pages (like the ICC official website)
- **📊 ICC-Style Tables**: Matches, Innings, Bat Avg, Runs, Not Outs, Strike Rate, Highest Score
- **📈 Interactive Charts**: Plotly bar charts, scatter plots, area charts with hover tooltips
- **🌙 Dark Theme**: Default dark mode with Streamlit `config.toml`
- **☁️ Cloud-Ready**: Ships with pre-computed Parquet — deploys to Streamlit Cloud without Spark
- **🔄 Dual Data Source**: Auto-detects Delta tables (local dev) or bundled Parquet (Cloud)

### Deploy to Streamlit Cloud

1. Push the branch to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Select the repo, branch (e.g., `feature/auto-refresh`), and `visualization/streamlit/app.py`
4. Deploy — the bundled Parquet files are used automatically

To refresh data on Cloud after new matches:
```bash
bash scripts/refresh_data.sh --deploy   # reprocess + git push
# Streamlit Cloud auto-redeploys on push
```

---

## 🏆 Gold KPIs (30 Queries)

All KPIs are computed both by the Spark pipeline (`GoldBatchKPIs.scala`) and the
lightweight Python pipeline (`compute_kpis_lightweight.py`).

| # | KPI | Category |
|---|-----|----------|
| 1 | Top run scorers (all players, with strike rate & highest score) | Batting |
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
├── pom.xml                          # Maven build (Scala 2.13, Spark 4.1.1)
├── .env.example                     # Environment variable template
├── .github/
│   └── workflows/
│       └── refresh_data.yml         # GitHub Actions: daily auto-refresh
├── conf/                            # Typesafe Config files
│   ├── application.conf             # Main app config (includes others)
│   ├── kafka.conf                   # Kafka bootstrap servers and topics
│   ├── delta.conf                   # Delta Lake table paths
│   └── cricapi.conf                 # CricAPI URL, key, poll interval
├── docker/
│   ├── docker-compose.yml           # Kafka + Zookeeper + Kafka UI
│   └── kafka-ui/application.yml     # Kafka UI configuration
├── scripts/
│   ├── setup_mac.sh                 # One-command macOS setup
│   ├── run_batch.sh                 # Run Spark batch pipeline
│   ├── run_streaming.sh             # Run Spark streaming pipeline
│   ├── refresh_data.sh              # One-command data refresh (local)
│   ├── refresh_data_ci.py           # Python: detect & download new matches
│   ├── compute_kpis_lightweight.py  # Python: compute 30 KPIs (no Spark)
│   ├── create_kafka_topics.sh       # Create Kafka topics
│   ├── reset_kafka.sh               # Reset Kafka state
│   └── fix_env.sh                   # Fix environment variables
├── src/
│   ├── main/scala/com/rajesh/cricket/
│   │   ├── Main.scala               # Single entry point (batch/streaming/poller/gold)
│   │   ├── config/
│   │   │   ├── AppConfig.scala      # Typesafe Config loader
│   │   │   ├── CricApiConfig.scala  # CricAPI settings
│   │   │   └── SparkSessionFactory.scala  # SparkSession builder
│   │   ├── model/
│   │   │   ├── cricsheet/           # Match, Delivery, Player case classes
│   │   │   └── streaming/           # CricApiResponse, LiveBall, LiveMatch
│   │   ├── ingestion/
│   │   │   ├── batch/CricsheetIngestionJob.scala    # JSON file ingestion
│   │   │   └── streaming/
│   │   │       ├── CricApiPoller.scala              # HTTP poller
│   │   │       ├── KafkaProducer.scala              # Publish to Kafka
│   │   │       └── KafkaStreamReader.scala          # Read from Kafka
│   │   ├── bronze/
│   │   │   ├── BronzeBatchJob.scala                 # Batch Bronze writer
│   │   │   └── BronzeStreamingJob.scala             # Streaming Bronze writer
│   │   ├── silver/
│   │   │   ├── SilverBatchJob.scala                 # Flatten + clean
│   │   │   └── SilverStreamingJob.scala             # Streaming flatten
│   │   ├── gold/
│   │   │   ├── GoldBatchKPIs.scala                  # 30 SQL KPI queries
│   │   │   └── GoldStreamingKPIs.scala              # Windowed streaming KPIs
│   │   ├── analytics/
│   │   │   ├── WindowFunctions.scala                # Spark window analytics
│   │   │   ├── PressureIndexCalculator.scala        # Pressure index formula
│   │   │   └── TossImpactAnalyzer.scala             # Toss impact analysis
│   │   └── utils/
│   │       ├── DeltaUtils.scala                     # Delta read/write helpers
│   │       ├── HttpUtils.scala                      # HTTP client (sttp)
│   │       ├── KafkaUtils.scala                     # Kafka config helpers
│   │       └── SchemaUtils.scala                    # JSON schema utilities
│   └── test/scala/
│       ├── BronzeJobSpec.scala       # Bronze layer unit tests
│       ├── SilverJobSpec.scala       # Silver layer unit tests
│       ├── GoldKPISpec.scala         # Gold KPI SQL tests
│       └── CricApiPollerSpec.scala   # API poller tests
├── notebooks/
│   ├── 01_cricsheet_eda.scala       # Exploratory data analysis
│   ├── 02_batch_sql_queries.sql     # SQL queries for KPIs
│   ├── 03_streaming_demo.scala      # Streaming pipeline demo
│   └── 04_gold_kpis_dashboard.sql   # Gold KPI dashboard queries
└── visualization/
    ├── streamlit/
    │   ├── app.py                   # Main Streamlit app (7-page router)
    │   ├── requirements.txt         # Python dependencies
    │   ├── build_player_map.py      # Build player→team JSON mapping
    │   ├── export_data.py           # Export Delta tables → Parquet
    │   ├── dashboard/
    │   │   ├── overview.py          # Overview page
    │   │   ├── batting.py           # Batting analytics (9 tabs)
    │   │   ├── bowling.py           # Bowling analytics (6 tabs)
    │   │   ├── team_analytics.py    # Team analytics (7 tabs)
    │   │   ├── venue_toss.py        # Venue & toss (4 tabs)
    │   │   ├── match_trends.py      # Match trends (3 tabs)
    │   │   ├── live_scorecard.py    # Live scorecard (streaming)
    │   │   └── helpers.py           # Shared: flags, team filter, data loader
    │   └── data/
    │       ├── raw_json/            # Raw match JSON files (Cricsheet)
    │       ├── player_teams.json    # Player→team mapping
    │       └── *.parquet            # 30 pre-computed KPI files
    └── powerbi/
        └── README.md               # Power BI integration guide
```

---

## Configuration Guide

### Environment Variables (.env file)

```bash
CRICAPI_KEY=your_api_key_here           # Get from https://cricapi.com
CRICAPI_MATCH_ID=your_match_id_here    # Match ID from CricAPI
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
DELTA_BASE_PATH=/tmp/cricket-delta
```

### Config Files (Typesafe Config)

| File                   | Purpose                                  |
|------------------------|------------------------------------------|
| `conf/application.conf`| Main app config (includes others)        |
| `conf/kafka.conf`      | Kafka bootstrap servers and topics       |
| `conf/delta.conf`      | Delta Lake table paths                   |
| `conf/cricapi.conf`    | CricAPI URL, key, poll interval          |

---

## Kafka & Docker

### Start Kafka

```bash
cd docker
docker-compose up -d
# Kafka UI: http://localhost:8080
```

### Create Topics

```bash
bash scripts/create_kafka_topics.sh
```

Kafka topics:
| Topic | Partitions | Purpose |
|-------|-----------|---------|
| `cricket-live-balls` | 3 | Ball-by-ball events from CricAPI |
| `cricket-live-matches` | 3 | Match metadata |
| `cricket-batch` | 3 | Batch processing events |

### Kafka UI

After `docker-compose up`, open **http://localhost:8080** to:
- Monitor topic offsets and consumer groups
- Browse messages in each topic
- Inspect partition distribution

### Reset Kafka

```bash
bash scripts/reset_kafka.sh
```

---

## Running Tests

```bash
mvn test
```

| Test Class | What It Tests |
|------------|---------------|
| `BronzeJobSpec` | File reading, Delta writing, schema validation |
| `SilverJobSpec` | Data quality filters, null handling, type coercion |
| `GoldKPISpec` | 5 KPI SQL queries against in-memory data |
| `CricApiPollerSpec` | JSON parsing from mock CricAPI responses |

---

## Tech Stack

| Component      | Technology                    |
|----------------|-------------------------------|
| Language       | Scala 2.13                    |
| Build          | Maven 3.8+                    |
| Processing     | Apache Spark 4.1.1            |
| Streaming      | Spark Structured Streaming    |
| Message Queue  | Apache Kafka 4.2.0            |
| Storage        | Delta Lake 4.1.0              |
| HTTP Client    | sttp 3.9.0                    |
| JSON           | Circe 0.14.6                  |
| Config         | Typesafe Config 1.4.2         |
| Testing        | ScalaTest 3.2.17              |
| Dashboard      | Streamlit 1.32.0              |
| Visualization  | Plotly 5.18.0                 |
| CI/CD          | GitHub Actions                |
| Cloud Deploy   | Streamlit Cloud               |

---

## 🛠️ Adding a New Tournament

To add analytics for a new tournament (e.g., ODI World Cup 2027):

```bash
# 1. Create a new branch from main
git checkout -b tournament/odi-wc-2027 main

# 2. Download the tournament data from Cricsheet
#    Place JSON files in visualization/streamlit/data/raw_json/

# 3. Compute KPIs
python3 scripts/compute_kpis_lightweight.py

# 4. Build player-team mapping
python3 visualization/streamlit/build_player_map.py

# 5. Update app.py sidebar title/branding for the tournament

# 6. Push and deploy to Streamlit Cloud
git add -A && git commit -m "Add ODI WC 2027 data" && git push -u origin tournament/odi-wc-2027
```

---

## License

This project uses publicly available cricket data from [Cricsheet](https://cricsheet.org)
(CC BY 4.0) and [CricAPI](https://cricapi.com) for live data.

