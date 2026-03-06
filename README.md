# 🏏 Cricket Analytics Platform

A **production-ready Cricket Analytics Platform** built in Scala (Maven) for macOS development.
Ingests live ball-by-ball cricket data from CricAPI (REST), streams it through Kafka, processes it
with Spark Structured Streaming, and stores it in a Delta Lake medallion architecture (Bronze → Silver → Gold),
with a Streamlit visualization layer.

---

## Architecture

```
CricAPI (REST Poll every 2s)
        ↓
CricApiPoller.scala (HTTP GET → JSON parse)
        ↓
KafkaProducer.scala → Kafka Topic: cricket-live-balls
        ↓
KafkaStreamReader.scala (Spark readStream from Kafka)
        ↓
BronzeStreamingJob.scala → Delta: /data/bronze/live_balls  (raw JSON)
        ↓  (watermark: 10 min, late data tolerated)
SilverStreamingJob.scala → Delta: /data/silver/live_balls  (flattened)
        ↓  (stateful aggregations: window, groupBy)
GoldStreamingKPIs.scala  → Delta: /data/gold/live_kpis     (aggregated KPIs)
        ↓
Streamlit Dashboard (reads Gold Delta via Python)
```

**Batch flow:**
```
Cricsheet (JSON files) → CricsheetIngestionJob → Bronze → Silver → GoldBatchKPIs (30+ SQL queries)
```

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

---

## Quick Start (macOS)

```bash
# 1. Clone the repository
git clone https://github.com/rajeshsantha/cricket-analytics-platform.git
cd cricket-analytics-platform

# 2. Run the automated macOS setup script
bash scripts/setup_mac.sh

# 3. Configure your API keys
cp .env.example .env
# Edit .env: set CRICAPI_KEY and CRICAPI_MATCH_ID
```

---

## 🔄 Data Refresh — When New Matches Finish

When a new cricket match finishes and Cricsheet publishes the data, use the refresh pipeline to update all statistics:

### Option A: One-command local refresh (recommended for dev)

```bash
# Dry run — see what new matches are available without changing anything
bash scripts/refresh_data.sh --dry-run

# Download new matches, reprocess pipeline, re-export Parquet
bash scripts/refresh_data.sh

# Download, reprocess, AND auto-commit + push for Streamlit Cloud deploy
bash scripts/refresh_data.sh --deploy
```

### Option B: Python-only refresh (no Spark required)

```bash
# Step 1: Detect & download new matches from Cricsheet
python3 scripts/refresh_data_ci.py

# Step 2: Recompute all 30 KPIs (pure Python/Pandas, ~3 seconds)
python3 scripts/compute_kpis_lightweight.py

# Step 3: Rebuild player-team mapping
python3 visualization/streamlit/build_player_map.py

# Step 4: Commit & push
git add visualization/streamlit/data/
git commit -m "Data refresh: +N new matches"
git push
```

### Option C: Automatic daily refresh (GitHub Actions)

A GitHub Actions workflow runs daily at **06:00 UTC** (11:30 AM IST — after day matches finish):
1. Downloads latest Cricsheet data
2. Detects new 2026 T20 World Cup matches
3. Recomputes all 30 KPIs (Python-only, no Spark)
4. Commits updated Parquet files back to the branch
5. Streamlit Cloud auto-deploys on push

To trigger manually: **Actions → 🏏 Refresh T20 WC Data → Run workflow**

### How the Pipeline Handles Incremental Data

| Layer | Strategy | Why |
|-------|----------|-----|
| **Raw JSON** | Append-only (`data/raw_json/`) | New match files are added; existing files never change |
| **KPI Parquet** | Full recompute | All 30 KPIs are recomputed from all matches (~3s in Python) |
| **Player Map** | Full rebuild | Ensures new players from new matches get team mappings |
| **Delta Lake** (local only) | Drop & recreate | Spark pipeline clears Delta, reprocesses all matches |

---

## Step-by-Step Setup

### Step 1: Run Batch Pipeline (Cricsheet data)

```bash
# Download Cricsheet data from https://cricsheet.org/downloads/
# Extract JSON files to /tmp/cricsheet-data/

bash scripts/run_batch.sh /tmp/cricsheet-data
```

This runs:
- `CricsheetIngestionJob` → reads JSON files
- `BronzeBatchJob` → writes to Bronze Delta
- `SilverBatchJob` → flattens and cleans data
- `GoldBatchKPIs` → computes 30+ KPI queries

### Step 2: Run Streaming Pipeline (Live CricAPI data)

```bash
# Ensure .env has CRICAPI_KEY and CRICAPI_MATCH_ID set
bash scripts/run_streaming.sh
```

This runs (in parallel threads):
1. `BronzeStreamingJob` → Kafka → Bronze Delta
2. `SilverStreamingJob` → Bronze → Silver Delta (with 10-min watermark)
3. `GoldStreamingKPIs` → Silver → Gold Delta (5-min sliding window)

### Step 3: Run CricAPI Poller (separate terminal)

```bash
spark-submit --class com.rajesh.cricket.Main \
  target/cricket-analytics-platform-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  --mode poller
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

### Config Files

| File                   | Purpose                                  |
|------------------------|------------------------------------------|
| `conf/application.conf`| Main app config (includes others)        |
| `conf/kafka.conf`      | Kafka bootstrap servers and topics       |
| `conf/delta.conf`      | Delta Lake table paths                   |
| `conf/cricapi.conf`    | CricAPI URL, key, poll interval          |

---

## Project Structure

```
cricket-analytics-platform/
├── pom.xml                          # Maven build (Scala 2.13, Spark 4.1.1)
├── .env.example                     # Environment variable template
├── conf/                            # Typesafe Config files
├── docker/                          # Docker Compose for Kafka + Kafka UI
├── scripts/                         # Shell scripts for setup and running
├── src/
│   ├── main/scala/com/rajesh/cricket/
│   │   ├── config/                  # AppConfig, SparkSessionFactory, CricApiConfig
│   │   ├── model/                   # Case classes (Match, Delivery, LiveBall, etc.)
│   │   ├── ingestion/               # CricApiPoller, KafkaProducer, CricsheetJob
│   │   ├── bronze/                  # Bronze batch and streaming jobs
│   │   ├── silver/                  # Silver batch and streaming jobs
│   │   ├── gold/                    # Gold KPI batch and streaming jobs
│   │   ├── analytics/               # WindowFunctions, PressureIndex, TossImpact
│   │   ├── utils/                   # SchemaUtils, DeltaUtils, KafkaUtils, HttpUtils
│   │   └── Main.scala               # Single entry point with mode dispatch
│   └── test/scala/                  # ScalaTest unit tests
├── notebooks/                       # Spark notebooks and SQL queries
└── visualization/
    ├── streamlit/                   # Python Streamlit dashboard
    └── powerbi/                     # Power BI integration guide
```

---

## Running Each Mode

```bash
# Batch pipeline (Cricsheet ingestion → Bronze → Silver → Gold)
spark-submit ... Main --mode batch --data-path /path/to/cricsheet

# Streaming pipeline (Kafka → Bronze → Silver → Gold)
spark-submit ... Main --mode streaming

# CricAPI poller only (publishes to Kafka)
spark-submit ... Main --mode poller

# Gold KPI batch computation only
spark-submit ... Main --mode gold
```

---

## Kafka UI

After running `docker-compose up`, open: **http://localhost:8080**

You can:
- Monitor topic offsets and consumer groups
- Browse messages in each topic
- Inspect partition distribution

Kafka topics created:
- `cricket-live-balls` — Ball-by-ball events (3 partitions)
- `cricket-live-matches` — Match metadata (3 partitions)
- `cricket-batch` — Batch processing events (3 partitions)

---

## Streamlit Dashboard

### Live demo: [cricket-insights.streamlit.app](https://cricket-insights.streamlit.app)

### Run locally
```bash
cd visualization/streamlit
pip install -r requirements.txt
streamlit run app.py
```

Opens at **http://localhost:8501** with 7 interactive pages:

| Page | KPIs | Charts |
|------|------|--------|
| 🏠 **Overview** | 1, 2, 8, 9, 17, 24 | Headline metrics, bar charts, run-rate line |
| 🏏 **Batting** | 1, 3, 5, 7, 8, 9, 18, 19, 28, 30 | Bar charts, scatter plots, tables (9 tabs) |
| 🎳 **Bowling** | 2, 4, 6, 11, 20, 29 | Horizontal bars, sortable tables (6 tabs) |
| 👥 **Team Analytics** | 10, 15, 16, 17, 22, 25, 27 | Grouped bars, stacked bars, head-to-head (7 tabs) |
| 🏟️ **Venue & Toss** | 12, 13, 14, 26 | Grouped bars, pie chart, venue ranking (4 tabs) |
| 📈 **Match Trends** | 21, 23, 24 | Phase analysis, pressure index, run-rate area (3 tabs) |
| 📊 **Live Scorecard** | Streaming KPIs | Real-time metrics with auto-refresh |

### Deploy to Streamlit Cloud
The dashboard ships pre-computed Gold KPI data as Parquet files in `visualization/streamlit/data/`.
This means it works on **Streamlit Cloud** without Spark, Kafka, or Delta Lake installed.

To refresh the bundled data after re-running the batch pipeline:
```bash
cd visualization/streamlit
python export_data.py      # exports Delta tables → data/*.parquet
git add data/
git commit -m "refresh KPI data"
git push
```

---

## Running Tests

```bash
mvn test
```

Test classes:
- `BronzeJobSpec` — Tests BronzeBatchJob file reading and Delta writing
- `SilverJobSpec` — Tests data quality filters and null handling
- `GoldKPISpec` — Tests 5 KPI SQL queries against in-memory data
- `CricApiPollerSpec` — Tests JSON parsing from mock CricAPI responses

---

## Gold KPIs (30+ queries)

The `GoldBatchKPIs` job computes:

| # | KPI |
|---|-----|
| 1 | Top 10 run scorers all time |
| 2 | Top 10 wicket takers |
| 3 | Best batting average (min 20 innings) |
| 4 | Best bowling average (min 50 wickets) |
| 5 | Best strike rate (min 500 balls) |
| 6 | Best economy rate (min 100 overs) |
| 7 | Highest individual scores |
| 8 | Most sixes hit |
| 9 | Most fours hit |
| 10 | Best powerplay run rate by team |
| 11 | Best death over economy |
| 12 | Win % by toss decision |
| 13 | Win % batting first vs chasing |
| 14 | Average score by venue |
| 15 | Highest team totals |
| 16 | Lowest successful chases |
| 17 | Most matches won by team |
| 18 | Player performance by match type |
| 19 | Partnership analysis |
| 20 | Dot ball % by bowler |
| 21 | Boundary % per over phase |
| 22 | Average runs per wicket by innings |
| 23 | Pressure index per over |
| 24 | Run rate progression over by over |
| 25 | Extras analysis by team |
| 26 | Home vs away win percentage |
| 27 | Head-to-head team records |
| 28 | Player consistency (std deviation) |
| 29 | Best bowling spells (5-over window) |
| 30 | Match-winning contribution index |

---

## Tech Stack

| Component      | Technology                    |
|----------------|-------------------------------|
| Language       | Scala 2.13                    |
| Build          | Maven 3.8+                    |
| Processing     | Apache Spark 4.1.1            |
| Streaming      | Spark Structured Streaming    |
| Message Queue  | Apache Kafka 4.2.0            |
| Storage        | Delta Lake 4.1.0                |
| HTTP Client    | sttp 3.9.0                    |
| JSON           | Circe 0.14.6                  |
| Config         | Typesafe Config 1.4.2         |
| Testing        | ScalaTest 3.2.17              |
| Dashboard      | Streamlit 1.32.0              |
| Visualization  | Plotly 5.18.0                 |