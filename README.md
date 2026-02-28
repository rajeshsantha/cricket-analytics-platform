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

## Prerequisites (macOS)

Only 3 things needed on your host machine:
1. **Docker Desktop** — runs Kafka, Zookeeper, Kafka UI (everything except the JVM)
2. **Java 11** — `brew install openjdk@11`
3. **Maven** — `brew install maven` (to build the fat JAR)

That's it. No Kafka, no Spark, no Scala installation needed on your Mac; everything else runs inside Docker containers.

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
java -cp target/cricket-analytics-platform-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  com.rajesh.cricket.Main --mode poller
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
├── pom.xml                          # Maven build (Scala 2.12, Spark 3.4.1)
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
java -cp target/cricket-analytics-platform-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  com.rajesh.cricket.Main --mode batch --data-path /path/to/cricsheet

# Streaming pipeline (Kafka → Bronze → Silver → Gold)
java -cp target/cricket-analytics-platform-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  com.rajesh.cricket.Main --mode streaming

# CricAPI poller only (publishes to Kafka)
java -cp target/cricket-analytics-platform-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  com.rajesh.cricket.Main --mode poller

# Gold KPI batch computation only
java -cp target/cricket-analytics-platform-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  com.rajesh.cricket.Main --mode gold
```

---

## Kafka UI

After running `docker compose up -d`, open: **http://localhost:8080**

You can:
- Monitor topic offsets and consumer groups
- Browse messages in each topic
- Inspect partition distribution

Kafka topics created:
- `cricket-live-balls` — Ball-by-ball events (3 partitions)
- `cricket-live-matches` — Match metadata (3 partitions)
- `cricket-batch` — Batch processing events (3 partitions)

## Spark UI

The optional Spark master container exposes its UI at **http://localhost:8090**.

---

## Streamlit Dashboard

```bash
cd visualization/streamlit
pip install -r requirements.txt
streamlit run app.py
```

Opens at **http://localhost:8501** with three tabs:
- **Live Scorecard** — Current run rate, wickets, batsman/bowler (auto-refreshes every 5s)
- **Player Stats** — Top batsmen and bowlers from Gold batch KPIs
- **Pressure Chart** — Over-by-over pressure index, color-coded by level

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
| Language       | Scala 2.12                    |
| Build          | Maven 3.8+                    |
| Processing     | Apache Spark 3.4.1            |
| Streaming      | Spark Structured Streaming    |
| Message Queue  | Apache Kafka 7.5.0 (Confluent)|
| Storage        | Delta Lake 2.4.0              |
| HTTP Client    | sttp 3.9.0                    |
| JSON           | Circe 0.14.6                  |
| Config         | Typesafe Config 1.4.2         |
| Testing        | ScalaTest 3.2.17              |
| Dashboard      | Streamlit 1.32.0              |
| Visualization  | Plotly 5.18.0                 |