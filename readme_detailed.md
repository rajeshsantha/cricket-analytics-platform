# 🏏 Cricket Analytics Platform

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/build-manual-lightgrey.svg)]()
[![Streamlit](https://img.shields.io/badge/visualization-streamlit-orange.svg)]()
[![Spark](https://img.shields.io/badge/processing-spark-blue.svg)]()

A production-ready Cricket Analytics Platform implemented in Scala + Spark with a Streamlit-based visualization layer. The platform ingests live ball-by-ball data from CricAPI (polling), streams data through Kafka, processes it using Spark Structured Streaming, and stores data in Delta Lake using a medallion architecture (Bronze → Silver → Gold). It also supports batch ingestion of Cricsheet historical match JSON files and computes 30+ Gold KPIs for analysis.

Maintainer: rajeshsantha
Repository: rajeshsantha/cricket-analytics-platform

---

Table of contents
- What this project is
- Highlights & Features
- Architecture (diagram + flow)
- Tech stack & versions
- Quick start (macOS)
- Docker & local orchestration
- Configuration (env + conf)
- Running modes (batch / streaming / poller / gold)
- Streamlit dashboard
- Project structure (key files)
- Testing & CI
- Gold KPIs overview
- Development notes & contribution guide
- Troubleshooting & FAQ
- Roadmap & ideas

---

What this project is
--------------------
This project is an end-to-end analytics platform for cricket match data. It supports:
- Live ingestion of ball-by-ball events (CricAPI poller → Kafka).
- Stream processing with Spark Structured Streaming (Bronze → Silver → Gold).
- Batch ingestion of historical Cricsheet JSON files.
- Delta Lake medallion architecture for reliable storage and incremental reprocessing.
- Streamlit dashboard for live and batch visualizations (live scorecard, player stats, pressure charts).
- A set of Gold KPI jobs (30+ analytics queries) useful for dashboards and reporting.

Highlights & Features
---------------------
- Robust streaming + batch architecture (single codebase).
- Delta Lake medallion pattern (Bronze / Silver / Gold).
- Kafka-based decoupling for live streams.
- Streamlit visualization with modular components.
- Pre-built KPIs: run rates, bowling economy, pressure index, partnerships, and more.
- Unit tests (ScalaTest) covering key jobs and KPIs.

Architecture (high-level flow)
------------------------------

CricAPI (REST Poll every ~2s)
↓
CricApiPoller.scala (HTTP GET → JSON parse)  →  KafkaProducer → Kafka topic: cricket-live-balls
↓
KafkaStreamReader.scala (Spark readStream from Kafka)
↓
BronzeStreamingJob.scala → Delta: /data/bronze/live_balls  (raw JSON)
↓  (watermark: 10 min)
SilverStreamingJob.scala → Delta: /data/silver/live_balls  (flattened, typed)
↓  (stateful aggregations: window, groupBy)
GoldStreamingKPIs.scala  → Delta: /data/gold/live_kpis     (aggregated KPIs)
↓
Streamlit Dashboard (reads Gold Delta via Python)

Batch flow:
Cricsheet (JSON files) → CricsheetIngestionJob → Bronze → Silver → GoldBatchKPIs (30+ SQL queries)

Tech stack
----------
| Component      | Technology / Notes |
|----------------|--------------------|
| Language       | Scala (project metadata indicates Scala 2.13 — confirm in pom.xml) |
| Build          | Maven 3.8+ |
| Processing     | Apache Spark (Structured Streaming) |
| Streaming      | Spark Structured Streaming |
| Message Queue  | Apache Kafka |
| Storage        | Delta Lake |
| HTTP Client    | sttp |
| JSON           | circe |
| Config         | Typesafe Config |
| Testing        | ScalaTest |
| Visualization  | Streamlit + Plotly (Python) |

Quick Start (macOS)
-------------------
1. Clone the repository:
```bash
git clone https://github.com/rajeshsantha/cricket-analytics-platform.git
cd cricket-analytics-platform
```

2. Copy environment template:
```bash
cp .env.example .env
# Edit .env: set CRICAPI_KEY and CRICAPI_MATCH_ID (and optionally KAFKA_BOOTSTRAP_SERVERS, DELTA_BASE_PATH)
```

3. Run the macOS setup script (installs prerequisites & builds):
```bash
bash scripts/setup_mac.sh
```

4. Run a batch job (Cricsheet historical data):
```bash
# download and extract Cricsheet JSON files to /tmp/cricsheet-data
bash scripts/run_batch.sh /tmp/cricsheet-data
```

5. Run streaming jobs:
```bash
bash scripts/run_streaming.sh
```

6. Run the CricAPI poller (publishes to Kafka):
```bash
spark-submit --class com.rajesh.cricket.Main \
  target/cricket-analytics-platform-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  --mode poller
```

7. Start the Streamlit dashboard:
```bash
cd visualization/streamlit
pip install -r requirements.txt
streamlit run app.py
# Open http://localhost:8501
```

Docker & local orchestration
----------------------------
- The repository contains a docker/ directory with a docker-compose configuration to bring up Kafka + Kafka UI.
- Run:
```bash
cd docker
docker-compose up -d
# Open Kafka UI: http://localhost:8080
```
Kafka topics created by the platform:
- cricket-live-balls
- cricket-live-matches
- cricket-batch

Configuration
-------------
Environment variables (.env sample)
```
CRICAPI_KEY=your_api_key_here
CRICAPI_MATCH_ID=your_match_id_here
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
DELTA_BASE_PATH=/tmp/cricket-delta
SPARK_MASTER=local[*]
```

Configuration files (conf/)
- conf/application.conf — main app configuration
- conf/kafka.conf — kafka connection & topic names
- conf/delta.conf — delta table base paths
- conf/cricapi.conf — cricapi url, poll interval

Running modes (Main.scala usage)
-------------------------------
Main supports multiple modes:
- batch -> Cricsheet ingestion and Gold KPI batch jobs
  Example:
  ```bash
  spark-submit ... Main --mode batch --data-path /path/to/cricsheet
  ```
- streaming -> run Bronze/Silver/Gold streaming flows reading from Kafka
  ```bash
  spark-submit ... Main --mode streaming
  ```
- poller -> CricAPI poller that writes to Kafka topics
  ```bash
  spark-submit ... Main --mode poller
  ```
- gold -> run only Gold KPI computation (batch)

Scripts
-------
- scripts/setup_mac.sh — macOS helper script; installs tools & builds jar
- scripts/run_batch.sh — wrapper to run batch ingestion and KPIs
- scripts/run_streaming.sh — wrapper to start streaming jobs
  (See respective scripts for options and environment usage.)

Streamlit Dashboard
-------------------
The Streamlit dashboard reads Gold Delta tables and provides 3 primary tabs:
- Live Scorecard — live match state (auto-refresh every 5s).
- Player Stats — top batsmen/bowlers from Gold KPIs.
- Pressure Chart — over-by-over pressure index visualization.

Start the dashboard:
```bash
cd visualization/streamlit
pip install -r requirements.txt
streamlit run app.py
```

Key code & components
---------------------
- src/main/scala/com/rajesh/cricket/Main.scala — program entrypoint and mode dispatch
- src/main/scala/com/rajesh/cricket/ingestion — CricAPI poller, Kafka producer, Cricsheet reader
- src/main/scala/com/rajesh/cricket/bronze — Bronze batch & streaming jobs
- src/main/scala/com/rajesh/cricket/silver — Silver transformations & aggregations
- src/main/scala/com/rajesh/cricket/gold — Gold KPI queries & streaming KPI jobs
- visualization/streamlit — Streamlit app & components (player_stats, live_scorecard, pressure_chart)
- conf/ — Typesafe config files
- scripts/ — helper scripts (setup_mac.sh, run_batch.sh, run_streaming.sh)
- docker/ — docker-compose for Kafka + UI

Testing & CI
------------
Run Scala unit tests:
```bash
mvn test
```
Test suites include:
- BronzeJobSpec — Bronze batch I/O and Delta writes
- SilverJobSpec — data quality & null handling
- GoldKPISpec — KPI SQL queries against in-memory Spark
- CricApiPollerSpec — JSON parsing tests with mock API responses

Gold KPIs (summary)
-------------------
GoldBatchKPIs computes 30+ analytics including:
- Top run scorers, wicket takers, batting averages, bowling averages
- Strike rates, economy rates, highest scores
- Powerplay and death overs KPIs
- Home vs away stats, head-to-head, consistency, match-winning contribution index
- Pressure index per over, run rate progression, partnerships

Contributing & development notes
--------------------------------
- Fork the repo and submit PRs to the main branch.
- Follow Scala code style and include unit tests for logic changes.
- Add a changelog entry for user-facing changes.
- Suggested development workflow:
    1. Create feature branch: git checkout -b feat/awesome-kpi
    2. Implement & add tests
    3. mvn -DskipTests=false test
    4. Build: mvn clean package -DskipTests
    5. Submit PR and link issue

Helpful developer tips
- Local delta path default: /tmp/cricket-delta (change DELTA_BASE_PATH to isolate local runs).
- Use SPARK_MASTER=local[*] for development.
- To run just one streaming stage, call the specific job from Main with appropriate flags.
- Check pom.xml for exact Scala and dependency versions before modifying libs.

Troubleshooting & FAQ
---------------------
- "Kafka connection refused" — ensure docker-compose is up or KAFKA_BOOTSTRAP_SERVERS points to a running Kafka.
- "Delta table path not found" — create the base folder or set DELTA_BASE_PATH and ensure Spark has write permissions.
- "CricAPI auth error" — verify CRICAPI_KEY in .env and confirm the API expects your account.
- "Tests failing due to networking" — run tests in offline mode or mock external calls.

Roadmap & ideas
---------------
- Add CI (GitHub Actions) to run tests and build artifact.
- Add Docker images for the Spark job runner and streamlit UI for easier local dev.
- Add cloud deployment (EMR/Dataproc + MSK or Confluent Cloud).
- Add more KPIs and automated anomaly detection on KPIs (alerts).
- Add TV-ready dashboard and scheduled report generation.

License
-------
This project is provided under the MIT License. See LICENSE for details.

Acknowledgements
----------------
- Cricsheet.org for historical match dataset structure and inspiration.
- Open-source libs: Apache Spark, Delta Lake, Kafka, Streamlit, Plotly.

Contact / Maintainer
--------------------
rajeshsantha — feel free to open issues, feature requests, or PRs.
