#!/bin/bash
set -e

echo "=== Cricket Analytics Platform - macOS Setup ==="

# ── Step 1: Host dependencies (Java + Maven only) ──────────────────────────
echo ""
echo "[1/5] Installing Java 11 and Maven via Homebrew..."
brew install openjdk@11 maven

# Set JAVA_HOME
export JAVA_HOME=$(/usr/libexec/java_home -v 11)
grep -qxF 'export JAVA_HOME=$(/usr/libexec/java_home -v 11)' ~/.zshrc \
  || echo 'export JAVA_HOME=$(/usr/libexec/java_home -v 11)' >> ~/.zshrc

java -version
mvn -version

# ── Step 2: Download Spark 3.4.1 (exact version, no brew) ──────────────────
echo ""
echo "[2/5] Downloading Apache Spark 3.4.1..."
SPARK_VERSION="3.4.1"
SPARK_DIR="$(dirname "$0")/../spark"
SPARK_TARBALL="spark-${SPARK_VERSION}-bin-hadoop3.tgz"
SPARK_DOWNLOAD_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_TARBALL}"

if [ ! -d "$SPARK_DIR/spark-${SPARK_VERSION}-bin-hadoop3" ]; then
  mkdir -p "$SPARK_DIR"
  echo "Downloading Spark from $SPARK_DOWNLOAD_URL ..."
  curl -L "$SPARK_DOWNLOAD_URL" -o "$SPARK_DIR/$SPARK_TARBALL"
  tar -xzf "$SPARK_DIR/$SPARK_TARBALL" -C "$SPARK_DIR"
  rm "$SPARK_DIR/$SPARK_TARBALL"
  echo "Spark 3.4.1 downloaded to $SPARK_DIR"
else
  echo "Spark 3.4.1 already downloaded, skipping."
fi

# Create a stable symlink: spark/current -> spark/spark-3.4.1-bin-hadoop3
ln -sfn "$(cd "$SPARK_DIR/spark-${SPARK_VERSION}-bin-hadoop3" && pwd)" "$SPARK_DIR/current"
export SPARK_HOME="$SPARK_DIR/current"
grep -qxF "export SPARK_HOME=$(realpath "$SPARK_DIR")/current" ~/.zshrc \
  || echo "export SPARK_HOME=$(realpath "$SPARK_DIR")/current" >> ~/.zshrc

echo "SPARK_HOME=$SPARK_HOME"
"$SPARK_HOME/bin/spark-submit" --version

# ── Step 3: Docker - start Kafka services ──────────────────────────────────
echo ""
echo "[3/5] Starting Kafka services via Docker Compose..."

if ! command -v docker &> /dev/null; then
  echo "Docker not found. Installing Docker Desktop..."
  brew install --cask docker
  open -a Docker
  echo "Please wait for Docker Desktop to start, then re-run this script."
  exit 0
fi

until docker info > /dev/null 2>&1; do
  echo "  Docker not ready yet, waiting 5s..."
  sleep 5
done
echo "Docker is ready."

cd "$(dirname "$0")/../docker"
docker compose up -d
echo "Waiting 15s for Kafka to initialise..."
sleep 15

# ── Step 4: Create Kafka topics (inside Docker container) ──────────────────
echo ""
echo "[4/5] Creating Kafka topics..."
cd ..
bash scripts/create_kafka_topics.sh

# ── Step 5: Build the Maven project ────────────────────────────────────────
echo ""
echo "[5/5] Building Maven project (fat JAR)..."

if [ ! -f .env ]; then
  cp .env.example .env
  echo "Created .env — please add your CRICAPI_KEY before running streaming."
fi

mvn clean package -DskipTests

echo ""
echo "======================================================"
echo "  Setup complete!"
echo "======================================================"
echo ""
echo "Docker containers running:"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo ""
echo "SPARK_HOME : $SPARK_HOME"
echo ""
echo "Next steps:"
echo "  1. Edit .env  →  add CRICAPI_KEY and CRICAPI_MATCH_ID"
echo "  2. Batch job  →  bash scripts/run_batch.sh /path/to/cricsheet/data"
echo "  3. Streaming  →  bash scripts/run_streaming.sh"
echo "  4. Kafka UI   →  http://localhost:8080"
echo "  5. Streamlit  →  cd visualization/streamlit && pip install -r requirements.txt && streamlit run app.py"
