#!/bin/bash
# Setup script for macOS development environment (Docker-based)
# Only installs Java + Maven on the host; everything else runs in Docker

set -e

echo "=== Cricket Analytics Platform - macOS Setup (Docker-based) ==="

# Only install true host dependencies: Java (to run the fat JAR) + Maven (to build it)
echo "Installing host dependencies via Homebrew..."
brew install openjdk@11 maven

# Set JAVA_HOME for Java 11
export JAVA_HOME=$(/usr/libexec/java_home -v 11)
echo 'export JAVA_HOME=$(/usr/libexec/java_home -v 11)' >> ~/.zshrc

java -version
mvn -version

# Install Docker Desktop if not present
if ! command -v docker &> /dev/null; then
  echo "Installing Docker Desktop..."
  brew install --cask docker
  echo "Docker installed. Please start Docker Desktop manually and re-run this script."
  open -a Docker
  exit 0
fi

# Wait for Docker daemon to be ready
echo "Waiting for Docker daemon..."
until docker info > /dev/null 2>&1; do
  echo "  Docker not ready yet, waiting 5s..."
  sleep 5
done
echo "Docker is ready."

# Start all services via Docker Compose (Kafka + Zookeeper + Kafka UI)
echo "Starting Kafka services via Docker Compose..."
cd "$(dirname "$0")/../docker"
docker compose up -d

echo "Waiting for Kafka to be ready..."
sleep 3

# Create Kafka topics (runs inside the Kafka container — no host kafka needed)
echo "Creating Kafka topics..."
cd ..
bash scripts/create_kafka_topics.sh

# Copy environment file
if [ ! -f .env ]; then
  cp .env.example .env
  echo "Created .env file — please update with your CRICAPI_KEY"
fi

# Build the Maven project (produces fat JAR)
echo "Building Maven project..."
mvn clean package -DskipTests

echo ""
echo "=== Setup complete! ==="
echo ""
echo "All services running in Docker:"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo ""
echo "Next steps:"
echo "  1. Edit .env → add CRICAPI_KEY and CRICAPI_MATCH_ID"
echo "  2. Run batch:     bash scripts/run_batch.sh /path/to/cricsheet/data"
echo "  3. Run streaming: bash scripts/run_streaming.sh"
echo "  4. Kafka UI:      http://localhost:8080"
echo "  5. Spark UI:      http://localhost:8090 (if spark-master service is running)"
echo "  6. Streamlit:     cd visualization/streamlit && pip install -r requirements.txt && streamlit run app.py"
