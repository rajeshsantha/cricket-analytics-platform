#!/bin/bash
# Setup script for macOS development environment
# Installs all required dependencies via Homebrew and starts services

set -e

echo "=== Cricket Analytics Platform - macOS Setup ==="

# Install Homebrew dependencies
echo "Installing dependencies via Homebrew..."
brew install openjdk@11 scala apache-spark kafka
brew install --cask docker

# Set JAVA_HOME for Java 11
export JAVA_HOME=$(/usr/libexec/java_home -v 11)
echo "export JAVA_HOME=\$(/usr/libexec/java_home -v 11)" >> ~/.zshrc

# Verify Java version
java -version

# Start Docker Desktop
echo "Starting Docker..."
open -a Docker
sleep 15

# Start Kafka and Zookeeper via Docker Compose
echo "Starting Kafka services..."
cd "$(dirname "$0")/../docker"
docker-compose up -d
sleep 10

# Create Kafka topics
echo "Creating Kafka topics..."
cd ..
bash scripts/create_kafka_topics.sh

# Copy environment file
if [ ! -f .env ]; then
  cp .env.example .env
  echo "Created .env file - please update with your API keys"
fi

# Build Maven project
echo "Building Maven project..."
mvn clean package -DskipTests

echo ""
echo "=== Setup complete! ==="
echo ""
echo "Next steps:"
echo "  1. Edit .env and add your CRICAPI_KEY and CRICAPI_MATCH_ID"
echo "  2. Run batch: bash scripts/run_batch.sh /path/to/cricsheet/data"
echo "  3. Run streaming: bash scripts/run_streaming.sh"
echo "  4. Open Kafka UI: http://localhost:8080"
echo "  5. Run Streamlit: cd visualization/streamlit && pip install -r requirements.txt && streamlit run app.py"
