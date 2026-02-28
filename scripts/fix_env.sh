#!/bin/bash
# ============================================================
# Cricket Analytics Platform — Environment Fix Script
# Fixes: stale SPARK_HOME, broken JAVA_HOME, Java 8 fallback
# ============================================================

echo ""
echo "=== Step 1: Removing ALL stale Spark/Java entries from ~/.zshrc ==="
# Remove any lines referencing old spark351, Cellar paths, or old SPARK_HOME
sed -i '' '/spark351/d' ~/.zshrc
sed -i '' '/SPARK_HOME/d' ~/.zshrc
sed -i '' '/JAVA_HOME/d' ~/.zshrc
sed -i '' '/openjdk@11\/bin/d' ~/.zshrc
sed -i '' '/Cellar\/openjdk/d' ~/.zshrc
echo "Done — stale entries removed."

echo ""
echo "=== Step 2: Writing clean Spark 4.1.1 + Java 11 config to ~/.zshrc ==="
cat >> ~/.zshrc << 'EOF'

# ── Cricket Analytics Platform ──────────────────────────────
# Java 11 (opt/ symlink — stable across brew upgrades)
export JAVA_HOME=/opt/homebrew/opt/openjdk@11
export PATH="$JAVA_HOME/bin:$PATH"

# Spark 4.1.1 (brew-managed, opt/ symlink — stable)
export SPARK_HOME=/opt/homebrew/opt/apache-spark
export PATH="$SPARK_HOME/bin:$PATH"
# ────────────────────────────────────────────────────────────
EOF
echo "Done — clean config written."

echo ""
echo "=== Step 3: Linking apache-spark (in case it is unlinked) ==="
brew link --overwrite apache-spark 2>/dev/null || true

echo ""
echo "=== Step 4: Reloading shell ==="
source ~/.zshrc

echo ""
echo "=== Verification ==="
echo -n "JAVA_HOME  : "; echo $JAVA_HOME
echo -n "SPARK_HOME : "; echo $SPARK_HOME
echo -n "which java : "; which java
echo -n "which spark-submit : "; which spark-submit
echo ""
java -version
echo ""
spark-submit --version 2>&1 | head -3
echo ""
echo "=== Done! Environment is clean. ==="