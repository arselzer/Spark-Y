#!/bin/bash
# Download custom PySpark package with optimization

set -e

echo "Downloading custom PySpark package..."

# Create spark directory if it doesn't exist
mkdir -p pyspark

# TODO: Replace with actual release URL once JAR is published
# For now, this is a placeholder
SPARK_JAR_URL="https://github.com/arselzer/spark/releases/download/v4/pyspark-3.5.0.dev0.tar.gz"

# Check if JAR already exists
if [ -f "pyspark/pyspark-3.5.0.dev0.tar.gz" ]; then
    echo "Custom package already exists at pyspark/pyspark-3.5.0.dev0.tar.gz"
    read -p "Do you want to re-download it? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Skipping download."
        exit 0
    fi
fi

# Download the package
echo "Downloading from: $SPARK_JAR_URL"
curl -L -o "pyspark/pyspark-3.5.0.dev0.tar.gz" "$SPARK_JAR_URL"
