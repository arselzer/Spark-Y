#!/bin/bash
# Quick setup script for VLDB demo

set -e

echo "=============================================="
echo "VLDB Query Optimization Demo - Setup"
echo "=============================================="
echo ""

# Check prerequisites
echo "Checking prerequisites..."

# Check Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker not found. Please install Docker first."
    exit 1
fi
echo "✓ Docker found"

# Check Docker Compose
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose not found. Please install Docker Compose first."
    exit 1
fi
echo "✓ Docker Compose found"

# Check Git
if ! command -v git &> /dev/null; then
    echo "❌ Git not found. Please install Git first."
    exit 1
fi
echo "✓ Git found"

echo ""
echo "Prerequisites OK!"
echo ""

# Initialize submodules
echo "Initializing git submodules..."
git submodule update --init --recursive
echo "✓ Submodules initialized"
echo ""

# Create necessary directories
echo "Creating directories..."
mkdir -p spark data logs
echo "✓ Directories created"
echo ""

# Create .env file
if [ ! -f .env ]; then
    echo "Creating .env file from template..."
    cp .env.example .env
    echo "✓ .env file created"
else
    echo "✓ .env file already exists"
fi
echo ""

# Custom Spark JAR setup
echo "=============================================="
echo "Custom Spark JAR Setup"
echo "=============================================="
echo ""
echo "The demo requires a custom Spark JAR with optimization."
echo ""

if [ -f "spark/custom-spark-sql.jar" ]; then
    echo "✓ Custom Spark JAR found at spark/custom-spark-sql.jar"
else
    echo "⚠️  Custom Spark JAR not found"
    echo ""
    read -p "Do you want to create a placeholder for development? (y/n) " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        ./scripts/download_spark.sh --dev-placeholder
        echo ""
        echo "⚠️  Development placeholder created."
        echo "   For full functionality, replace with actual JAR (see README)."
    fi
fi
echo ""

# Ask about data setup
echo "=============================================="
echo "Data Setup"
echo "=============================================="
echo ""
read -p "Do you want to set up benchmark data now? (y/n) " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    ./scripts/prepare_data.sh
fi

echo ""
echo "=============================================="
echo "Setup Complete!"
echo "=============================================="
echo ""
echo "To start the demo:"
echo ""
echo "  Production mode (recommended):"
echo "    docker-compose up"
echo ""
echo "  Development mode (with hot reload):"
echo "    docker-compose -f docker-compose.yml -f docker-compose.dev.yml up"
echo ""
echo "Then open your browser:"
echo "  Application: http://localhost:3000 (production) or http://localhost:5173 (dev)"
echo "  API Docs:    http://localhost:8000/docs"
echo ""
echo "For more information, see README.md"
echo ""
