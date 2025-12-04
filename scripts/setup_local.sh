#!/bin/bash

# AML Transaction Monitoring - Local Setup Script
# This script sets up the local development environment

set -e

echo "=============================================="
echo "AML Transaction Monitoring - Local Setup"
echo "=============================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Docker is not installed. Please install Docker first.${NC}"
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}Docker Compose is not installed. Please install Docker Compose first.${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Docker and Docker Compose are installed${NC}"

# Create necessary directories
echo ""
echo "Creating project directories..."
mkdir -p data/sample
mkdir -p data/raw
mkdir -p data/processed
mkdir -p logs
mkdir -p checkpoints

echo -e "${GREEN}✓ Directories created${NC}"

# Install Python dependencies
echo ""
echo "Installing Python dependencies..."
if command -v python3 &> /dev/null; then
    python3 -m pip install -r requirements.txt
    echo -e "${GREEN}✓ Python dependencies installed${NC}"
else
    echo -e "${YELLOW}⚠ Python 3 not found. Skipping Python dependencies installation.${NC}"
fi

# Start Docker containers
echo ""
echo "Starting Docker containers..."
cd infrastructure/docker
docker-compose up -d

echo ""
echo "Waiting for services to start (30 seconds)..."
sleep 30

# Check service health
echo ""
echo "Checking service health..."

# Check Kafka
if docker-compose ps | grep -q "aml-kafka.*Up"; then
    echo -e "${GREEN}✓ Kafka is running${NC}"
else
    echo -e "${RED}✗ Kafka failed to start${NC}"
fi

# Check Spark
if docker-compose ps | grep -q "aml-spark-master.*Up"; then
    echo -e "${GREEN}✓ Spark Master is running${NC}"
else
    echo -e "${RED}✗ Spark Master failed to start${NC}"
fi

# Check Airflow
if docker-compose ps | grep -q "aml-airflow-webserver.*Up"; then
    echo -e "${GREEN}✓ Airflow is running${NC}"
else
    echo -e "${RED}✗ Airflow failed to start${NC}"
fi

cd ../..

# Create Kafka topics
echo ""
echo "Creating Kafka topics..."
docker exec aml-kafka kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic transactions \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists

docker exec aml-kafka kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic alerts \
    --partitions 1 \
    --replication-factor 1 \
    --if-not-exists

echo -e "${GREEN}✓ Kafka topics created${NC}"

# Generate sample data
echo ""
echo "Generating sample transaction data..."
python3 src/data_generator/transaction_producer.py \
    --mode batch \
    --output-file data/sample/transactions.json \
    --num-transactions 1000 \
    --suspicious-rate 0.02

echo -e "${GREEN}✓ Sample data generated${NC}"

# Print access information
echo ""
echo "=============================================="
echo -e "${GREEN}Setup Complete!${NC}"
echo "=============================================="
echo ""
echo "Service URLs:"
echo "  • Kafka:              localhost:9092"
echo "  • Spark Master UI:    http://localhost:8080"
echo "  • Spark Worker UI:    http://localhost:8081"
echo "  • Airflow UI:         http://localhost:8082"
echo "    (username: airflow, password: airflow)"
echo ""
echo "Data directories:"
echo "  • Sample data:        ./data/sample/"
echo "  • Raw data:           ./data/raw/"
echo "  • Processed data:     ./data/processed/"
echo ""
echo "Next steps:"
echo "  1. Start transaction producer:"
echo "     python3 src/data_generator/transaction_producer.py --mode stream --tps 10"
echo ""
echo "  2. Submit Spark streaming job:"
echo "     docker exec aml-spark-master spark-submit \\"
echo "       --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-core_2.12:3.0.0 \\"
echo "       /opt/spark/work-dir/src/streaming/transaction_processor.py"
echo ""
echo "  3. Access Airflow UI and enable DAGs"
echo ""
echo "To stop all services:"
echo "  cd infrastructure/docker && docker-compose down"
echo ""
echo "=============================================="
