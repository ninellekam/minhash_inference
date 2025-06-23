#!/bin/bash
set -e

echo "🚀 Starting MinHash Inference Deployment..."

# Загрузить переменные окружения
if [ -f .env ]; then
    export $(cat .env | grep -v '#' | xargs)
else
    echo "❌ .env file not found! Copy .env.example to .env and configure it."
    exit 1
fi

echo "📦 Step 1: Creating directories..."
mkdir -p data/minhash_index
mkdir -p data/elasticsearch_data
mkdir -p logs

echo "🔽 Step 2: Downloading data from S3..."
python scripts/download_data.py

echo "🐳 Step 3: Starting services..."
docker-compose down --remove-orphans
docker-compose up -d elasticsearch
echo "⏳ Waiting for Elasticsearch to start..."
sleep 30

echo "📊 Step 4: Setting up Elasticsearch index..."
python scripts/setup_elasticsearch.py

echo "🏗️ Step 5: Building and starting MinHash server..."
docker-compose up -d --build minhash-server

echo "📈 Step 6: Starting monitoring (optional)..."
if [ "$ENABLE_MONITORING" = "true" ]; then
    docker-compose -f monitoring/docker-compose.monitoring.yml up -d
fi

echo "🔍 Step 7: Health check..."
python scripts/health_check.py

echo "✅ Deployment completed successfully!"
echo "🌐 MinHash API: http://localhost:8080"
echo "📊 Elasticsearch: http://localhost:9200"
if [ "$ENABLE_MONITORING" = "true" ]; then
    echo "📈 Prometheus: http://localhost:9090"
fi

echo "🧪 Test command:"
echo 'curl -X POST http://localhost:8080/api/search -H "Content-Type: application/json" -H "X-API-Key: $API_KEY" -d '"'"'{"context":"def test()","file_path":"test.py","project_name":"test","date":"2024-01-01"}'"'"
