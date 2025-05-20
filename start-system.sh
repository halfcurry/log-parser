#!/bin/bash

# Start the entire log processing system

# Check if Docker and Docker Compose are available
if ! command -v docker >/dev/null 2>&1; then
  echo "Error: Docker is not installed. Please install Docker first."
  exit 1
fi

if ! command -v docker-compose >/dev/null 2>&1; then
  echo "Error: Docker Compose is not installed. Please install Docker Compose first."
  exit 1
fi

# Set Kafka topic
export KAFKA_TOPIC="log-events"

echo "Starting the log processing system..."
echo "This will create all necessary containers and networks."

# Build and start all services
docker-compose up -d

echo "Waiting for services to initialize (30 seconds)..."
sleep 30

# Verify services are running
echo "Checking if services are running..."
if [ $(docker-compose ps | grep "Up" | wc -l) -lt 9 ]; then
  echo "Warning: Not all services appear to be running. Please check docker-compose logs for errors."
else
  echo "All services are running."
fi

# Submit Spark job to process logs
echo "Submitting Spark job to start processing logs..."
docker-compose exec spark-submit bash -c "cd /app && ./submit-job.sh"

echo ""
echo "Log processing system is now running."
echo ""
echo "Access points:"
echo "- Kafka UI: http://localhost:8080"
echo "- Spark Master UI: http://localhost:8181"
echo "- Mock API: http://localhost:5000/health"
echo ""
echo "To view logs, run:"
echo "  docker-compose logs -f <service-name>"
echo ""
echo "Available services: zookeeper, kafka, kafka-ui, postgres, mock-api,"
echo "spark-master, spark-worker-1, spark-worker-2, log-producer, spark-submit"
echo ""
echo "To stop the system, run:"
echo "  docker-compose down -v"