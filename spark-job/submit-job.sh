#!/bin/bash

# Script to submit Spark job
# This script can be run inside the spark-submit container

# Wait for Spark master to be ready
echo "Waiting for Spark master to be ready..."
sleep 10

# Environment variables
SPARK_MASTER=${SPARK_MASTER:-"spark://spark-master:7077"}
KAFKA_BROKER=${KAFKA_BROKER:-"kafka:9092"}
MOCK_API_URL=${MOCK_API_URL:-"http://mock-api:5000"}
PROCESSING_MODE=${PROCESSING_MODE:-"stream"}

echo "Submitting Spark job with the following configuration:"
echo "Spark Master: $SPARK_MASTER"
echo "Kafka Broker: $KAFKA_BROKER"
echo "Mock API URL: $MOCK_API_URL"
echo "Processing Mode: $PROCESSING_MODE"

# Define the Kafka packages to be downloaded by Spark
# Ensure the Scala version (2.12) and Spark version (3.5.1) match your base image
KAFKA_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.kafka:kafka-clients:3.5.1,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.1"

# Submit the Spark job
spark-submit \
  --master $SPARK_MASTER \
  --name "Log Processing Pipeline" \
  --packages "$KAFKA_PACKAGES" \
  --conf "spark.executor.memory=1g" \
  --conf "spark.driver.memory=1g" \
  --conf "spark.executor.cores=1" \
  --conf "spark.driver.cores=1" \
  --conf "spark.sql.shuffle.partitions=10" \
  --conf "spark.default.parallelism=10" \
  --py-files /app/log_processing_pipeline.py \
  /app/log_processing_pipeline.py

# Check exit status
if [ $? -eq 0 ]; then
  echo "Spark job submitted successfully."
else
  echo "Failed to submit Spark job."
  exit 1
fi