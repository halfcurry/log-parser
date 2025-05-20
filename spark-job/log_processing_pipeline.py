#!/usr/bin/env python3
"""
Log Processing Pipeline with PySpark
Usage: spark-submit --master spark://spark-master:7077 log_processing_pipeline.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode, array, from_json, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
import requests
import re
import os
import json
from typing import List, Dict
import time

# Get configuration from environment variables
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "log-events")
MOCK_API_URL = os.environ.get("MOCK_API_URL", "http://mock-api:5000")
API_KEY = os.environ.get("API_KEY", "default-key")  # For a real system, use a secure method

# Define your regex patterns
REGEX_PATTERNS = {
    "error": r"(?i)error|exception|fail|failed",
    "warning": r"(?i)warning|warn",
    "ip_address": r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b",
    "timestamp": r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}",
    "user_id": r"user_id[=:]\s*(\w+)",
    "session_id": r"session[_-]id[=:]\s*(\w+)",
    "request_id": r"request[_-]id[=:]\s*([a-zA-Z0-9-]+)"
}

def fetch_log_content(log_id: str) -> str:
    """Fetch log content from REST API using log ID"""
    try:
        headers = {"Authorization": f"Bearer {API_KEY}"}
        response = requests.get(f"{MOCK_API_URL}/logs/{log_id}", headers=headers, timeout=5)
        
        if response.status_code == 200:
            return response.text
        else:
            print(f"Failed to fetch log {log_id}: {response.status_code}")
            return ""
    except Exception as e:
        print(f"Error fetching log {log_id}: {str(e)}")
        return ""

def post_results_to_db(results: List[Dict]) -> bool:
    """Post match results to database via API"""
    if not results:
        return True  # Nothing to post
        
    try:
        headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json"
        }
        response = requests.post(f"{MOCK_API_URL}/results", headers=headers, json=results, timeout=10)
        
        if response.status_code in [200, 201]:
            print(f"Successfully posted {len(results)} results")
            return True
        else:
            print(f"Failed to post results: {response.status_code}")
            return False
    except Exception as e:
        print(f"Error posting results: {str(e)}")
        return False

def process_log_with_regex(log_id: str, log_content: str) -> List[Dict]:
    """Process log content with regex patterns and return matches with line numbers"""
    if not log_content:
        return []
        
    results = []
    lines = log_content.split('\n')
    
    for line_num, line in enumerate(lines, 1):
        for regex_id, pattern in REGEX_PATTERNS.items():
            matches = re.findall(pattern, line)
            if matches:
                for match in matches:
                    match_text = match if isinstance(match, str) else match[0]
                    results.append({
                        "regex_id": regex_id,
                        "line_number": line_num,
                        "log_id": log_id,
                        "match": match_text,
                        "line_content": line[:100]  # Store part of the line for context
                    })
    
    return results

def create_spark_session():
    """Initialize and configure Spark Session"""
    # For local development/testing
    return SparkSession.builder \
        .appName("LogProcessingPipeline") \
        .config("spark.sql.shuffle.partitions", 10) \
        .config("spark.default.parallelism", 10) \
        .getOrCreate()

def batch_process_logs():
    """Process logs in batches from the mock API directly (for testing without Kafka)"""
    spark = create_spark_session()
    
    try:
        # For testing: Fetch a batch of log IDs from the mock API
        response = requests.get(f"{MOCK_API_URL}/list-logs", timeout=5)
        log_ids = response.json() if response.status_code == 200 else []
        
        if not log_ids:
            print("No logs found to process")
            return
            
        print(f"Processing {len(log_ids)} logs in batch mode")
        
        # Convert log IDs to DataFrame
        log_ids_rdd = spark.sparkContext.parallelize(log_ids)
        log_df = log_ids_rdd.map(lambda id: (id,)).toDF(["log_id"])
        
        # Process logs
        process_logs_dataframe(spark, log_df)
        
    finally:
        spark.stop()

def stream_process_logs():
    """Process logs in streaming mode from Kafka"""
    spark = create_spark_session()
    
    try:
        # Define schema for incoming Kafka messages
        schema = StructType([
            StructField("log_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("source", StringType(), True)
        ])
        
        # Create Kafka stream
        kafka_stream = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse Kafka messages
        parsed_stream = kafka_stream \
            .selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")
        
        # Process each batch of the stream
        query = parsed_stream \
            .writeStream \
            .foreachBatch(process_logs_dataframe) \
            .outputMode("update") \
            .trigger(processingTime="30 seconds") \
            .start()
        
        # Wait for termination
        query.awaitTermination()
        
    finally:
        spark.stop()

def process_logs_dataframe(spark, batch_df):
    """Process a DataFrame of log IDs"""
    # Register UDFs
    spark.udf.register("fetch_log_content_udf", fetch_log_content, StringType())
    
    # Fetch log content for each log ID
    logs_with_content = batch_df.withColumn(
        "content", 
        spark.udf.expr("fetch_log_content_udf(log_id)")
    )
    
    # Filter out logs with no content
    valid_logs = logs_with_content.filter(col("content").isNotNull() & (col("content") != ""))
    
    # Convert DataFrame operations to RDD operations for better control
    results_rdd = valid_logs.rdd.flatMap(
        lambda row: process_log_with_regex(row["log_id"], row["content"])
    )
    
    # Convert results back to DataFrame for further processing
    if not results_rdd.isEmpty():
        results_schema = StructType([
            StructField("regex_id", StringType(), True),
            StructField("line_number", IntegerType(), True),
            StructField("log_id", StringType(), True),
            StructField("match", StringType(), True),
            StructField("line_content", StringType(), True)
        ])
        
        results_df = spark.createDataFrame(results_rdd, results_schema)
        
        # For debugging
        print(f"Found {results_df.count()} regex matches")
        
        # Collect results and post to DB in batches
        results = results_df.collect()
        result_dicts = [row.asDict() for row in results]
        
        # Post results in batches of 100
        for i in range(0, len(result_dicts), 100):
            batch = result_dicts[i:i+100]
            post_results_to_db(batch)

def run_optimized_processing():
    """
    Alternative implementation with optimized parallel processing
    This avoids UDFs (which can be slower) and uses more native Spark operations
    """
    spark = create_spark_session()
    
    try:
        # For testing: Fetch a batch of log IDs from the mock API
        try:
            response = requests.get(f"{MOCK_API_URL}/list-logs", timeout=5)
            log_ids = response.json() if response.status_code == 200 else []
        except Exception:
            # Fallback to sample data if API not available
            log_ids = [f"log_{i}" for i in range(10)]
        
        # Create RDD from log IDs
        log_ids_rdd = spark.sparkContext.parallelize(log_ids, numPartitions=10)
        
        # Fetch log content in parallel
        logs_content_rdd = log_ids_rdd.map(lambda log_id: (log_id, fetch_log_content(log_id)))
        
        # Filter out empty content
        valid_logs_rdd = logs_content_rdd.filter(lambda x: x[1] != "")
        
        # Process each log with regex patterns
        def process_log(log_tuple):
            log_id, content = log_tuple
            return process_log_with_regex(log_id, content)
        
        # Process all logs and flatten results
        all_matches_rdd = valid_logs_rdd.flatMap(process_log)
        
        # If there are matches, convert to DataFrame and save
        if not all_matches_rdd.isEmpty():
            # Convert to a list for posting to API
            results = all_matches_rdd.collect()
            
            # Post results in batches
            for i in range(0, len(results), 100):
                batch = results[i:i+100]
                post_results_to_db(batch)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    print("Starting Log Processing Pipeline")
    
    # Choose processing mode based on environment variable
    processing_mode = os.environ.get("PROCESSING_MODE", "stream")
    
    if processing_mode == "stream":
        print("Running in streaming mode")
        stream_process_logs()
    elif processing_mode == "batch":
        print("Running in batch mode")
        batch_process_logs()
    elif processing_mode == "optimized":
        print("Running optimized processing")
        run_optimized_processing()
    else:
        print(f"Unknown processing mode: {processing_mode}")