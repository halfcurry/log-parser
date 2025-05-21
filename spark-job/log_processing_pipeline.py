import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode, array, from_json, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
import requests
import re
import json
from typing import List, Dict
import time

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# --- Configuration from Environment Variables ---
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "log-events") # Topic containing messages with log_id
SPARK_CHECKPOINT_DIR = os.environ.get("SPARK_CHECKPOINT_DIR", "/tmp/spark_checkpoints/minimal_kafka_consumer")
STREAM_TRIGGER_INTERVAL = os.environ.get("STREAM_TRIGGER_INTERVAL", "30 seconds")
MOCK_API_URL = os.environ.get("MOCK_API_URL", "http://mock-api:5000")
BULK_API_URL = f"{MOCK_API_URL}/logs/bulk" # Correctly constructs the bulk API URL
API_KEY = os.environ.get("API_KEY", "default-key")  # For a real system, use a secure method

def fetch_log_contents_from_api(log_ids_list):
    """
    Makes a POST request to the Flask API to fetch log contents for a list of log IDs.
    Returns a list of dictionaries with 'log_id' and 'content'.
    """
    if not log_ids_list:
        return []

    try:
        logger.info(f"Calling Flask API at {BULK_API_URL} for {len(log_ids_list)} log IDs.") # Changed to use BULK_API_URL
        headers = {"Authorization": f"Bearer {API_KEY}"}
        response = requests.post(BULK_API_URL, json={"log_ids": log_ids_list}, headers=headers, timeout=10) # Changed to use BULK_API_URL
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        return response.json()
    except requests.exceptions.Timeout:
        logger.error(f"Flask API call timed out after 10 seconds for {len(log_ids_list)} log IDs.")
        return []
    except requests.exceptions.RequestException as e:
        logger.error(f"Error calling Flask API: {e}", exc_info=True)
        return []
    except Exception as e:
        logger.error(f"An unexpected error occurred during API call: {e}", exc_info=True)
        return []

def process_batch(df, batch_id):
    """
    Processes each micro-batch of log_ids.
    Collects log_ids, calls the Flask API, and then prints the results.
    """
    logger.info(f"Processing batch ID: {batch_id}")

    # Collect log_ids from the DataFrame. This brings data to the driver.
    # Be cautious with very large batches as this can cause OOM errors on the driver.
    log_ids_collected = [row.log_id for row in df.collect()]

    if not log_ids_collected:
        logger.info(f"Batch {batch_id} is empty. Skipping API call.")
        return

    logger.info(f"Batch {batch_id}: Collected {len(log_ids_collected)} unique log IDs for API call.")

    # Call the Flask API to get log contents
    fetched_contents = fetch_log_contents_from_api(log_ids_collected)

    if fetched_contents:
        spark = SparkSession.builder.getOrCreate()
        from pyspark.sql import Row
        rows = [Row(**d) for d in fetched_contents]

        content_df_schema = StructType([
            StructField("log_id", StringType(), True),
            StructField("content", StringType(), True)
        ])

        content_df = spark.createDataFrame(rows, schema=content_df_schema)

        # Show the results (log_id and content)
        logger.info(f"Batch {batch_id}: Fetched contents from API:")
        content_df.show(truncate=False)
    else:
        logger.warning(f"Batch {batch_id}: No contents fetched from API.")

def main():
    """
    Main function to run the Kafka consumer, fetch log IDs,
    and then use them to retrieve full log content via a Flask API.
    """
    logger.info("Starting Kafka Log Content Consumer...") # Updated app name

    try:
        spark = SparkSession.builder \
            .appName("KafkaLogContentConsumer") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
            .getOrCreate()

        # Set Spark log level to WARN to reduce verbosity from Spark itself
        spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
        logger.info("SparkSession initialized successfully.")

        # Define the schema for the incoming Kafka messages (assuming JSON with a 'log_id' field)
        # Adjust this schema if your Kafka message structure is different.
        kafka_message_schema = StructType([
            StructField("log_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("source", StringType(), True)
        ])

        logger.info(f"Attempting to read from Kafka broker: {KAFKA_BROKER}, topic: {KAFKA_TOPIC}")

        # Read from Kafka
        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        logger.info("Successfully connected to Kafka stream source.")

        # Parse the JSON message from the 'value' column (which is binary)
        # and select the 'log_id' field.
        parsed_log_ids_df = kafka_df \
            .select(from_json(col("value").cast("string"), kafka_message_schema).alias("data")) \
            .select("data.log_id") \
            .filter(col("log_id").isNotNull() & (col("log_id") != "")) # Ensure log_id is not null or empty

        logger.info("Stream parsing and transformation defined.")

        # # Output the log_ids to the console
        # # This is a sink operation that starts the streaming query.
        # query = parsed_log_ids_df.writeStream \
        #     .outputMode("append") \
        #     .format("console") \
        #     .option("truncate", "false") \
        #     .trigger(processingTime=STREAM_TRIGGER_INTERVAL) \
        #     .option("checkpointLocation", SPARK_CHECKPOINT_DIR) \
        #     .start()

        # Use foreachBatch to process each micro-batch.
        # This allows us to collect log_ids and make an external API call.
        query = parsed_log_ids_df.writeStream \
            .foreachBatch(process_batch) \
            .trigger(processingTime=STREAM_TRIGGER_INTERVAL) \
            .option("checkpointLocation", SPARK_CHECKPOINT_DIR) \
            .start()

        logger.info(f"Streaming query started. Processing batches with API calls. Checkpoint: {SPARK_CHECKPOINT_DIR}")
        logger.info("Waiting for streaming query to terminate (e.g., by manual interruption)...")
        query.awaitTermination()

    except Exception as e:
        logger.error(f"An error occurred in the Spark application: {e}", exc_info=True)
    finally:
        logger.info("Attempting to stop SparkSession...")
        if 'spark' in locals() and spark: # Check if spark variable exists
            spark.stop()
            logger.info("SparkSession stopped.")
        logger.info("Kafka Log Content Consumer finished.") # Updated log message

if __name__ == "__main__":
    main()

