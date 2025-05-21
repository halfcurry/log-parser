import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode, array, from_json, lit, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
import requests
import re
import json
from typing import List, Dict, Any
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
KAFKA_INPUT_TOPIC = os.environ.get("KAFKA_TOPIC", "log-events") # Topic containing messages with log_id
KAFKA_OUTPUT_TOPIC = os.environ.get("KAFKA_OUTPUT_TOPIC", "regex-matches") # New topic for regex matches
SPARK_CHECKPOINT_DIR = os.environ.get("SPARK_CHECKPOINT_DIR", "/tmp/spark_checkpoints/minimal_kafka_consumer")
STREAM_TRIGGER_INTERVAL = os.environ.get("STREAM_TRIGGER_INTERVAL", "30 seconds")
MOCK_API_URL = os.environ.get("MOCK_API_URL", "http://mock-api:5000")
BULK_API_URL = f"{MOCK_API_URL}/logs/bulk" # Correctly constructs the bulk API URL
API_KEY = os.environ.get("API_KEY", "default-key")  # For a real system, use a secure method

# --- Pre-defined Regexes (Updated to match generate_log_content function) ---
PREDEFINED_REGEXES = [
    {"regexId": "LOG_TYPE_ERROR", "pattern": r"\[ERROR\]"},
    {"regexId": "LOG_TYPE_WARNING", "pattern": r"\[WARNING\]"},
    {"regexId": "LOG_TYPE_INFO", "pattern": r"\[INFO\]"},
    {"regexId": "LOG_TYPE_DEBUG", "pattern": r"\[DEBUG\]"},
    {"regexId": "ERROR_DETAIL_NPE", "pattern": r"NullPointerException"},
    {"regexId": "ERROR_DETAIL_OOM", "pattern": r"OutOfMemoryError"},
    {"regexId": "ERROR_DETAIL_FILE_NOT_FOUND", "pattern": r"FileNotFoundException"},
    {"regexId": "ERROR_DETAIL_CONN_REFUSED", "pattern": r"ConnectionRefused"},
    {"regexId": "ERROR_DETAIL_TIMEOUT", "pattern": r"Timeout waiting for response"},
    {"regexId": "WARNING_DETAIL_CONN_TIMEOUT", "pattern": r"Connection attempt timed out"},
    {"regexId": "WARNING_DETAIL_RETRY", "pattern": r"Retrying operation"},
    {"regexId": "WARNING_DETAIL_SLOW_QUERY", "pattern": r"Slow query detected"},
    {"regexId": "WARNING_DETAIL_LOW_MEMORY", "pattern": r"Low memory warning"},
    {"regexId": "WARNING_DETAIL_DEPRECATED", "pattern": r"Warning: deprecated method used"},
    {"regexId": "USER_LOGIN_SUCCESS", "pattern": r"User logged in"},
    {"regexId": "PAGE_VIEWED", "pattern": r"Page viewed"},
    {"regexId": "REQUEST_PROCESSED", "pattern": r"Request processed successfully"},
    {"regexId": "DATA_SAVED", "pattern": r"Data saved"},
    {"regexId": "OPERATION_COMPLETED", "pattern": r"Operation completed"},
    {"regexId": "ADDITIONAL_INFO_LINE", "pattern": r"^\s+Additional info: request_id=([a-f0-9]{10}), process_time=(\d+)ms$"},
    {"regexId": "USER_ID_EXTRACT", "pattern": r"user_id=(\w+)"},
    {"regexId": "SESSION_ID_EXTRACT", "pattern": r"session_id=(\w+)"},
    {"regexId": "IP_ADDRESS_EXTRACT", "pattern": r"from IP (\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})"}
]


def fetch_log_contents_from_api(log_ids_list: List[str]) -> List[Dict[str, Any]]:
    """
    Makes a POST request to the Flask API to fetch log contents for a list of log IDs.
    Returns a list of dictionaries with 'log_id' and 'content'.
    """
    if not log_ids_list:
        return []

    try:
        logger.info(f"Calling Flask API at {BULK_API_URL} for {len(log_ids_list)} log IDs.")
        headers = {"Authorization": f"Bearer {API_KEY}"}
        response = requests.post(BULK_API_URL, json={"log_ids": log_ids_list}, headers=headers, timeout=10)
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

# --- UDF for Regex Matching ---
# Define the schema for the output of the regex matching UDF
REGEX_MATCH_SCHEMA = ArrayType(StructType([
    StructField("regexId", StringType(), False),
    StructField("log_id", StringType(), False),
    StructField("line_number", IntegerType(), False),
    StructField("matched_text", StringType(), True) # Stores the entire line that matched
]))

@udf(REGEX_MATCH_SCHEMA)
def match_regex_in_log_content(log_id: str, log_content: str) -> List[Dict[str, Any]]:
    """
    Applies a pre-defined list of regexes to each line of a given log content.
    Returns a list of dictionaries, each representing a match with regexId, log_id, and line_number.
    """
    matches = []
    if not log_content:
        return matches

    lines = log_content.splitlines()
    for line_num, line_text in enumerate(lines):
        for regex_def in PREDEFINED_REGEXES:
            regex_id = regex_def["regexId"]
            pattern = regex_def["pattern"]
            
            # Compile regex for efficiency
            compiled_regex = re.compile(pattern) 

            match = compiled_regex.search(line_text)
            if match:
                matches.append({
                    "regexId": regex_id,
                    "log_id": log_id,
                    "line_number": line_num + 1, # Line numbers are 1-based
                    "matched_text": line_text # Store the entire line that matched
                })
    return matches


def process_batch(df, batch_id):
    """
    Processes each micro-batch of log_ids.
    Collects log_ids, calls the Flask API, performs regex matching, and then writes results to Kafka.
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

        # Prepare data for Spark DataFrame: ensure 'content' is present for processing
        processed_rows = []
        for d in fetched_contents:
            # Ensure 'content' is present, even if None, for the UDF input
            if 'content' not in d:
                d['content'] = None
            processed_rows.append(Row(**d))

        # Schema for the initial DataFrame created from API response
        content_df_schema = StructType([
            StructField("log_id", StringType(), True),
            StructField("content", StringType(), True)
        ])

        content_df = spark.createDataFrame(processed_rows, schema=content_df_schema)

        # Filter out rows where content is null, as regex matching won't apply
        valid_logs_df = content_df.filter(col("content").isNotNull())

        if valid_logs_df.count() > 0:
            logger.info(f"Batch {batch_id}: Applying regex matching to fetched log contents.")
            # Apply the UDF to each row to get regex matches
            matched_logs_df = valid_logs_df.withColumn(
                "regex_matches",
                match_regex_in_log_content(col("log_id"), col("content"))
            )

            # Explode the array of matches to get one row per match
            final_results_df = matched_logs_df.select(
                explode(col("regex_matches")).alias("match_details")
            ).select(
                "match_details.regexId",
                "match_details.log_id",
                "match_details.line_number",
                "match_details.matched_text"
            )

            logger.info(f"Batch {batch_id}: Regex matching results (also writing to Kafka topic '{KAFKA_OUTPUT_TOPIC}'):")
            final_results_df.show(truncate=False)

            # --- Write results to Kafka ---
            # Convert the DataFrame rows to JSON strings for Kafka
            # The 'value' column must be a string or binary type
            kafka_output_df = final_results_df.select(
                to_json(struct(col("regexId"), col("log_id"), col("line_number"), col("matched_text"))).alias("value")
            )

            # Write the DataFrame to Kafka
            kafka_output_df.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BROKER) \
                .option("topic", KAFKA_OUTPUT_TOPIC) \
                .save() # Use .save() for batch write within foreachBatch

            logger.info(f"Batch {batch_id}: Successfully wrote {kafka_output_df.count()} regex matches to Kafka topic '{KAFKA_OUTPUT_TOPIC}'.")

        else:
            logger.warning(f"Batch {batch_id}: No valid log contents to apply regex matching.")

    else:
        logger.warning(f"Batch {batch_id}: No contents fetched from API.")

def main():
    """
    Main function to run the Kafka consumer, fetch log IDs,
    and then use them to retrieve full log content via a Flask API.
    """
    logger.info("Starting Kafka Log Content Consumer...")

    try:
        spark = SparkSession.builder \
            .appName("KafkaLogContentConsumer") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
            .getOrCreate()

        spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
        logger.info("SparkSession initialized successfully.")

        kafka_message_schema = StructType([
            StructField("log_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("source", StringType(), True)
        ])

        logger.info(f"Attempting to read from Kafka broker: {KAFKA_BROKER}, topic: {KAFKA_INPUT_TOPIC}") # Changed to KAFKA_INPUT_TOPIC

        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("subscribe", KAFKA_INPUT_TOPIC) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        logger.info("Successfully connected to Kafka stream source.")

        parsed_log_ids_df = kafka_df \
            .select(from_json(col("value").cast("string"), kafka_message_schema).alias("data")) \
            .select("data.log_id") \
            .filter(col("log_id").isNotNull() & (col("log_id") != ""))

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
        if 'spark' in locals() and spark:
            spark.stop()
            logger.info("SparkSession stopped.")
        logger.info("Kafka Log Content Consumer finished.")

if __name__ == "__main__":
    main()

