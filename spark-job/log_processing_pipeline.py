import logging
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf, explode, trim, posexplode, split, expr, regexp_replace, rlike, lit, array, struct, col, when, size, coalesce, from_json, to_json, monotonically_increasing_id, row_number
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType
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

# Your PREDEFINED_REGEXES (use the original string patterns, not compiled ones)
# This list needs to be available to your Spark code.
PREDEFINED_REGEXES_NATIVE = [
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

REGEX_MATCH_SCHEMA = StructType([
    StructField("regexId", StringType(), False),
    StructField("log_id", StringType(), False),
    StructField("line_number", IntegerType(), False),
    StructField("matched_text", StringType(), True)
])

# Define the schema for the output of the UDF
# It will return an array of structs, where each struct contains regexId and matched_text
REGEX_UDF_OUTPUT_SCHEMA = ArrayType(StructType([
    StructField("regexId", StringType(), False),
    StructField("matched_text", StringType(), True)
]))

def process_batch_native(df, batch_id):
    """
    Processes each micro-batch of log_ids.
    Collects log_ids, calls the Flask API, performs regex matching, and then writes results to Kafka.
    This version captures ALL regex matches per line (multiple regexes can match the same line).
    Optimized to use a single UDF for all regex matching.
    """
    logger.info(f"Processing batch ID: {batch_id}")

    # Convert the streaming DataFrame to a static DataFrame for this batch
    # This is safe within foreachBatch
    batch_df = df.cache() # Cache for potential multiple uses if needed, though here not strictly necessary.

    # Collect log_ids from the DataFrame on the driver
    # This is still necessary as the API call is external and needs a list.
    log_ids_collected = [row.log_id for row in batch_df.collect()]

    if not log_ids_collected:
        logger.info(f"Batch {batch_id} is empty. Skipping API call.")
        batch_df.unpersist() # Unpersist if cached
        return

    logger.info(f"Batch {batch_id}: Collected {len(log_ids_collected)} unique log IDs for API call.")

    # Call the Flask API to get log contents
    fetched_contents = fetch_log_contents_from_api(log_ids_collected)

    if fetched_contents:
        spark = SparkSession.builder.getOrCreate()
        from pyspark.sql import Row
        from pyspark.sql.functions import posexplode, lit, explode, array, when, col, to_json, struct

        # Prepare data for Spark DataFrame
        processed_rows = []
        for d in fetched_contents:
            log_id_val = d.get('log_id')
            content_val = d.get('content')
            processed_rows.append(Row(log_id=log_id_val, content=content_val))

        # Schema for the initial DataFrame
        content_df_schema = StructType([
            StructField("log_id", StringType(), True),
            StructField("content", StringType(), True)
        ])

        content_df = spark.createDataFrame(processed_rows, schema=content_df_schema)

        # DEBUG: Show initial content DataFrame
        logger.info(f"Batch {batch_id}: Initial content DataFrame count: {content_df.count()}")

        # Filter out rows where content is null
        valid_logs_df = content_df.filter(col("content").isNotNull())

        logger.info(f"Batch {batch_id}: Valid logs count: {valid_logs_df.count()}")

        if valid_logs_df.count() > 0:
            logger.info(f"Batch {batch_id}: Applying native Spark regex matching to fetched log contents.")

            # --- Cleaning BEFORE Splitting ---
            # Normalize line endings first, then split
            df_normalized = valid_logs_df.withColumn(
                "content_normalized",
                regexp_replace(col("content"), "\\r\\n|\\r", "\n")
            )

            # Split the content into individual lines with proper line numbers
            # Use posexplode to get both position (line_number) and the line content
            df_with_lines = df_normalized.withColumn("split_content", split(col("content_normalized"), "\n")) \
                                         .select(
                                             col("log_id"),
                                             posexplode(col("split_content")).alias("position", "line_content")) \
                                         .filter(col("line_content") != "") \
                                         .withColumn("line_number", col("position") + 1) \
                                         .drop("position")

            # --- Cleaning AFTER Splitting ---
            # Apply multiple cleaning steps on each individual line
            cleaned_df = df_with_lines.withColumn(
                "cleaned_line",
                trim(col("line_content")) # Trim leading/trailing whitespace
            ) \
            .withColumn(
                "cleaned_line",
                regexp_replace(col("cleaned_line"), "\\s+", " ") # Replace multiple spaces with single space
            ) \
            .withColumn(
                "cleaned_line",
                regexp_replace(col("cleaned_line"), "[^\\x00-\\x7F]", "") # Remove non-ASCII
            ) \
            .filter(
                col("cleaned_line") != "" # Remove completely empty lines after cleaning
            )

            logger.info(f"Batch {batch_id}: Lines DataFrame after splitting and cleaning count: {cleaned_df.count()}")
            # cleaned_df.show(20, truncate=False) # Uncomment for detailed debug

            # Broadcast the regex patterns to all executors
            # This makes the regexes available locally for the UDF.
            broadcasted_regexes = spark.sparkContext.broadcast(PREDEFINED_REGEXES_NATIVE)

            @udf(returnType=REGEX_UDF_OUTPUT_SCHEMA)
            def find_all_regex_matches(line: str) -> List[Dict[str, str]]:
                """
                UDF to find all matches for all predefined regexes in a single line.
                Returns a list of dictionaries, each with 'regexId' and 'matched_text'.
                """
                if not line:
                    return []
                
                matches = []
                regex_defs = broadcasted_regexes.value # Access the broadcasted value
                for regex_def in regex_defs:
                    regex_id = regex_def["regexId"]
                    pattern = regex_def["pattern"]
                    # Use re.search and group(0) for the full match,
                    # or re.findall for all non-overlapping matches if the pattern
                    # has groups. For simplicity, we'll just check for a match
                    # and return the whole line that matched.
                    # If you need captured groups, you'd adjust this logic.
                    
                    # For patterns like r"user_id=(\w+)", if you want the captured group,
                    # you'd use re.search(pattern, line) and then `match.group(1)`.
                    # For simplicity, returning the whole line for now if it matches.
                    # If the pattern has capturing groups and you want the first group,
                    # you'd need to extend the UDF to handle it.
                    
                    # Current logic: If the pattern matches ANYWHERE in the line, add the line itself.
                    # If you need specific captured groups for regexes like "ADDITIONAL_INFO_LINE",
                    # you'll need a more sophisticated UDF or parse it later.
                    
                    # For now, let's assume `matched_text` is the whole `cleaned_line` that matched.
                    if re.search(pattern, line):
                        matches.append({"regexId": regex_id, "matched_text": line})
                        
                return matches

            # Apply the UDF to find all matches for each cleaned line
            df_with_matches = cleaned_df.withColumn(
                "regex_matches", find_all_regex_matches(col("cleaned_line"))
            )

            # Filter out rows where no regexes matched
            df_with_valid_matches = df_with_matches.filter(
                (col("regex_matches").isNotNull()) & (size(col("regex_matches")) > 0)
            )

            # Explode the array of matches to get one row per (log_id, line_number, regexId, matched_text) tuple
            final_results_df = df_with_valid_matches.select(
                col("log_id"),
                col("line_number"),
                explode(col("regex_matches")).alias("match") # Explode the array of structs
            ).select(
                col("log_id"),
                col("line_number"),
                col("match.regexId").alias("regexId"),
                col("match.matched_text").alias("matched_text")
            ).distinct() # Use distinct to avoid duplicate matches if a regex was defined redundantly

            # Ensure the schema matches for Kafka output
            final_results_df = final_results_df.select(
                col("regexId"), col("log_id"), col("line_number"), col("matched_text")
            ).alias("value_struct") # Alias the struct for clarity before to_json

            final_count = final_results_df.count()
            logger.info(f"Batch {batch_id}: Final results count after processing: {final_count}")

            if final_count > 0:
                logger.info(f"Batch {batch_id}: Sample native Regex matching results:")
                final_results_df.show(truncate=False)

                # Write results to Kafka
                kafka_output_df = final_results_df.select(
                    to_json(struct(col("regexId"), col("log_id"), col("line_number"), col("matched_text"))).alias("value")
                )

                kafka_output_df.write \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
                    .option("topic", KAFKA_OUTPUT_TOPIC) \
                    .save()

                logger.info(f"Batch {batch_id}: Successfully wrote {final_count} regex matches to Kafka topic '{KAFKA_OUTPUT_TOPIC}'.")
            else:
                logger.info(f"Batch {batch_id}: No regex matches found to write to Kafka.")
        else:
            logger.warning(f"Batch {batch_id}: No valid log contents to apply regex matching after cleaning.")
    else:
        logger.warning(f"Batch {batch_id}: No log contents fetched from API.")
    
    batch_df.unpersist() # Unpersist the cached batch_df

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

        logger.info(f"Attempting to read from Kafka broker: {KAFKA_BROKER}, topic: {KAFKA_INPUT_TOPIC}")

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

        query = parsed_log_ids_df.writeStream \
            .foreachBatch(process_batch_native) \
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