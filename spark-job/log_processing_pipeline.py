import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode, posexplode, split, expr, rlike, lit, array, struct, col, when, size, coalesce, from_json, to_json
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
# Compile regexes once at startup
PREDEFINED_REGEXES_COMPILED = [
    {"regexId": entry["regexId"], "pattern": re.compile(entry["pattern"])}
    for entry in [
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
]

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

# --- UDF for Regex Matching ---
# Define the schema for the output of the regex matching UDF
# REGEX_MATCH_SCHEMA = ArrayType(StructType([
#     StructField("regexId", StringType(), False),
#     StructField("log_id", StringType(), False),
#     StructField("line_number", IntegerType(), False),
#     StructField("matched_text", StringType(), True) # Stores the entire line that matched
# ]))

REGEX_MATCH_SCHEMA = StructType([
    StructField("regexId", StringType(), False),
    StructField("log_id", StringType(), False),
    StructField("line_number", IntegerType(), False),
    StructField("matched_text", StringType(), True)
])


# I don't want to use the below thingy. Its bad, I can't debug anything. 
@udf(REGEX_MATCH_SCHEMA)
def match_regex_in_log_content(log_id: str, log_content: str) -> List[Dict[str, Any]]:
    """
    Applies a pre-defined list of regexes to each line of a given log content.
    Returns a list of dictionaries, each representing a match with regexId, log_id, and line_number.
    """
    matches = []
    if not log_content:
        logger.warn(f"UDF: No log content for log_id: {log_id}")
        return matches

    lines = log_content.splitlines()
    logger.warn(f"UDF: Processing log_id: {log_id} with {len(lines)} lines.")

    for line_num, line_text in enumerate(lines):
        # Iterate over the already compiled regexes
        # Log the line content itself
        logger.warn(f"UDF: Checking log_id: {log_id}, Line {line_num + 1}: '{line_text}'")

        for regex_def in PREDEFINED_REGEXES_COMPILED:
            regex_id = regex_def["regexId"]
            compiled_regex = regex_def["pattern"] # Use the pre-compiled pattern

            # Log the regex pattern being applied
            logger.warn(f"UDF: Applying regex '{compiled_regex.pattern}' (ID: {regex_id}) to line.")

            match = compiled_regex.search(line_text)
            if match:
                logger.warn(f"UDF: MATCH FOUND! Log ID: {log_id}, Regex ID: {regex_id}, Line {line_num + 1}: '{line_text}'")
                matches.append({
                    "regexId": regex_id,
                    "log_id": log_id,
                    "line_number": line_num + 1, # Line numbers are 1-based
                    "matched_text": line_text # Store the entire line that matched
                })
            else:
                logger.warn(f"UDF: No match for Regex ID: {regex_id} on Line {line_num + 1}.")

    if not matches:
        logger.warn(f"UDF: No matches found for log_id: {log_id} after checking all lines and regexes.")
    else:
        logger.warn(f"UDF: Total {len(matches)} matches found for log_id: {log_id}.")
    
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
            logger.info(f"Batch {batch_id}: Applying native Spark regex matching to fetched log contents.")

            # Step 1: Split content into individual lines with proper line numbers
            # Use posexplode to get both position (line_number) and the line content
            df_with_lines = valid_logs_df.select(
                col("log_id"),
                posexplode(split(col("content"), "\n")).alias("line_number_temp", "line")
            ).withColumn("line_number", col("line_number_temp") + 1).drop("line_number_temp")

            # Step 2: Process each regex separately to capture all matches per line
            result_dfs = []

            
            for regex_def in PREDEFINED_REGEXES_NATIVE:
                regex_id = regex_def["regexId"]
                pattern = regex_def["pattern"]
                
                # Create a DataFrame for lines that match this specific regex
                matched_lines = df_with_lines.filter(col("line").rlike(pattern)) \
                                             .select(
                                                 lit(regex_id).alias("regexId"),
                                                 col("log_id"),
                                                 col("line_number"),
                                                 col("line").alias("matched_text")
                                             )
                result_dfs.append(matched_lines)
            
            # Union all the results
            if result_dfs:
                final_results_df = result_dfs[0]
                for i in range(1, len(result_dfs)):
                    final_results_df = final_results_df.unionByName(result_dfs[i])
            else:
                final_results_df = spark.createDataFrame([], schema=REGEX_MATCH_SCHEMA)

            logger.info(f"Batch {batch_id}: Native Regex matching results (also writing to Kafka topic '{KAFKA_OUTPUT_TOPIC}'):")
            final_results_df.show(truncate=False)

            # Write results to Kafka
            if final_results_df.count() > 0:
                kafka_output_df = final_results_df.select(
                    to_json(struct(col("regexId"), col("log_id"), col("line_number"), col("matched_text"))).alias("value")
                )

                kafka_output_df.write \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
                    .option("topic", KAFKA_OUTPUT_TOPIC) \
                    .save()

                logger.info(f"Batch {batch_id}: Successfully wrote {final_results_df.count()} regex matches to Kafka topic '{KAFKA_OUTPUT_TOPIC}'.")
            else:
                logger.info(f"Batch {batch_id}: No regex matches found to write to Kafka.")

        else:
            logger.warning(f"Batch {batch_id}: No valid log contents to apply regex matching.")

    else:
        logger.warning(f"Batch {batch_id}: No contents fetched from API.")

def process_batch_native(df, batch_id):
    """
    Processes each micro-batch of log_ids.
    Collects log_ids, calls the Flask API, performs regex matching, and then writes results to Kafka.
    This version captures ALL regex matches per line (multiple regexes can match the same line).
    """
    logger.info(f"Processing batch ID: {batch_id}")

    # Collect log_ids from the DataFrame
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
        from pyspark.sql.functions import posexplode, lit, explode, array, when, col

        # Prepare data for Spark DataFrame
        processed_rows = []
        for d in fetched_contents:
            if 'content' not in d:
                d['content'] = None
            processed_rows.append(Row(**d))

        # Schema for the initial DataFrame
        content_df_schema = StructType([
            StructField("log_id", StringType(), True),
            StructField("content", StringType(), True)
        ])

        content_df = spark.createDataFrame(processed_rows, schema=content_df_schema)
        
        # DEBUG: Show initial content DataFrame
        logger.info(f"Batch {batch_id}: Initial content DataFrame:")
        # content_df.show(truncate=False)
        logger.info(f"Batch {batch_id}: Content DataFrame count: {content_df.count()}")

        # Filter out rows where content is null
        valid_logs_df = content_df.filter(col("content").isNotNull())
        
        # DEBUG: Show valid logs DataFrame
        # logger.info(f"Batch {batch_id}: Valid logs DataFrame:")
        # valid_logs_df.show(truncate=False)
        logger.info(f"Batch {batch_id}: Valid logs count: {valid_logs_df.count()}")

        if valid_logs_df.count() > 0:
            logger.info(f"Batch {batch_id}: Applying native Spark regex matching to fetched log contents.")

            # Step 1: Split content into individual lines with proper line numbers
            # Use posexplode to get both position (line_number) and the line content

            df_split = df.withColumn("content", split(df["text"], "\\n"))

            df_with_lines = valid_logs_df.select(
                col("log_id"),
                posexplode(split(col("content"), "\\n"))
            ).toDF("log_id", "line_number_temp", "line") \
             .withColumn("line_number", col("line_number_temp") + 1) \
             .drop("line_number_temp")

            # DEBUG: Show lines DataFrame
            logger.info(f"Batch {batch_id}: Lines DataFrame after splitting:")
            # df_with_lines.show(20, truncate=False)
            logger.info(f"Batch {batch_id}: Lines count: {df_with_lines.count()}")
            
            # DEBUG: Show sample of lines to check content
            # logger.info(f"Batch {batch_id}: Sample lines:")
            # sample_lines = df_with_lines.select("line").limit(10).collect()
            # for i, row in enumerate(sample_lines):
            #     logger.info(f"Line {i}: '{row.line}'")

            # Step 2: Process each regex separately to capture all matches per line
            result_dfs = []
            
            # DEBUG: Show regex patterns being used
            # logger.info(f"Batch {batch_id}: Processing {len(PREDEFINED_REGEXES_NATIVE)} regex patterns:")
            # for regex_def in PREDEFINED_REGEXES_NATIVE:
            #     logger.info(f"  - {regex_def['regexId']}: {regex_def['pattern']}")

            
            # for regex_def in PREDEFINED_REGEXES_NATIVE:
            #     regex_id = regex_def["regexId"]
            #     pattern = regex_def["pattern"]
                
            #     # Create a DataFrame for lines that match this specific regex
            #     matched_lines = df_with_lines.filter(col("line").rlike(pattern)) \
            #                                  .select(
            #                                      lit(regex_id).alias("regexId"),
            #                                      col("log_id"),
            #                                      col("line_number"),
            #                                      col("line").alias("matched_text")
            #                                  )
                
            #     # DEBUG: Show matches for each regex
            #     match_count = matched_lines.count()
            #     logger.info(f"Batch {batch_id}: Regex '{regex_id}' matched {match_count} lines")
            #     if match_count > 0:
            #         logger.info(f"Batch {batch_id}: Sample matches for regex '{regex_id}':")
            #         matched_lines.show(5, truncate=False)
                
            #     result_dfs.append(matched_lines)
            
            # # Union all the results
            # if result_dfs:
            #     final_results_df = result_dfs[0]
            #     for i in range(1, len(result_dfs)):
            #         final_results_df = final_results_df.unionByName(result_dfs[i])
                
            #     # DEBUG: Show final results before Kafka write
            #     final_count = final_results_df.count()
            #     logger.info(f"Batch {batch_id}: Final results count after union: {final_count}")
                
            # else:
            #     final_results_df = spark.createDataFrame([], schema=REGEX_MATCH_SCHEMA)
            #     logger.info(f"Batch {batch_id}: No result DataFrames to union - creating empty DataFrame")

            # logger.info(f"Batch {batch_id}: Native Regex matching results (also writing to Kafka topic '{KAFKA_OUTPUT_TOPIC}'):")
            # final_results_df.show(truncate=False)

            # # Write results to Kafka
            # if final_results_df.count() > 0:
            #     kafka_output_df = final_results_df.select(
            #         to_json(struct(col("regexId"), col("log_id"), col("line_number"), col("matched_text"))).alias("value")
            #     )

            #     kafka_output_df.write \
            #         .format("kafka") \
            #         .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            #         .option("topic", KAFKA_OUTPUT_TOPIC) \
            #         .save()

            #     logger.info(f"Batch {batch_id}: Successfully wrote {final_results_df.count()} regex matches to Kafka topic '{KAFKA_OUTPUT_TOPIC}'.")
            # else:
            #     logger.info(f"Batch {batch_id}: No regex matches found to write to Kafka.")

        else:
            logger.warning(f"Batch {batch_id}: No valid log contents to apply regex matching.")

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
        # query = parsed_log_ids_df.writeStream \
        #     .foreachBatch(process_batch) \
        #     .trigger(processingTime=STREAM_TRIGGER_INTERVAL) \
        #     .option("checkpointLocation", SPARK_CHECKPOINT_DIR) \
        #     .start()

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

