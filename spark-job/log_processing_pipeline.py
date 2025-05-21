# /app/log_processing_pipeline.py

import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

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

def main():
    """
    Main function to run the minimal Kafka consumer.
    """
    logger.info("Starting Minimal Kafka Log ID Consumer...")

    try:
        spark = SparkSession.builder \
            .appName("MinimalKafkaLogIdConsumer") \
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

        # Output the log_ids to the console
        # This is a sink operation that starts the streaming query.
        query = parsed_log_ids_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .trigger(processingTime=STREAM_TRIGGER_INTERVAL) \
            .option("checkpointLocation", SPARK_CHECKPOINT_DIR) \
            .start()

        logger.info(f"Streaming query started. Outputting to console. Checkpoint: {SPARK_CHECKPOINT_DIR}")
        logger.info("Waiting for streaming query to terminate (e.g., by manual interruption)...")
        query.awaitTermination()

    except Exception as e:
        logger.error(f"An error occurred in the Spark application: {e}", exc_info=True)
    finally:
        logger.info("Attempting to stop SparkSession...")
        if 'spark' in locals() and spark: # Check if spark variable exists
            spark.stop()
            logger.info("SparkSession stopped.")
        logger.info("Minimal Kafka Log ID Consumer finished.")

if __name__ == "__main__":
    main()
