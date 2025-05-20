#!/usr/bin/env python3
"""
Log Producer - Generates sample logs and sends them to Kafka
"""
import json
import os
import random
import time
import uuid
from datetime import datetime
from kafka import KafkaProducer
import requests

# Configuration from environment variables
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "log-events")
API_ENDPOINT = os.environ.get("API_ENDPOINT", "http://mock-api:5000")

# Connect to Kafka
def create_kafka_producer():
    retries = 0
    max_retries = 10
    
    while retries < max_retries:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print(f"Successfully connected to Kafka at {KAFKA_BROKER}")
            return producer
        except Exception as e:
            retries += 1
            wait_time = retries * 5
            print(f"Failed to connect to Kafka: {str(e)}. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
    
    print("Could not connect to Kafka after maximum retries")
    return None

# Generate random log content
def generate_log_content():
    log_types = ["info", "error", "warning", "debug"]
    log_type = random.choice(log_types)
    
    user_ids = [f"user_{i}" for i in range(1, 11)]
    session_ids = [f"session_{uuid.uuid4().hex[:8]}" for _ in range(10)]
    ip_addresses = [f"192.168.1.{i}" for i in range(1, 255)]
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    user_id = random.choice(user_ids)
    session_id = random.choice(session_ids)
    ip = random.choice(ip_addresses)
    
    if log_type == "error":
        errors = [
            "NullPointerException",
            "OutOfMemoryError",
            "FileNotFoundException",
            "ConnectionRefused",
            "Timeout waiting for response"
        ]
        message = f"{timestamp} [ERROR] user_id={user_id} session_id={session_id} from IP {ip}: {random.choice(errors)}"
    elif log_type == "warning":
        warnings = [
            "Connection attempt timed out",
            "Retrying operation",
            "Slow query detected",
            "Low memory warning",
            "Warning: deprecated method used"
        ]
        message = f"{timestamp} [WARNING] user_id={user_id} session_id={session_id} from IP {ip}: {random.choice(warnings)}"
    else:
        messages = [
            "User logged in",
            "Page viewed",
            "Request processed successfully",
            "Data saved",
            "Operation completed"
        ]
        message = f"{timestamp} [{log_type.upper()}] user_id={user_id} session_id={session_id} from IP {ip}: {random.choice(messages)}"
    
    # Create multi-line content
    lines = [message]
    num_extra_lines = random.randint(0, 5)
    
    for _ in range(num_extra_lines):
        extra = f"    Additional info: request_id={uuid.uuid4().hex[:10]}, process_time={random.randint(10, 500)}ms"
        lines.append(extra)
    
    return "\n".join(lines)

# Send log to API
def send_log_to_api(log_id, content):
    try:
        response = requests.post(
            f"{API_ENDPOINT}/logs",
            json={"log_id": log_id, "content": content},
            timeout=5
        )
        return response.status_code == 201
    except Exception as e:
        print(f"Error sending log to API: {str(e)}")
        return False

# Send log event to Kafka
def send_log_event(producer, log_id):
    try:
        event = {
            "log_id": log_id,
            "timestamp": datetime.now().isoformat(),
            "source": "log-producer"
        }
        producer.send(KAFKA_TOPIC, event)
        print(f"Sent log event for {log_id} to Kafka topic {KAFKA_TOPIC}")
        return True
    except Exception as e:
        print(f"Error sending to Kafka: {str(e)}")
        return False

# Main function
def main():
    print("Starting Log Producer...")
    
    # Connect to Kafka
    producer = create_kafka_producer()
    if not producer:
        print("Exiting due to Kafka connection failure")
        return
    
    # Generate and send logs continuously
    count = 0
    while True:
        try:
            # Generate log
            log_id = f"log_{uuid.uuid4().hex[:10]}"
            content = generate_log_content()
            
            # Send to API
            api_success = send_log_to_api(log_id, content)
            
            # If API succeeded, notify Kafka
            if api_success:
                kafka_success = send_log_event(producer, log_id)
                if kafka_success:
                    count += 1
                    print(f"Successfully processed log #{count}: {log_id}")
            
            # Sleep between logs
            delay = random.uniform(0.5, 3.0)
            time.sleep(delay)
            
        except KeyboardInterrupt:
            print("Log Producer stopped by user")
            break
        except Exception as e:
            print(f"Unexpected error: {str(e)}")
            time.sleep(5)
    
    if producer:
        producer.close()

if __name__ == "__main__":
    main()