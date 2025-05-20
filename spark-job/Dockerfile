FROM bitnami/spark:latest

USER root

# Install Python dependencies
RUN apt-get update && apt-get install -y python3-pip

# Install required Python packages
RUN pip3 install pyspark kafka-python requests

# Create app directory
WORKDIR /app

# Copy the spark job file
COPY log_processing_pipeline.py /app/

# Make the script executable
RUN chmod +x /app/log_processing_pipeline.py

# Keep the container running
CMD ["tail", "-f", "/dev/null"]