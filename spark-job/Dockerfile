FROM bitnami/spark:3.5.1

#Python is 3.11

USER root

# Install Python dependencies
RUN apt-get update && apt-get install -y python3-pip wget

# Install required Python packages
RUN pip3 install pyspark==3.5.1 kafka-python requests aiohttp

# Download the Spark Kafka connector JAR
# Make sure to match the version with your Spark version
# RUN mkdir -p /opt/bitnami/spark/jars/ 
# RUN wget -P /opt/bitnami/spark/jars/ https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar
# RUN wget -P /opt/bitnami/spark/jars/ https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar
# RUN wget -P /opt/bitnami/spark/jars/ https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar
# RUN wget -P /opt/bitnami/spark/jars/ https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar

# Create app directory
WORKDIR /app

# Copy the spark job file and script
COPY log_processing_pipeline.py submit-job.sh /app/

# Make the scripts executable and verify (the 'ls' will show in build logs)
RUN chmod +x /app/log_processing_pipeline.py /app/submit-job.sh && \
    ls -la /app/

# Keep the container running
CMD ["tail", "-f", "/dev/null"]