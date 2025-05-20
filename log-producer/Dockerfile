FROM python:3.9-slim

WORKDIR /app

# Install dependencies
RUN pip install kafka-python requests

# Copy application files
COPY log_producer.py .

# Make script executable
RUN chmod +x log_producer.py

# Command to run the application
CMD ["python", "log_producer.py"]