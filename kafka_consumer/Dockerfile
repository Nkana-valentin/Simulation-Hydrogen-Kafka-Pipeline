# kafka_consumer/Dockerfile
FROM python:3.10-slim

WORKDIR /app

# Copy requirements
COPY kafka_consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy shared ingestion module
COPY ingestion ./ingestion

# Copy consumer service
COPY kafka_consumer/kafka_consumer_to_tsdb.py .

# Copy configuration
COPY TSDB.yml .

# Set PYTHONPATH
ENV PYTHONPATH=/app

CMD ["python", "-u", "kafka_consumer_to_tsdb.py"]