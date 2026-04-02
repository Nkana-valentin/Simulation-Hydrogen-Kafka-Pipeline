# kafka_producer/Dockerfile
FROM python:3.10-slim

WORKDIR /app

# Copy requirements
COPY kafka_producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy shared ingestion module
COPY ingestion ./ingestion

# Copy producer service
COPY kafka_producer/kafka_producer_service.py .

# Copy configuration
COPY TSDB.yml .

# Set PYTHONPATH
ENV PYTHONPATH=/app

CMD ["python", "-u", "kafka_producer_service.py"]