FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy new shared packages
COPY config ./config
COPY domain ./domain
COPY infrastructure ./infrastructure
COPY services ./services

# Copy entry point
COPY kafka_consumer/kafka_consumer_to_tsdb.py .

COPY TSDB.yml .

ENV PYTHONPATH=/app

CMD ["python", "-u", "kafka_consumer_to_tsdb.py"]
