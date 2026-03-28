# H2SmartLab Data Pipeline

## About

The aim of this project is to simulate a **real-time communication** between a physical hydrogen laboratory and its digital twin platform through **secure and reliable data exchange**.

---

## Description

This project implements a **real-time data ingestion and processing pipeline** for hydrogen laboratory data, designed following **FAIR principles** and modern **industrial streaming architectures**.

The system streams data from a **Physical Twin (lab sensors)** through Apache Kafka, applies **data quality validation**, stores it in a **Time-Series Database (QuestDB)**, and exposes it via a **REST API** for integration with a **Digital Twin (SISSA Hydor platform)**.

---

## Architecture Overview
<p align="center">
  <img src="images/github_internship_image_cropped.jpg" width="800"/>
</p>

---

## Table of Contents

- [Installation](#installation)
- [Getting Started](#getting-started)
- [Pipeline Execution](#pipeline-execution)
- [Contributing](#contributing)
- [License](#license)
- [Acknowledgments](#acknowledgments)
- [Contact](#contact)

---

## Installation

```bash
# Clone the repository
git clone https://github.com/Nkana-valentin/Simulation-Hydrogen-Kafka-Pipeline.git

# Navigate to the project directory
cd Simulation-Hydrogen-Kafka-Pipeline
```

---

## Prerequisites

- Python 3.9+
- Docker & Docker Compose
- Apache Kafka (via Docker)
- QuestDB (via Docker)

---

## Getting Started

### 1. Create a virtual environment
    ```bash
    python3 -m venv .venv
    ```
### 2. Activate the environment
    ```bash
    source .venv/bin/activate
    ```

### 3. Install dependencies
    ```bash
    pip install -r requirements.txt
    ```

### 4. Start infrastructure services  
    ```bash
    docker compose up -d
    ```

### 5. Start the FastAPI server 
    ```bash
    uvicorn main:app --reload
    ```

### 6. Run the Kafka producer (data simulation) 
    ```bash
    python3 kafkaservices/kafka_producer_service.py
```

### 7. Run the Kafka consumer (ingestion → TSDB)
    ```bash
        python3 kafkaservices/kafka_consumer_to_tsdb.py
    ```   

---

## Data Flow
- Physical Twin generates sensor data (temperature, pressure, flow, etc.)
- Kafka streams real-time events
- Quality Layer validates and cleans the data
- QuestDB stores time-series data efficiently
- API (FastAPI) exposes validated data
- Digital Twin consumes and integrates the data


| FAIR Principle         | Implementation                                |
| ----------------- | --------------------------------------------- |
| **Findable**      | Indexed time-series with structured metadata  |
| **Accessible**    | REST API via FastAPI                          |
| **Interoperable** | JSON-based standardized data formats          |
| **Reusable**      | Validated, high-quality, well-documented data |


## **Contributing**

---

## **License**

---

## **Acknowledgments**

---

## **Contact Information**

