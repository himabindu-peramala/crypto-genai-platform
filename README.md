#  Real-Time GenAI Crypto Intelligence Platform

A high-performance Data Engineering project that combines **Real-Time Streaming** with **Generative AI** to analyze crypto market trends.

![Status](https://img.shields.io/badge/Status-Live-green) ![Stack](https://img.shields.io/badge/Stack-Spark%20%7C%20Redpanda%20%7C%20Iceberg%20%7C%20MinIO-blue)

##  What it does
This platform ingests live cryptocurrency data (Bitcoin, Ethereum, Solana), processes it in real-time, stores it in a Data Lakehouse, and uses an AI Agent to answer questions about market trends.

##  Architecture
1.  **Ingestion**: Python Producer fetches live data from **CoinGecko API** -> **Redpanda (Kafka)**.
2.  **Processing**: **Apache Spark Structured Streaming** aggregates data (windowing, cleaning).
3.  **Storage**: Data is written to **MinIO (S3)** using **Apache Iceberg** format (The Lakehouse).
4.  **AI Analysis**: **Streamlit** dashboard queries the Lakehouse via **DuckDB** and uses **OpenAI GPT-4** for insights.

##  Tech Stack
*   **Streaming**: Redpanda (Kafka-compatible, C++ performance)
*   **Processing**: Apache Spark 3.5 (Structured Streaming)
*   **Storage**: MinIO (S3 Object Storage) + Apache Iceberg (Table Format)
*   **Interface**: Streamlit + Altair (Charts) + LangChain/OpenAI (RAG)
*   **Infrastructure**: Docker Compose

##  How to Run

### 1. Prerequisites
*   Docker & Docker Compose
*   Python 3.9+
*   OpenAI API Key (Optional - Demo Mode available)

### 2. Start Infrastructure
```bash
docker compose up -d
```
*Starts Redpanda, Spark Master/Worker, MinIO, and Airflow.*

### 3. Start Data Pipeline
**Terminal 1 (Producer):**
```bash
source venv/bin/activate
python scripts/producer.py
```

**Terminal 2 (Spark Job):**
```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/workspace/scripts/spark_minio.py
```

### 4. Launch AI Dashboard
**Terminal 3 (UI):**
```bash
streamlit run scripts/genai_app.py
```
Access at: http://localhost:8501


## 📄 License
MIT
