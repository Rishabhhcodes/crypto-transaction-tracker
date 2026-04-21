# 🚀 Crypto Transaction Tracker

## 📌 Overview
A real-time blockchain transaction monitoring system using Kafka, Spark, PostgreSQL, and Streamlit.

## 🧱 Architecture
Producer → Kafka → Spark → PostgreSQL → Dashboard

## ⚙️ Tech Stack
- Python
- Apache Kafka
- Apache Spark
- PostgreSQL
- Streamlit
- Docker

## ▶️ How to Run

### 1. Start containers
docker-compose up -d

### 2. Run Spark job
docker exec -it spark /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0 /opt/spark/jobs/spark_stream.py

### 3. Run dashboard
streamlit run dashboard.py

## 📊 Features
- Real-time transaction tracking
- ETH volume analysis
- Whale transaction detection
- Transaction filtering

## 🔐 Environment Variables
Create `.env`:

ETHERSCAN_API_KEY=your_key  
WALLET_ADDRESS=your_wallet  

## 📌 Notes
Full streaming pipeline runs locally using Docker.