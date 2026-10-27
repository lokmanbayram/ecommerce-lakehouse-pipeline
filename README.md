# End-to-End E-Commerce & FX Data Lakehouse Pipeline

An automated, idempotent, and production-grade data pipeline built on **Medallion Architecture (Bronze -> Silver -> Gold)**. This project demonstrates modern data engineering practices by ingesting REST API data into an AWS S3 Data Lake, staging it in a Cloud PostgreSQL Data Warehouse, and applying dimensional modeling via dbt Core.

## 🏗️ Architecture & Data Flow

1. **Ingestion (Bronze Layer):** Python script extracts live FX rates and e-commerce transactions (JSON) with resilient HTTP retry strategies, landing them into an **AWS S3** bucket partitioned by `ingestion_date`.
2. **Staging (Silver Layer):** A loader parses the semi-structured nested JSON objects from S3 and performs **Idempotent Upserts** (`ON CONFLICT DO UPDATE`) into a staging schema on **Cloud PostgreSQL (Neon)** via bulk executions.
3. **Analytics (Gold Layer):** **dbt Core** transforms staging tables into a Star Schema (fact & dimension tables), applying real-time currency conversions and executing automated data quality tests.
4. **Orchestration:** Fully automated via **GitHub Actions** running daily (Serverless CI/CD).

## 🛠️ Tech Stack
* **Language:** Python 3.10 (Object-Oriented, Type Hinting, Dataclasses)
* **Cloud & Storage:** AWS S3, `boto3`
* **Database & Warehouse:** Cloud PostgreSQL (Neon), `psycopg2`
* **Transformation & Testing:** dbt Core (Jinja SQL)
* **Orchestration & CI/CD:** GitHub Actions
* **Security:** Python `dotenv`, GitHub Secrets (No hardcoded credentials)

## 🚀 Key Engineering Standards Implemented
* **Idempotency:** The pipeline can be re-run safely without duplicating records.
* **Fail-Fast Configuration:** Centralized `config.py` validates all environment variables before execution.
* **Fault Tolerance:** API requests implement `urllib3` Exponential Backoff for resilient network handling.
* **Hive-Style Partitioning:** S3 artifacts are stored hierarchically (e.g., `raw/orders/ingestion_date=YYYY-MM-DD/`) to optimize future big data querying costs (Athena/Spark).

## 📂 Project Structure